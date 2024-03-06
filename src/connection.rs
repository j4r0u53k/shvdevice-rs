use crate::runtime::{current_task_runtime, Runtime};
use crate::shvnode::METH_PING;
pub use crate::Sender;
use duration_str::parse;
use futures::{select, Future, FutureExt, StreamExt};
use generics_alias::*;
use log::*;
pub use shv::client::ClientConfig;
use shv::client::LoginParams;
use shv::framerw::{FrameReader, FrameWriter};
use shv::rpcframe::RpcFrame;
use shv::util::login_from_url;
use shv::{client, RpcMessage};
use url::Url;

pub fn spawn_connection_task(config: &ClientConfig, conn_evt_tx: Sender<ConnectionEvent>) {
    match current_task_runtime() {
        #[cfg(feature = "tokio")]
        Runtime::Tokio => tokio::spawn_connection_task(config, conn_evt_tx),
        #[cfg(feature = "async_std")]
        Runtime::AsyncStd => async_std::spawn_connection_task(config, conn_evt_tx),
        _ => panic!("Could not find suitable async runtime"),
    };
}

#[cfg(feature = "tokio")]
mod tokio {
    use super::{connection_task, ClientConfig, ConnectionEvent, Sender};
    use tokio::io::BufReader;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::TcpStream;
    use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    pub fn spawn_connection_task(config: &ClientConfig, conn_evt_tx: Sender<ConnectionEvent>) {
        tokio::spawn(connection_task(config.clone(), conn_evt_tx, connect));
    }

    async fn connect(
        address: String,
    ) -> shv::Result<(Compat<BufReader<OwnedReadHalf>>, Compat<OwnedWriteHalf>)>
// async fn connect(address: String) -> shv::Result<(BufReader<ReadHalf<TcpStream>>, WriteHalf<TcpStream>)>
    {
        // let stream = TcpStream::connect(&address.parse()?).await?;
        // let (reader, writer) = stream.split();
        // let reader = BufReader::new(reader);
        // Ok((reader, writer))

        let stream = TcpStream::connect(&address).await?;
        let (reader, writer) = stream.into_split();
        let writer = writer.compat_write();
        let reader = BufReader::new(reader).compat();
        Ok((reader, writer))
    }
}

#[cfg(feature = "async_std")]
mod async_std {
    use super::{connection_task, ClientConfig, ConnectionEvent, Sender};
    use futures::io::{BufReader, ReadHalf, WriteHalf};
    use futures::AsyncReadExt;
    use futures_net::TcpStream;

    pub(super) fn spawn_connection_task(
        config: &ClientConfig,
        conn_evt_tx: Sender<ConnectionEvent>,
    ) {
        async_std::task::spawn(connection_task(config.clone(), conn_evt_tx, connect));
    }

    async fn connect(
        address: String,
    ) -> shv::Result<(BufReader<ReadHalf<TcpStream>>, WriteHalf<TcpStream>)> {
        let stream = TcpStream::connect(&address.parse()?).await?;
        let (reader, writer) = stream.split();
        let reader = BufReader::new(reader);
        Ok((reader, writer))
    }
}

pub enum ConnectionEvent {
    RpcFrameReceived(RpcFrame),
    Connected(Sender<ConnectionCommand>),
    Disconnected,
}

pub enum ConnectionCommand {
    SendMessage(RpcMessage),
}

generics_def!(
    ConnectBounds<
        F: Future<Output = shv::Result<(R, W)>>,
        R: futures::AsyncRead + Send + Unpin,
        W: futures::AsyncWrite + Send + Unpin,
    >
);

#[generics(ConnectBounds)]
async fn connection_task<C>(config: ClientConfig, conn_event_sender: Sender<ConnectionEvent>, connect: C) -> shv::Result<()>
where
    C: FnOnce(String) -> F + Clone,
{
    let res = async {
        if let Some(time_str) = &config.reconnect_interval {
            match parse(time_str) {
                Ok(interval) => {
                    info!("Reconnect interval set to: {:?}", interval);
                    loop {
                        match connection_loop(&config, &conn_event_sender, connect.clone()).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(err) => {
                                error!("Error in connection loop: {err}");
                                info!("Reconnecting after: {:?}", interval);
                                futures_time::task::sleep(interval.into()).await;
                            }
                        }
                    }
                }
                Err(err) => {
                    Err(err.into())
                }
            }
        } else {
            connection_loop(&config, &conn_event_sender, connect).await
        }
    }
    .await;

    match &res {
        Ok(_) => info!("Connection task finished OK"),
        Err(e) => error!("Connection task finished with error: {e}"),
    }
    res
    // NOTE: The connection_task termination is detected in the device_task
    // by conn_event_sender drop that occurs here.
}

#[generics(ConnectBounds)]
async fn connection_loop<C>(
    config: &ClientConfig,
    conn_event_sender: &Sender<ConnectionEvent>,
    connect: C,
) -> shv::Result<()>
where
    C: FnOnce(String) -> F,
{
    let url = Url::parse(&config.url)?;
    let (scheme, host, port) = (
        url.scheme(),
        url.host_str().unwrap_or_default(),
        url.port().unwrap_or(3755),
    );
    if scheme != "tcp" {
        panic!("Scheme {scheme} is not supported yet.");
    }
    let address = format!("{host}:{port}");
    // Establish a connection
    info!("Connecting to: {address}");
    let (reader, writer) = connect(address).await?;
    let mut frame_reader = shv::streamrw::StreamFrameReader::new(reader);
    let mut frame_writer = shv::streamrw::StreamFrameWriter::new(writer);

    // login
    let (user, password) = login_from_url(&url);
    let heartbeat_interval = config.heartbeat_interval_duration()?;
    let login_params = LoginParams {
        user,
        password,
        mount_point: config.mount.clone().unwrap_or_default().to_owned(),
        device_id: config.device_id.clone().unwrap_or_default().to_owned(),
        heartbeat_interval,
        ..Default::default()
    };

    info!("Connected OK");
    info!("Heartbeat interval set to: {:?}", heartbeat_interval);
    client::login(&mut frame_reader, &mut frame_writer, &login_params).await?;

    let (conn_cmd_sender, mut conn_cmd_receiver) = futures::channel::mpsc::unbounded();
    conn_event_sender.unbounded_send(ConnectionEvent::Connected(conn_cmd_sender.clone()))?;

    let res: shv::Result<()> = async move {
        let mut fut_heartbeat_timeout = futures_time::task::sleep(heartbeat_interval.into()).fuse();
        let mut next_conn_cmd = conn_cmd_receiver.next().fuse();
        let mut fut_receive_frame = frame_reader.receive_frame().fuse();

        loop {
            select! {
                _ = fut_heartbeat_timeout => {
                    // send heartbeat
                    let message = RpcMessage::new_request(".app", METH_PING, None);
                    conn_cmd_sender.unbounded_send(ConnectionCommand::SendMessage(message))?;
                },
                conn_cmd_result = next_conn_cmd => {
                    match conn_cmd_result {
                        Some(connection_command) => {
                            match connection_command {
                                ConnectionCommand::SendMessage(message) => {
                                    // reset heartbeat timer
                                    fut_heartbeat_timeout = futures_time::task::sleep(heartbeat_interval.into()).fuse();
                                    frame_writer.send_message(message).await?;
                                },
                            }
                        },
                        None => {
                            error!("Couldn't get ConnectionCommand from the channel");
                        },
                    }
                    next_conn_cmd = conn_cmd_receiver.next().fuse();
                }
                receive_frame_result = fut_receive_frame => {
                    match receive_frame_result {
                        Ok(frame) => {
                            conn_event_sender.unbounded_send(ConnectionEvent::RpcFrameReceived(frame))?;
                        }
                        Err(e) => {
                            return Err(format!("Receive frame error - {e}").into());
                        }
                    }
                    // The drop before the reassignment is needed because the future is holding
                    // &mut frame_reader until it is dropped, therefore it cannot be borrowed
                    // again on the rhs of the assignment.
                    drop(fut_receive_frame);
                    fut_receive_frame = frame_reader.receive_frame().fuse();
                }
            }
        }
    }.await;
    conn_event_sender.unbounded_send(ConnectionEvent::Disconnected)?;
    res
}
