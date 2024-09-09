use crate::runtime::{current_task_runtime, Runtime};
use crate::clientnode::METH_PING;
pub use crate::client::Sender;
use duration_str::parse;
use futures::{select, AsyncReadExt, FutureExt, StreamExt};
use log::*;
pub use shvrpc::client::ClientConfig;
use shvrpc::client::LoginParams;
use shvrpc::framerw::{FrameReader, FrameWriter};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::util::login_from_url;
use shvrpc::{client, RpcMessage};
use url::Url;

pub fn spawn_connection_task(config: &ClientConfig, conn_evt_tx: Sender<ConnectionEvent>) {
    match current_task_runtime() {
        #[cfg(feature = "tokio")]
        Runtime::Tokio => {
            tokio::spawn(connection_task(config.clone(), conn_evt_tx, Runtime::Tokio));
        }
        #[cfg(feature = "async_std")]
        Runtime::AsyncStd => {
            async_std::task::spawn(connection_task(config.clone(), conn_evt_tx, Runtime::AsyncStd));
        }
    };
}

async fn connect(address: String, runtime: Runtime)
    -> shvrpc::Result<(Box<dyn futures::AsyncRead + Send + Unpin>, Box<dyn futures::AsyncWrite + Send + Unpin>)>
{
    match runtime {
        #[cfg(feature = "tokio")]
        Runtime::Tokio => {
            use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
            let stream = tokio::net::TcpStream::connect(address).await?;
            let (reader, writer) = stream.into_split();
            let writer = writer.compat_write();
            let reader = tokio::io::BufReader::new(reader).compat();
            Ok((Box::new(reader), Box::new(writer)))
        }
        #[cfg(feature = "async_std")]
        Runtime::AsyncStd => {
            let stream = async_std::net::TcpStream::connect(address).await?;
            let (reader, writer) = stream.split();
            let reader = futures::io::BufReader::new(reader);
            Ok((Box::new(reader), Box::new(writer)))
        }
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

async fn connection_task(
    config: ClientConfig,
    conn_event_sender: Sender<ConnectionEvent>,
    runtime: Runtime,
) -> shvrpc::Result<()>
{
    let res = async {
        if let Some(time_str) = &config.reconnect_interval {
            match parse(time_str) {
                Ok(interval) => {
                    info!("Reconnect interval set to: {:?}", interval);
                    loop {
                        match connection_loop(&config, &conn_event_sender, runtime).await {
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
            connection_loop(&config, &conn_event_sender, runtime).await
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

async fn connection_loop(
    config: &ClientConfig,
    conn_event_sender: &Sender<ConnectionEvent>,
    runtime: Runtime,
) -> shvrpc::Result<()> {
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
    let (reader, writer) = connect(address, runtime).await?;
    let mut frame_reader = shvrpc::streamrw::StreamFrameReader::new(reader);
    let mut frame_writer = shvrpc::streamrw::StreamFrameWriter::new(writer);

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
    let client_id = client::login(&mut frame_reader, &mut frame_writer, &login_params).await?;
    info!("Login OK, client ID: {client_id}");

    let (writer_tx, mut writer_rx) = futures::channel::mpsc::unbounded();
    let _writer_task = crate::runtime::spawn_task(async move {
        debug!("Writer task start");
        let res: shvrpc::Result<()> = {
            while let Some(frame) = writer_rx.next().await {
                frame_writer.send_message(frame).await?;
            }
            Ok(())
        };
        debug!("Writer task finish");
        res
    });

    let (conn_cmd_sender, mut conn_cmd_receiver) = futures::channel::mpsc::unbounded();
    conn_event_sender.unbounded_send(ConnectionEvent::Connected(conn_cmd_sender.clone()))?;

    let res: shvrpc::Result<()> = async move {
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
                                    writer_tx.unbounded_send(message)?;
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
    // writer_task.cancel().await;
    res
}
