pub mod appnodes;
pub mod shvnode;

use crate::shvnode::{find_longest_prefix, process_local_dir_ls, ShvNode, METH_PING};
use async_broadcast::RecvError;
use async_std::io::BufReader;
use async_std::net::TcpStream;
use duration_str::parse;
use futures::future::LocalBoxFuture;
use futures::{select, AsyncReadExt, FutureExt};
use log::*;
use shv::broker::node::{METH_SUBSCRIBE, METH_UNSUBSCRIBE};
use shv::client::{ClientConfig, LoginParams};
use shv::metamethod::MetaMethod;
use shv::rpcframe::RpcFrame;
use shv::rpcmessage::{RpcError, RpcErrorCode};
use shv::util::login_from_url;
use shv::{client, make_map, rpcvalue, RpcMessage, RpcMessageMetaTags, RpcValue};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use url::Url;

pub type Sender<K> = async_std::channel::Sender<K>;
pub type Receiver<K> = async_std::channel::Receiver<K>;

pub type BroadcastSender<K> = async_broadcast::Sender<K>;
pub type BroadcastReceiver<K> = async_broadcast::Receiver<K>;

#[derive(Clone)]
pub struct RequestData {
    pub mount_path: String,
    pub request: RpcMessage,
}

pub enum DeviceCommand {
    SendMessage {
        message: RpcMessage,
    },
    RpcCall {
        request: RpcMessage,
        response_sender: Sender<RpcFrame>,
    },
    Subscribe {
        path: String,
        // methods: String, // Not implemented
        notifications_sender: Sender<RpcFrame>,
    },
    Unsubscribe {
        path: String,
    },
}

const BROKER_APP_NODE: &str = ".broker/app";

pub enum RequestResult {
    Response(RpcValue),
    Error(RpcError),
}

pub type HandlerFn<S> = Box<
    dyn for<'a> Fn(RequestData, Sender<DeviceCommand>, &'a mut Option<S>) -> LocalBoxFuture<'_, ()>,
>;

pub struct Route<S> {
    handler: HandlerFn<S>,
    methods: Vec<String>,
}

#[macro_export]
macro_rules! handler {
    ($func:ident) => {
        Box::new(move |r, s, t| Box::pin($func(r, s, t)))
    };
}

#[macro_export]
macro_rules! handler_stateless {
    ($func:ident) => {
        Box::new(move |r, s, _t| Box::pin($func(r, s)))
    };
}

impl<S> Route<S> {
    pub fn new<I>(methods: I, handler: HandlerFn<S>) -> Self
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        Self {
            handler,
            methods: methods.into_iter().map(|x| x.into()).collect(),
        }
    }
}

#[derive(Clone)]
pub enum DeviceEvent {
    /// Device core sends this event when connected to a broker
    Connected,
    Disconnected,
}

enum ConnectionEvent {
    RpcFrameReceived(RpcFrame),
    Connected(Sender<ConnectionCommand>),
    Disconnected,
}

enum ConnectionCommand {
    SendMessage(RpcMessage),
}

pub struct DeviceEventsReceiver(BroadcastReceiver<DeviceEvent>);

impl DeviceEventsReceiver {
    pub async fn wait_for_event(&mut self) -> Result<DeviceEvent, RecvError> {
        loop {
            match self.0.recv().await {
                Ok(evt) => break Ok(evt),
                Err(async_broadcast::RecvError::Overflowed(cnt)) => {
                    warn!("Device event receiver missed {cnt} event(s)!");
                },
                err => break err,
            }
        }
    }

    pub fn recv_event(&mut self) -> Pin<Box<async_broadcast::Recv<'_, DeviceEvent>>> {
        self.0.recv()
    }
}


pub struct ShvDevice<S> {
    mounts: BTreeMap<String, ShvNode<S>>,
    state: Option<S>,
}

impl<S> ShvDevice<S> {
    pub fn new() -> Self {
        Self {
            mounts: Default::default(),
            state: Default::default(),
        }
    }

    pub fn mount<P, M, R>(&mut self, path: P, defined_methods: M, routes: R) -> &mut Self
    where
        P: AsRef<str>,
        M: Into<Vec<MetaMethod>>,
        R: Into<Vec<Route<S>>>,
    {
        let path = path.as_ref();
        let node = ShvNode::new(defined_methods).add_routes(routes.into());
        self.mounts.insert(path.into(), node);
        self
    }

    pub fn with_state(&mut self, state: S) -> &mut Self {
        self.state = Some(state);
        self
    }

    fn run_with_init_opt<H>(&mut self, config: &ClientConfig, handler: Option<H>) -> shv::Result<()>
    where
        H: FnOnce(Sender<DeviceCommand>, DeviceEventsReceiver),
    {
        let (conn_evt_tx, conn_evt_rx) = async_std::channel::unbounded::<ConnectionEvent>();
        async_std::task::spawn(connection_task(config.clone(), conn_evt_tx));
        async_std::task::block_on(self.device_loop(conn_evt_rx, handler))
    }

    pub fn run(&mut self, config: &ClientConfig) -> shv::Result<()> {
        self.run_with_init_opt(config, Option::<fn(Sender<DeviceCommand>, DeviceEventsReceiver)>::None)
    }

    pub fn run_with_init<H>(&mut self, config: &ClientConfig, handler: H) -> shv::Result<()>
    where
        H: FnOnce(Sender<DeviceCommand>, DeviceEventsReceiver),
    {
        self.run_with_init_opt(config, Some(handler))
    }

    async fn process_rpc_frame(
        mounts: &mut BTreeMap<String, ShvNode<S>>,
        state: &mut Option<S>,
        device_cmd_sender: &Sender<DeviceCommand>,
        pending_rpc_calls: &mut HashMap<i64, Sender<RpcFrame>>,
        subscriptions: &mut HashMap<String, Sender<RpcFrame>>,
        frame: RpcFrame) -> shv::Result<()>
    {
        if frame.is_request() {
            if let Ok(mut rpcmsg) = frame.to_rpcmesage() {
                if let Ok(mut resp) = rpcmsg.prepare_response() {
                    let shv_path = frame.shv_path().unwrap_or_default();
                    let local_result = process_local_dir_ls(mounts, &frame);
                    match local_result {
                        None => {
                            if let Some((mount, path)) = find_longest_prefix(mounts, &shv_path) {
                                rpcmsg.set_shvpath(path);
                                let node = mounts.get(mount).unwrap();
                                node.process_request(
                                    RequestData { mount_path: mount.into(), request: rpcmsg },
                                    device_cmd_sender.clone(),
                                    state).await;
                            } else {
                                let method = frame.method().unwrap_or_default();
                                resp.set_error(RpcError::new(RpcErrorCode::MethodNotFound, &format!("Invalid shv path {}:{}()", shv_path, method)));
                                device_cmd_sender.send(DeviceCommand::SendMessage { message: resp }).await?;
                            }
                        }
                        Some(result) => {
                            match result {
                                RequestResult::Response(r) => {
                                    resp.set_result(r);
                                    device_cmd_sender.send(DeviceCommand::SendMessage { message: resp }).await?;
                                }
                                RequestResult::Error(e) => {
                                    resp.set_error(e);
                                    device_cmd_sender.send(DeviceCommand::SendMessage { message: resp }).await?;
                                }
                            }
                        }
                    };
                } else {
                    warn!("Invalid request frame received.");
                }
            } else {
                warn!("Invalid shv request");
            }
        } else if frame.is_response() {
            if let Some(req_id) = frame.request_id() {
                if let Some(response_sender) = pending_rpc_calls.remove(&req_id) {
                    if let Err(_) = response_sender.send(frame.clone()).await {
                        warn!("Response channel closed before received response: {}", &frame)
                    }
                }
            }
        } else if frame.is_signal() {
            if let Some(path) = frame.shv_path() {
                if let Some((subscribed_path, _)) = find_longest_prefix(subscriptions, &path) {
                    let notifications_sender = subscriptions.get(subscribed_path).unwrap();
                    let subscribed_path = subscribed_path.to_owned();
                    if let Err(_) = notifications_sender.send(frame).await {
                        warn!("Notification channel for path `{}` closed while subscription still active. Automatically unsubscribing.", &subscribed_path);
                        subscriptions.remove(&subscribed_path);
                        let request = create_subscription_request(&subscribed_path, SubscriptionRequest::Unsubscribe);
                        device_cmd_sender
                            .send(DeviceCommand::SendMessage { message: request })
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    async fn device_loop<H>(&mut self, conn_event_receiver: Receiver<ConnectionEvent>, init_handler: Option<H>) -> shv::Result<()>
    where
        H: FnOnce(Sender<DeviceCommand>, DeviceEventsReceiver),
    {
        let mut pending_rpc_calls: HashMap<i64, Sender<RpcFrame>> = HashMap::new();
        let mut subscriptions: HashMap<String, Sender<RpcFrame>> = HashMap::new();

        let (device_cmd_sender, device_cmd_receiver) =
            async_std::channel::unbounded::<DeviceCommand>();
        let (mut device_events_tx, device_events_rx) = async_broadcast::broadcast(10);
        device_events_tx.set_overflow(true);
        let dev_events_receiver = DeviceEventsReceiver(device_events_rx.clone());
        let mut conn_cmd_sender: Option<Sender<ConnectionCommand>> = None;

        if let Some(init_handler) = init_handler {
            init_handler(device_cmd_sender.clone(), dev_events_receiver);
        }

        loop {
            select! {
                device_cmd_result = device_cmd_receiver.recv().fuse() => match device_cmd_result {
                    Ok(device_cmd) => {
                        use DeviceCommand::*;
                        match device_cmd {
                            SendMessage{message} => {
                                if let Some(ref conn_cmd_sender) = conn_cmd_sender {
                                    if let Err(e) = conn_cmd_sender.send(ConnectionCommand::SendMessage(message)).await {
                                        error!("Cannot send message through ConnectionCommand channel: {e}");
                                    }
                                }
                            },
                            RpcCall{request, response_sender} => {
                                let req_id = request.request_id().expect("request_id in the request of a RpcCall must be set");
                                if pending_rpc_calls.insert(req_id, response_sender).is_some() {
                                    error!("request_id {req_id} for async RpcCall has already been registered");
                                }
                                device_cmd_sender.send(SendMessage{ message: request }).await?;
                            },
                            Subscribe{path, /* methods, */ notifications_sender} => {
                                if subscriptions.insert(path.clone(), notifications_sender).is_some() {
                                    warn!("Path {} has already been subscribed!", &path);
                                }
                                let request = create_subscription_request(&path, SubscriptionRequest::Subscribe);
                                device_cmd_sender
                                    .send(SendMessage { message: request })
                                    .await
                                    .expect("Cannot send subscription request through DeviceCommand channel");
                            },
                            Unsubscribe{path} => {
                                if let None = subscriptions.remove(&path) {
                                    warn!("No subscription found for path `{}`", &path);
                                }
                                let request = create_subscription_request(&path, SubscriptionRequest::Unsubscribe);
                                device_cmd_sender
                                    .send(SendMessage { message: request })
                                    .await
                                    .expect("Cannot send subscription request through DeviceCommand channel");
                            },
                        }
                    },
                    Err(err) => {
                        panic!("Couldn't get RpcCommand from the channel: {err}");
                    },
                },
                conn_event_result = conn_event_receiver.recv().fuse() => match conn_event_result {
                    Ok(conn_event) => {
                        use ConnectionEvent::*;
                        match conn_event {
                            RpcFrameReceived(frame) => {
                                Self::process_rpc_frame(&mut self.mounts, &mut self.state, &device_cmd_sender, &mut pending_rpc_calls, &mut subscriptions, frame)
                                    .await
                                    .expect("Cannot process RPC frame");
                            },
                            Connected(sender) => {
                                conn_cmd_sender = Some(sender);
                                if let Err(err) = device_events_tx.try_broadcast(DeviceEvent::Connected) {
                                    error!("Device event `Connected` broadcast error: {err}");
                                }
                            },
                            Disconnected => {
                                conn_cmd_sender = None;
                                subscriptions.clear();
                                pending_rpc_calls.clear();
                                if let Err(err) = device_events_tx.try_broadcast(DeviceEvent::Disconnected) {
                                    error!("Device event `Disconnected` broadcast error: {err}");
                                }
                            },
                        }
                    }
                    Err(_) => {
                        warn!("Connection task terminated, exiting");
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn connection_task(config: ClientConfig, conn_event_sender: Sender<ConnectionEvent>) -> shv::Result<()> {
    let res = async {
        if let Some(time_str) = &config.reconnect_interval {
            match parse(time_str) {
                Ok(interval) => {
                    info!("Reconnect interval set to: {:?}", interval);
                    loop {
                        match connection_loop(&config, &conn_event_sender).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(err) => {
                                error!("Error in connection loop: {err}");
                                info!("Reconnecting after: {:?}", interval);
                                async_std::task::sleep(interval).await;
                            }
                        }
                    }
                }
                Err(err) => {
                    return Err(err.into());
                }
            }
        } else {
            connection_loop(&config, &conn_event_sender).await
        }
    }.await;

    match &res {
        Ok(_) => info!("Connection task finished OK"),
        Err(e) => error!("Connection task finished with error: {e}"),
    }
    res
    // NOTE: The connection_task termination is detected in the device_task
    // by conn_event_sender drop that occurs here.
}

async fn connection_loop(config: &ClientConfig, conn_event_sender: &Sender<ConnectionEvent>) -> shv::Result<()> {
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
    let stream = TcpStream::connect(&address).await?;
    let (reader, mut writer) = stream.split();

    let mut brd = BufReader::new(reader);
    let mut frame_reader = shv::connection::FrameReader::new(&mut brd);
    let mut frame_writer = shv::connection::FrameWriter::new(&mut writer);

    // login
    let (user, password) = login_from_url(&url);
    let heartbeat_interval = config.heartbeat_interval_duration()?;
    let login_params = LoginParams {
        user,
        password,
        mount_point: (&config.mount.clone().unwrap_or_default()).to_owned(),
        device_id: (&config.device_id.clone().unwrap_or_default()).to_owned(),
        heartbeat_interval,
        ..Default::default()
    };

    info!("Connected OK");
    info!("Heartbeat interval set to: {:?}", heartbeat_interval);
    client::login(&mut frame_reader, &mut frame_writer, &login_params).await?;

    let mut fut_heartbeat_timeout = Box::pin(async_std::task::sleep(heartbeat_interval)).fuse();

    let (conn_cmd_sender, conn_cmd_receiver) = async_std::channel::unbounded::<ConnectionCommand>();
    conn_event_sender.send(ConnectionEvent::Connected(conn_cmd_sender.clone())).await?;

    let res: shv::Result<()> = async move {
        loop {
            let fut_receive_frame = frame_reader.receive_frame();
            select! {
                _ = fut_heartbeat_timeout => {
                    // send heartbeat
                    let message = RpcMessage::new_request(".app", METH_PING, None);
                    conn_cmd_sender.send(ConnectionCommand::SendMessage(message)).await?;
                },
                conn_cmd_result = conn_cmd_receiver.recv().fuse() => match conn_cmd_result {
                    Ok(connection_command) => {
                        match connection_command {
                            ConnectionCommand::SendMessage(message) => {
                                // reset heartbeat timer
                                fut_heartbeat_timeout = Box::pin(async_std::task::sleep(heartbeat_interval)).fuse();
                                frame_writer.send_message(message).await?;
                            },
                        }
                    },
                    Err(err) => {
                        // return Err(format!("Couldn't get ConnectionCommand from the channel: {err}").into());
                        error!("Couldn't get ConnectionCommand from the channel: {err}");
                    },
                },
                receive_frame_result = fut_receive_frame.fuse() => match receive_frame_result {
                    Ok(None) => {
                        return Err("Device socket closed".into());
                    }
                    Ok(Some(frame)) => {
                        conn_event_sender.send(ConnectionEvent::RpcFrameReceived(frame)).await?;
                    }
                    Err(e) => {
                        error!("Receive frame error - {e}");
                    }
                }
            }
        }
    }.await;
    conn_event_sender.send(ConnectionEvent::Disconnected).await?;
    res
}

enum SubscriptionRequest {
    Subscribe,
    Unsubscribe,
}

fn create_subscription_request(path: &str, request_method: SubscriptionRequest) -> RpcMessage {
    RpcMessage::new_request(
        BROKER_APP_NODE,
        match request_method {
            SubscriptionRequest::Subscribe => METH_SUBSCRIBE,
            SubscriptionRequest::Unsubscribe => METH_UNSUBSCRIBE,
        },
        Some(make_map!("methods" => "", "path" => path).into()),
    )
}
