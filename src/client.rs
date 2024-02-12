
use crate::connection::{ConnectionEvent, spawn_connection_task, ConnectionCommand};
use crate::shvnode::{find_longest_prefix, process_local_dir_ls, ShvNode};
use async_broadcast::RecvError;
use futures::future::LocalBoxFuture;
use futures::{select, FutureExt, StreamExt};
use log::*;
use shv::broker::node::{METH_SUBSCRIBE, METH_UNSUBSCRIBE};
use shv::client::{ClientConfig};
use shv::metamethod::MetaMethod;
use shv::rpcframe::RpcFrame;
use shv::rpcmessage::{RpcError, RpcErrorCode};
use shv::{make_map, rpcvalue, RpcMessage, RpcMessageMetaTags, RpcValue};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;

pub type Sender<K> = futures::channel::mpsc::UnboundedSender<K>;
pub type Receiver<K> = futures::channel::mpsc::UnboundedReceiver<K>;

type BroadcastReceiver<K> = async_broadcast::Receiver<K>;

#[derive(Clone)]
pub struct RequestData {
    pub mount_path: String,
    pub request: RpcMessage,
}

pub enum ClientCommand {
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
    dyn for<'a> Fn(RequestData, Sender<ClientCommand>, &'a mut Option<S>) -> LocalBoxFuture<'_, ()>,
>;

pub struct Route<S> {
    pub handler: HandlerFn<S>,
    pub methods: Vec<String>,
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
pub enum ClientEvent {
    /// Client core broadcasts this event when connected to a broker
    Connected,
    Disconnected,
}

pub struct ClientEventsReceiver(BroadcastReceiver<ClientEvent>);

impl ClientEventsReceiver {
    pub async fn wait_for_event(&mut self) -> Result<ClientEvent, RecvError> {
        loop {
            match self.0.recv().await {
                Ok(evt) => break Ok(evt),
                Err(async_broadcast::RecvError::Overflowed(cnt)) => {
                    warn!("Client event receiver missed {cnt} event(s)!");
                },
                err => break err,
            }
        }
    }

    pub fn recv_event(&mut self) -> Pin<Box<async_broadcast::Recv<'_, ClientEvent>>> {
        self.0.recv()
    }
}


pub struct Client<S> {
    mounts: BTreeMap<String, ShvNode<S>>,
    app_data: Option<S>,
}

impl<S> Client<S> {
    pub fn new() -> Self {
        Self {
            mounts: Default::default(),
            app_data: Default::default(),
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

    pub fn with_app_data(&mut self, app_data: S) -> &mut Self {
        self.app_data = Some(app_data);
        self
    }

    async fn run_with_init_opt<H>(&mut self, config: &ClientConfig, init_handler: Option<H>) -> shv::Result<()>
    where
        H: FnOnce(Sender<ClientCommand>, ClientEventsReceiver),
    {
        // let (conn_evt_tx, conn_evt_rx) = tokio::sync::mpsc::channel::<ConnectionEvent>(32);
        // let (conn_evt_tx, conn_evt_rx) = async_std::channel::unbounded::<ConnectionEvent>();
        let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
        spawn_connection_task(config, conn_evt_tx);
        self.client_loop(conn_evt_rx, init_handler).await
    }

    pub async fn run(&mut self, config: &ClientConfig) -> shv::Result<()> {
        self
            .run_with_init_opt(config, Option::<fn(Sender<ClientCommand>, ClientEventsReceiver)>::None)
            .await
    }

    pub async fn run_with_init<H>(&mut self, config: &ClientConfig, handler: H) -> shv::Result<()>
    where
        H: FnOnce(Sender<ClientCommand>, ClientEventsReceiver),
    {
        self
            .run_with_init_opt(config, Some(handler))
            .await
    }

    async fn client_loop<H>(&mut self, mut conn_events_rx: Receiver<ConnectionEvent>, init_handler: Option<H>) -> shv::Result<()>
    where
        H: FnOnce(Sender<ClientCommand>, ClientEventsReceiver),
    {
        let mut pending_rpc_calls: HashMap<i64, Sender<RpcFrame>> = HashMap::new();
        let mut subscriptions: HashMap<String, Sender<RpcFrame>> = HashMap::new();

        let (client_cmd_tx, mut client_cmd_rx) =
            // tokio::sync::mpsc::channel::<DeviceCommand>(32);
            futures::channel::mpsc::unbounded();
        let (mut client_events_tx, client_events_rx) = async_broadcast::broadcast(10);
        client_events_tx.set_overflow(true);
        let client_events_receiver = ClientEventsReceiver(client_events_rx.clone());
        let mut conn_cmd_sender: Option<Sender<ConnectionCommand>> = None;

        if let Some(init_handler) = init_handler {
            init_handler(client_cmd_tx.clone(), client_events_receiver);
        }

        loop {
            select! {
                // device_cmd_result = device_cmd_receiver.recv().fuse() => match device_cmd_result {
                client_cmd_result = client_cmd_rx.next().fuse() => match client_cmd_result {
                    Some(client_cmd) => {
                    // Ok(device_cmd) => {
                        use ClientCommand::*;
                        match client_cmd {
                            SendMessage{message} => {
                                if let Some(ref conn_cmd_sender) = conn_cmd_sender {
                                    // if let Err(e) = conn_cmd_sender.send(ConnectionCommand::SendMessage(message)).await {
                                    if let Err(e) = conn_cmd_sender.unbounded_send(ConnectionCommand::SendMessage(message)) {
                                        error!("Cannot send message through ConnectionCommand channel: {e}");
                                    }
                                }
                            },
                            RpcCall{request, response_sender} => {
                                let req_id = request.request_id().expect("request_id in the request of a RpcCall must be set");
                                if pending_rpc_calls.insert(req_id, response_sender).is_some() {
                                    error!("request_id {req_id} for async RpcCall has already been registered");
                                }
                                // device_cmd_sender.send(SendMessage{ message: request }).await?;
                                client_cmd_tx.unbounded_send(SendMessage{ message: request })?;
                            },
                            Subscribe{path, /* methods, */ notifications_sender} => {
                                if subscriptions.insert(path.clone(), notifications_sender).is_some() {
                                    warn!("Path {} has already been subscribed!", &path);
                                }
                                let request = create_subscription_request(&path, SubscriptionRequest::Subscribe);
                                // device_cmd_sender
                                //     .send(SendMessage { message: request })
                                //     .await
                                //     .expect("Cannot send subscription request through DeviceCommand channel");
                                client_cmd_tx
                                    .unbounded_send(SendMessage { message: request })
                                    .expect("Cannot send subscription request through ClientCommand channel");
                            },
                            Unsubscribe{path} => {
                                if let None = subscriptions.remove(&path) {
                                    warn!("No subscription found for path `{}`", &path);
                                }
                                let request = create_subscription_request(&path, SubscriptionRequest::Unsubscribe);
                                // device_cmd_sender
                                //     .send(SendMessage { message: request })
                                //     .await
                                //     .expect("Cannot send subscription request through DeviceCommand channel");
                                client_cmd_tx
                                    .unbounded_send(SendMessage { message: request })
                                    .expect("Cannot send subscription request through ClientCommand channel");
                            },
                        }
                    },
                    None => {
                    // Err(_) => {
                        panic!("Couldn't get ClientCommand from the channel");
                    },
                },
                // conn_event_result = conn_event_receiver.recv().fuse() => match conn_event_result {
                conn_event_result = conn_events_rx.next().fuse() => match conn_event_result {
                    Some(conn_event) => {
                    // Ok(conn_event) => {
                        use ConnectionEvent::*;
                        match conn_event {
                            RpcFrameReceived(frame) => {
                                process_rpc_frame(&mut self.mounts, &mut self.app_data, &client_cmd_tx, &mut pending_rpc_calls, &mut subscriptions, frame)
                                    .await
                                    .expect("Cannot process RPC frame");
                            },
                            Connected(sender) => {
                                conn_cmd_sender = Some(sender);
                                if let Err(err) = client_events_tx.try_broadcast(ClientEvent::Connected) {
                                    error!("Client event `Connected` broadcast error: {err}");
                                }
                            },
                            Disconnected => {
                                conn_cmd_sender = None;
                                subscriptions.clear();
                                pending_rpc_calls.clear();
                                if let Err(err) = client_events_tx.try_broadcast(ClientEvent::Disconnected) {
                                    error!("Client event `Disconnected` broadcast error: {err}");
                                }
                            },
                        }
                    }
                    None => {
                    // Err(_) => {
                        warn!("Connection task terminated, exiting");
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn process_rpc_frame<S>(
    mounts: &mut BTreeMap<String, ShvNode<S>>,
    state: &mut Option<S>,
    client_cmd_tx: &Sender<ClientCommand>,
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
                                client_cmd_tx.clone(),
                                state).await;
                        } else {
                            let method = frame.method().unwrap_or_default();
                            resp.set_error(RpcError::new(RpcErrorCode::MethodNotFound, &format!("Invalid shv path {}:{}()", shv_path, method)));
                            // device_cmd_sender.send(DeviceCommand::SendMessage { message: resp }).await?;
                            client_cmd_tx.unbounded_send(ClientCommand::SendMessage { message: resp })?;
                        }
                    }
                    Some(result) => {
                        match result {
                            RequestResult::Response(r) => {
                                resp.set_result(r);
                                // device_cmd_sender.send(DeviceCommand::SendMessage { message: resp }).await?;
                                client_cmd_tx.unbounded_send(ClientCommand::SendMessage { message: resp })?;
                            }
                            RequestResult::Error(e) => {
                                resp.set_error(e);
                                // device_cmd_sender.send(DeviceCommand::SendMessage { message: resp }).await?;
                                client_cmd_tx.unbounded_send(ClientCommand::SendMessage { message: resp })?;
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
                // if let Err(_) = response_sender.send(frame.clone()).await {
                if let Err(_) = response_sender.unbounded_send(frame.clone()) {
                    warn!("Response channel closed before received response: {}", &frame)
                }
            }
        }
    } else if frame.is_signal() {
        if let Some(path) = frame.shv_path() {
            if let Some((subscribed_path, _)) = find_longest_prefix(subscriptions, &path) {
                let notifications_sender = subscriptions.get(subscribed_path).unwrap();
                let subscribed_path = subscribed_path.to_owned();
                // if let Err(_) = notifications_sender.send(frame).await {
                if let Err(_) = notifications_sender.unbounded_send(frame) {
                    warn!("Notification channel for path `{}` closed while subscription still active. Automatically unsubscribing.", &subscribed_path);
                    subscriptions.remove(&subscribed_path);
                    let request = create_subscription_request(&subscribed_path, SubscriptionRequest::Unsubscribe);
                    // device_cmd_sender
                    //     .send(DeviceCommand::SendMessage { message: request })
                    //     .await?;
                    client_cmd_tx
                        .unbounded_send(ClientCommand::SendMessage { message: request })?;
                }
            }
        }
    }
    Ok(())
}

enum SubscriptionRequest {
    Subscribe,
    Unsubscribe,
}

fn create_subscription_request(path: &str, request: SubscriptionRequest) -> RpcMessage {
    RpcMessage::new_request(
        BROKER_APP_NODE,
        match request {
            SubscriptionRequest::Subscribe => METH_SUBSCRIBE,
            SubscriptionRequest::Unsubscribe => METH_UNSUBSCRIBE,
        },
        Some(make_map!("methods" => "", "path" => path).into())
    )
}
