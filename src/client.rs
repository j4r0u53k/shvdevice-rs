use crate::connection::{spawn_connection_task, ConnectionCommand, ConnectionEvent, ConnectionFailedKind};
use crate::clientnode::{find_longest_path_prefix, process_local_dir_ls, ClientNode, RequestResult, Route, METH_DIR, METH_LS};
use async_broadcast::RecvError;
use futures::future::BoxFuture;
use futures::{select, Future, FutureExt, StreamExt};
use futures::channel::mpsc::TrySendError;
use log::*;
use shvrpc::client::ClientConfig;
use shvrpc::metamethod::MetaMethod;
use shvrpc::rpcdiscovery::{DirParam, DirResult, LsParam, LsResult, MethodInfo};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use shvproto::RpcValue;
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;

const METH_SUBSCRIBE: &str = "subscribe";
const METH_UNSUBSCRIBE: &str = "unsubscribe";

pub type Sender<K> = futures::channel::mpsc::UnboundedSender<K>;
pub type Receiver<K> = futures::channel::mpsc::UnboundedReceiver<K>;

type BroadcastReceiver<K> = async_broadcast::Receiver<K>;

mod sealed {
    static SUBSCRIPTION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    pub fn next_subscription_id() -> u64 {
        SUBSCRIPTION_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
use sealed::next_subscription_id;

pub struct Subscriber {
    notifications_rx: Receiver<RpcFrame>,
    // For unsubscribe on drop
    client_cmd_tx: Sender<ClientCommand>,
    path: String,
    signal: String,
    subscription_id: u64,
}

impl Subscriber {
    pub fn path_signal(&self) -> (&str, &str) {
        (&self.path, &self.signal)
    }
}

impl futures::Stream for Subscriber {
    type Item = RpcFrame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().notifications_rx.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.notifications_rx.size_hint()
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        if let Err(err) = self.client_cmd_tx.unbounded_send(
            ClientCommand::Unsubscribe {
                path: self.path.clone(),
                signal: self.signal.clone(),
                subscription_id: self.subscription_id,
            }) {
            warn!("Cannot unsubscribe path {}, signal {}, error: {}", &self.path, &self.signal, err);
        };
    }
}

#[derive(Clone,Debug)]
pub enum CallRpcMethodErrorKind {
    // The receive channel got closed before the response received
    ConnectionClosed,
    // Received frame could not be parsed to an RpcMessage
    InvalidMessage(String),
    // Got an error instead of a result
    RpcError(RpcError),
    // Could not convert result to target data type
    ResultTypeMismatch(String),
}

impl std::fmt::Display for CallRpcMethodErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let err_msg = match self {
            CallRpcMethodErrorKind::ConnectionClosed => "Connection closed",
            CallRpcMethodErrorKind::InvalidMessage(msg) => msg,
            CallRpcMethodErrorKind::RpcError(err) => &err.to_string(),
            CallRpcMethodErrorKind::ResultTypeMismatch(msg) => msg,
        };
        write!(f, "{}", err_msg)
    }
}

#[derive(Clone,Debug)]
pub struct CallRpcMethodError {
    path: String,
    method: String,
    error: CallRpcMethodErrorKind,
}

impl CallRpcMethodError {
    fn new(path: &str, method: &str, error: CallRpcMethodErrorKind) -> Self {
        Self {
            path: path.to_owned(),
            method: method.to_owned(),
            error
        }
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn method(&self) -> &str {
        &self.method
    }
    pub fn error(&self) -> &CallRpcMethodErrorKind {
        &self.error
    }

}

impl std::fmt::Display for CallRpcMethodError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RPC call on path `{path}`, method `{method}`, error: {error}",
            path = self.path,
            method = self.method,
            error = self.error,
        )
    }
}

#[derive(Clone)]
pub struct ClientCommandSender {
    pub(crate) sender: Sender<ClientCommand>,
}

impl ClientCommandSender {
    pub fn terminate_client(&self) {
        self.sender
            .unbounded_send(ClientCommand::TerminateClient)
            .unwrap_or_else(|e| error!("Failed to send TerminateClient command: {e}"));
    }

    pub fn do_rpc_call_param<'a>(
        &self,
        shvpath: impl Into<&'a str>,
        method: impl Into<&'a str>,
        param: Option<RpcValue>,
    ) -> Result<Receiver<RpcFrame>, TrySendError<ClientCommand>>
    {
        let (response_sender, response_receiver) = futures::channel::mpsc::unbounded();
        self.sender.unbounded_send(ClientCommand::RpcCall {
            request: RpcMessage::new_request(shvpath.into(), method.into(), param),
            response_sender
        })
        .map(|_| response_receiver)
    }

    pub fn do_rpc_call<'a>(
        &self,
        shvpath: impl Into<&'a str>,
        method: impl Into<&'a str>,
    ) -> Result<Receiver<RpcFrame>, TrySendError<ClientCommand>>
    {
        self.do_rpc_call_param(shvpath, method, None)
    }

    pub async fn call_dir(&self, path: &str, param: DirParam) -> Result<DirResult, CallRpcMethodError> {
        self.call_dir_into(path, param).await
    }

    pub async fn call_dir_brief(&self, path: &str) -> Result<Vec<MethodInfo>, CallRpcMethodError> {
        self.call_dir_into(path, DirParam::Brief).await
    }

    pub async fn call_dir_full(&self, path: &str) -> Result<Vec<MethodInfo>, CallRpcMethodError> {
        self.call_dir_into(path, DirParam::Full).await
    }

    pub async fn call_dir_exists(&self, path: &str, method: &str) -> Result<bool, CallRpcMethodError> {
        self.call_dir_into(path, DirParam::Exists(method.into())).await
    }

    async fn call_dir_into<T, E>(&self, path: &str, param: DirParam) -> Result<T, CallRpcMethodError>
    where
        T: TryFrom<DirResult, Error = E>,
        E: std::fmt::Display,
    {
        self.call_rpc_method(path, METH_DIR, Some(RpcValue::from(param)))
            .await
            .and_then(|dir_res|
                T::try_from(dir_res).map_err(|e|
                    CallRpcMethodError::new(
                        path,
                        METH_DIR,
                        CallRpcMethodErrorKind::ResultTypeMismatch(e.to_string())
                    )
                )
            )
    }

    pub async fn call_ls(&self, path: &str, param: LsParam) -> Result<LsResult, CallRpcMethodError> {
        self.call_ls_into(path, param).await
    }

    pub async fn call_ls_exists(&self, path: &str, dirname: &str) -> Result<bool, CallRpcMethodError> {
        self.call_ls_into(path, LsParam::Exists(dirname.into())).await
    }

    pub async fn call_ls_list(&self, path: &str) -> Result<Vec<String>, CallRpcMethodError> {
        self.call_ls_into(path, LsParam::List).await
    }

    async fn call_ls_into<T, E>(&self, path: &str, param: LsParam) -> Result<T, CallRpcMethodError>
    where
        T: TryFrom<LsResult, Error = E>,
        E: std::fmt::Display,
    {
        self.call_rpc_method(path, METH_LS, Some(RpcValue::from(param)))
            .await
            .and_then(|ls_res|
                T::try_from(ls_res).map_err(|e|
                    CallRpcMethodError::new(
                        path,
                        METH_LS,
                        CallRpcMethodErrorKind::ResultTypeMismatch(e.to_string())
                    )
                )
            )
    }

    pub async fn call_rpc_method<T, E>(
        &self,
        path: &str,
        method: &str,
        param: Option<RpcValue>,
    ) -> Result<T, CallRpcMethodError>
    where
        T: TryFrom<RpcValue, Error = E>,
        E: std::fmt::Display,
    {
        let make_error = |error_kind: CallRpcMethodErrorKind| {
            CallRpcMethodError::new(path, method, error_kind)
        };

        use CallRpcMethodErrorKind::*;
        self.do_rpc_call_param(path, method, param)
            .unwrap_or_else(|err|
                panic!("Cannot send RPC request to the client core. \
                    Path: `{path}`, method: `{method}`, error: {err}")
            )
            .next()
            .await
            .ok_or_else(|| make_error(ConnectionClosed))?
            .to_rpcmesage()
            .map_err(|e| make_error(InvalidMessage(e.to_string())))?
            .result()
            .map_err(|e| make_error(RpcError(e)))
            .cloned()
            .and_then(|r|
                T::try_from(r).map_err(|e| make_error(ResultTypeMismatch(e.to_string())))
            )
    }

    pub fn send_message(&self, message: RpcMessage) -> Result<(), TrySendError<ClientCommand>> {
        self.sender.unbounded_send(ClientCommand::SendMessage { message })
    }

    pub fn subscribe(&self, path: impl Into<String>, signal: impl Into<String>) -> Result<Subscriber, TrySendError<ClientCommand>> {
        let path = path.into();
        let signal = signal.into();
        let subscription_id = next_subscription_id();
        let (notifications_sender, notifications_receiver) = futures::channel::mpsc::unbounded();
        self.sender.unbounded_send(
            ClientCommand::Subscribe {
                path: path.clone(),
                signal: signal.clone(),
                subscription_id,
                notifications_sender
            }
        ).map(move |_| {
            Subscriber {
                notifications_rx: notifications_receiver,
                client_cmd_tx: self.sender.clone(),
                path,
                signal,
                subscription_id,
            }
        })
    }
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
        signal: String,
        subscription_id: u64,
        notifications_sender: Sender<RpcFrame>,
    },
    Unsubscribe {
        path: String,
        signal: String,
        subscription_id: u64,
    },
    TerminateClient,
}

const BROKER_APP_NODE: &str = ".broker/app";

// The wrapping struct itself is descriptive
#[allow(clippy::type_complexity)]
pub struct MethodsGetter<T>(pub(crate) Box<dyn Fn(String, Option<AppState<T>>) -> BoxFuture<'static, Option<Vec<&'static MetaMethod>>> + Sync + Send>);

impl<T> MethodsGetter<T> {
    pub fn new<F, Fut>(func: F) -> Self
    where
        F: Fn(String, Option<AppState<T>>) -> Fut + Sync + Send + 'static,
        Fut: Future<Output=Option<Vec<&'static MetaMethod>>> + Send + 'static,
    {
        Self(Box::new(move |path, data| Box::pin(func(path, data))))
    }
}

// The wrapping struct itself is descriptive
#[allow(clippy::type_complexity)]
pub struct RequestHandler<T>(pub(crate) Box<dyn Fn(RpcMessage, ClientCommandSender, Option<AppState<T>>) -> BoxFuture<'static, ()> + Sync + Send>);

impl<T> RequestHandler<T> {
    pub fn stateful<F, Fut>(func: F) -> Self
    where
        F: Fn(RpcMessage, ClientCommandSender, Option<AppState<T>>) -> Fut + Sync + Send + 'static,
        Fut: Future<Output=()> + Send + 'static
    {
        Self(Box::new(move |req, tx, data| Box::pin(func(req, tx, data))))
    }

    pub fn stateless<F, Fut>(func: F) -> Self
    where
        F: Fn(RpcMessage, ClientCommandSender) -> Fut + Sync + Send + 'static,
        Fut: Future<Output=()> + Send + 'static
    {
        Self(Box::new(move |req, tx, _data| Box::pin(func(req, tx))))
    }
}

#[derive(Clone)]
pub enum ClientEvent {
    ConnectionFailed(ConnectionFailedKind),
    Connected,
    Disconnected,
}

#[derive(Clone)]
pub struct ClientEventsReceiver(BroadcastReceiver<ClientEvent>);

impl futures::Stream for ClientEventsReceiver {
    type Item = ClientEvent;

   fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
       self.get_mut().0.poll_next_unpin(cx)
   }
   fn size_hint(&self) -> (usize, Option<usize>) {
       self.0.size_hint()
   }
}

impl ClientEventsReceiver {
    pub async fn wait_for_event(&mut self) -> Result<ClientEvent, RecvError> {
        loop {
            match self.0.recv().await {
                Ok(evt) => break Ok(evt),
                Err(async_broadcast::RecvError::Overflowed(cnt)) => {
                    warn!("Client event receiver missed {cnt} event(s)!");
                }
                err => break err,
            }
        }
    }

    pub fn recv_event(&mut self) -> Pin<Box<async_broadcast::Recv<'_, ClientEvent>>> {
        self.0.recv()
    }
}

pub struct AppState<T: ?Sized>(Arc<T>);

impl<T> AppState<T> {
    pub fn new(data: T) -> Self {
        Self(Arc::new(data))
    }
}

impl<T: ?Sized> std::ops::Deref for AppState<T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ?Sized> Clone for AppState<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: ?Sized> From<Arc<T>> for AppState<T> {
    fn from(value: Arc<T>) -> Self {
        Self(value)
    }
}

// path -> signal -> subscription ID -> notification sender
#[derive(Debug, Default)]
struct Subscriptions(BTreeMap<String, BTreeMap<String, BTreeMap<u64, Sender<RpcFrame>>>>);

impl Subscriptions {
    fn new() -> Self {
        Default::default()
    }

    fn clear(&mut self) {
        self.0.clear();
    }

    fn add(
        &mut self,
        path: impl Into<String>,
        signal: impl Into<String>,
        subscription_id: u64,
        notifications_sender: Sender<RpcFrame>,
    ) -> bool {
        let path = path.into();
        let signal = signal.into();

        let path_signal_subscriptions = self.0
            .entry(path.clone()).or_default()
            .entry(signal.clone()).or_default();

        let added_new = path_signal_subscriptions.is_empty();

        if path_signal_subscriptions.insert(subscription_id, notifications_sender).is_some() {
            panic!("BUG: Subscription with the same ID {} for path: {}, method: {}. Dump: {:?}",
                subscription_id, path, signal, &self);
        }

        added_new
    }

    fn remove(&mut self, path: impl Into<String>, signal: impl Into<String>, subscription_id: u64) -> bool {
        let path = path.into();
        let signal = signal.into();

        let removed_last = if let Some(signals) = self.0.get_mut(&path) {
            if let Some(ids) = signals.get_mut(&signal) {
                ids.remove(&subscription_id).map(|_| ids.is_empty())
            } else {
                None
            }
        } else {
            None
        };

        if removed_last.is_none() {
            // NOTE: On broker Disconnect all subscriptions are cleared.
            // If there is any NotificationsReceiver that gets dropped,
            // it will try to remove the subscription again.
            debug!("Remove non-existing subscription for path: {}, signal: {}, id: {}. Dump: {:?}",
                &path, &signal, &subscription_id, &self);
        }

        removed_last.is_some_and(|was_last| was_last)
    }
}

pub struct Client<T> {
    mounts: BTreeMap<String, ClientNode<'static, T>>,
    app_state: Option<AppState<T>>,
}

impl<T: Send + Sync + 'static> Client<T> {
    pub fn new(app_node: crate::appnodes::DotAppNode) -> Self {
        let mut client = Self {
            mounts: Default::default(),
            app_state: Default::default(),
        };
        client.mount(".app", ClientNode::constant(app_node));
        client
    }

    pub fn new_device(app_node: crate::appnodes::DotAppNode, device_node: crate::appnodes::DotDeviceNode) -> Self {
        let mut client = Self::new(app_node);
        client.mount(".device", ClientNode::constant(device_node));
        client
    }

    pub fn mount<P: Into<String>>(&mut self, path: P, node: ClientNode<'static, T>) -> &mut Self {
        self.mounts.insert(path.into(), node);
        self
    }

    pub fn mount_fixed<P, M, R>(&mut self, path: P, defined_methods: M, routes: R) -> &mut Self
    where
        P: Into<String>,
        M: IntoIterator<Item = &'static MetaMethod>,
        R: IntoIterator<Item = Route<T>>,
    {
        self.mounts.insert(path.into(), ClientNode::fixed(defined_methods, routes));
        self
    }

    pub fn mount_dynamic<P>(&mut self, path: P, methods_getter: MethodsGetter<T>, request_handler: RequestHandler<T>) -> &mut Self
    where
        P: Into<String>,
    {
        self.mounts.insert(path.into(), ClientNode::dynamic(methods_getter, request_handler));
        self
    }

    pub fn with_app_state(&mut self, app_state: AppState<T>) -> &mut Self {
        self.app_state = Some(app_state);
        self
    }

    async fn run_with_init_opt<H>(
        &mut self,
        config: &ClientConfig,
        init_handler: Option<H>,
    ) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver),
    {
        let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
        spawn_connection_task(config, conn_evt_tx);
        self.client_loop(conn_evt_rx, init_handler).await
    }

    pub async fn run(&mut self, config: &ClientConfig) -> shvrpc::Result<()> {
        self.run_with_init_opt(
            config,
            Option::<fn(ClientCommandSender, ClientEventsReceiver)>::None,
        )
        .await
    }

    pub async fn run_with_init<H>(&mut self, config: &ClientConfig, handler: H) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver),
    {
        self.run_with_init_opt(config, Some(handler)).await
    }

    async fn client_loop<H>(
        &mut self,
        mut conn_events_rx: Receiver<ConnectionEvent>,
        init_handler: Option<H>,
    ) -> shvrpc::Result<()>
    where
        H: FnOnce(ClientCommandSender, ClientEventsReceiver),
    {
        let mut pending_rpc_calls: HashMap<i64, Sender<RpcFrame>> = HashMap::new();
        let mut subscriptions = Subscriptions::new();

        let (client_cmd_tx, mut client_cmd_rx) = futures::channel::mpsc::unbounded();
        let client_cmd_tx = ClientCommandSender { sender: client_cmd_tx };
        let (mut client_events_tx, client_events_rx) = async_broadcast::broadcast(10);
        client_events_tx.set_overflow(true);
        let client_events_receiver = ClientEventsReceiver(client_events_rx.clone());
        let mut conn_cmd_sender: Option<Sender<ConnectionCommand>> = None;

        if let Some(init_handler) = init_handler {
            init_handler(client_cmd_tx.clone(), client_events_receiver);
        }

        let mut next_client_cmd = client_cmd_rx.next().fuse();
        let mut next_conn_event = conn_events_rx.next().fuse();

        loop {
            select! {
                client_cmd_result = next_client_cmd => match client_cmd_result {
                    Some(client_cmd) => {
                        use ClientCommand::*;
                        match client_cmd {
                            SendMessage { message } => {
                                if let Some(ref conn_cmd_sender) = conn_cmd_sender {
                                    if let Err(e) = conn_cmd_sender.unbounded_send(ConnectionCommand::SendMessage(message)) {
                                        error!("Cannot send message through ConnectionCommand channel: {e}");
                                    }
                                } else {
                                    warn!("Try to send an RPC message when a connection is not established. Message: {message:?}");
                                }
                            },
                            RpcCall { request, response_sender } => {
                                let req_id = request.request_id().expect("request_id in the request of a RpcCall must be set");
                                if pending_rpc_calls.insert(req_id, response_sender).is_some() {
                                    error!("request_id {req_id} for async RpcCall has already been registered");
                                }
                                client_cmd_tx
                                    .send_message(request)
                                    .unwrap_or_else(|e| error!("BUG: Cannot send a message through ClientCommand channel: {e}"));
                            },
                            Subscribe { path, signal, subscription_id, notifications_sender } => {
                                if subscriptions.add(&path, &signal, subscription_id, notifications_sender) {
                                    let request = create_subscription_request(&path, &signal, SubscriptionRequest::Subscribe);
                                    client_cmd_tx
                                        .send_message(request)
                                        .unwrap_or_else(|e| error!("BUG: Cannot send a message through ClientCommand channel: {e}"));
                                } else {
                                    warn!("Path {} and signal {} have already been subscribed!", &path, &signal);
                                }
                            },
                            Unsubscribe { path, signal, subscription_id } => {
                                if subscriptions.remove(&path, &signal, subscription_id) {
                                    let request = create_subscription_request(&path, &signal, SubscriptionRequest::Unsubscribe);
                                    client_cmd_tx
                                        .send_message(request)
                                        .unwrap_or_else(|e| error!("BUG: Cannot send a message through ClientCommand channel: {e}"));
                                }
                            },
                            TerminateClient => {
                                info!("TerminateClient command received, exiting client loop");
                                return Ok(());
                            },
                        }
                        next_client_cmd = client_cmd_rx.next().fuse();
                    },
                    None => {
                        // This should not happen because we keep a copy of
                        // client_cmd_tx in this task, so at least one sender
                        // exists and the close() method of the underlying TX
                        // channel is not accessible to the user.
                        panic!("BUG: Couldn't get ClientCommand from the channel");
                    },
                },
                conn_event_result = next_conn_event => match conn_event_result {
                    Some(conn_event) => {
                        use ConnectionEvent::*;
                        match conn_event {
                            RpcFrameReceived(frame) => {
                                self.process_rpc_frame(frame, &client_cmd_tx, &mut pending_rpc_calls, &mut subscriptions)
                                    .await
                                    .unwrap_or_else(|e| error!("Cannot process RPC frame: {e}"));
                            },
                            ConnectionFailed(kind) => {
                                if let Err(err) = client_events_tx.try_broadcast(ClientEvent::ConnectionFailed(kind)) {
                                    error!("Client event `ConnectionFailed` broadcast error: {err}");
                                }
                            }
                            Connected(sender) => {
                                conn_cmd_sender = Some(sender);
                                if let Err(err) = client_events_tx.try_broadcast(ClientEvent::Connected) {
                                    error!("Client event `Connected` broadcast error: {err}");
                                }
                            },
                            Disconnected => {
                                conn_cmd_sender = None;
                                // NOTE: When the client is disconnected, the broker also knows that
                                // (because of heartbeats) and it should remove all the subscriptions
                                // registered by the client, so the client can also safely clear
                                // the subscriptions here.
                                subscriptions.clear();
                                pending_rpc_calls.clear();
                                if let Err(err) = client_events_tx.try_broadcast(ClientEvent::Disconnected) {
                                    error!("Client event `Disconnected` broadcast error: {err}");
                                }
                            },
                        }
                        next_conn_event = conn_events_rx.next().fuse();
                    }
                    None => {
                        info!("Connection task terminated, exiting client loop");
                        return Ok(());
                    }
                },
            }
        }
    }

    async fn process_rpc_frame(
        &self,
        frame: RpcFrame,
        client_cmd_tx: &ClientCommandSender,
        pending_rpc_calls: &mut HashMap<i64, Sender<RpcFrame>>,
        subscriptions: &mut Subscriptions,
    ) -> shvrpc::Result<()> {
        if frame.is_request() {
            if let Ok(mut request_msg) = frame.to_rpcmesage() {
                if let Ok(mut resp) = request_msg.prepare_response() {
                    let shv_path = frame.shv_path().unwrap_or_default();
                    let local_result = process_local_dir_ls(&self.mounts, &frame);
                    match local_result {
                        None => {
                            if let Some((mount, path)) = find_longest_path_prefix(&self.mounts, shv_path) {
                                request_msg.set_shvpath(path);
                                let node = self.mounts.get(mount).unwrap_or_else(|| panic!("A node on path '{mount}' should exist"));
                                node.process_request(request_msg, mount.to_owned(), client_cmd_tx.clone(), &self.app_state).await;
                            } else {
                                let method = frame.method().unwrap_or_default();
                                resp.set_error(RpcError::new(
                                    RpcErrorCode::MethodNotFound,
                                    format!("Invalid shv path {shv_path}:{method}()"),
                                ));
                                client_cmd_tx.send_message(resp)?;
                            }
                        }
                        Some(result) => {
                            match result {
                                RequestResult::Response(r) => {
                                    resp.set_result(r);
                                    client_cmd_tx.send_message(resp)?;
                                }
                                RequestResult::Error(e) => {
                                    resp.set_error(e);
                                    client_cmd_tx.send_message(resp)?;
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
                    if response_sender.unbounded_send(frame.clone()).is_err() {
                        warn!("Response channel closed before received response: {}", &frame);
                    }
                }
            }
        } else if frame.is_signal() {
            if let (Some(path), Some(signal)) = (frame.shv_path(), frame.method()) {
                for (subscribed_path, subscribed_signals) in &subscriptions.0 {
                    if path.strip_prefix(subscribed_path).is_some_and(|path_rest| path_rest.is_empty() || path_rest.starts_with('/')) {
                        if let Some(subscribers) = subscribed_signals.get(signal) {
                            for (subscription_id, notifications_sender) in subscribers {
                                if notifications_sender.unbounded_send(frame.clone()).is_err() {
                                    warn!("Notification channel for path `{}`, signal `{}`, id: {} closed while the subscription is still active",
                                        &subscribed_path,
                                        &signal,
                                        subscription_id);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

enum SubscriptionRequest {
    Subscribe,
    Unsubscribe,
}

fn create_subscription_request(path: &str, signal: &str, request: SubscriptionRequest) -> RpcMessage {
    RpcMessage::new_request(
        BROKER_APP_NODE,
        match request {
            SubscriptionRequest::Subscribe => METH_SUBSCRIBE,
            SubscriptionRequest::Unsubscribe => METH_UNSUBSCRIBE,
        },
        Some({
            let mut map = shvproto::Map::new();
            map.insert("signal".to_string(), signal.into());
            map.insert("paths".to_string(),path.into());
            map.into()
        })
    )
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use futures::Future;
    use generics_alias::*;

    mod drivers {
        use super::*;
        use crate::appnodes::DotAppNode;
        use futures_time::future::FutureExt;
        use futures_time::time::Duration;
        use crate::clientnode::{SIG_CHNG, PROPERTY_METHODS};
        use shvrpc::metamethod::AccessLevel;

        struct ConnectionMock {
            conn_evt_tx: Sender<ConnectionEvent>,
            conn_cmd_rx: Receiver<ConnectionCommand>,
        }

        impl Drop for ConnectionMock {
            fn drop(&mut self) {
                if self.conn_evt_tx.unbounded_send(ConnectionEvent::Disconnected).is_err() {
                    error!("Disconnected event send error");
                }
            }
        }

        impl ConnectionMock {
            fn new(conn_evt_tx: &Sender<ConnectionEvent>) -> Self {
                let (conn_cmd_tx, conn_cmd_rx) = futures::channel::mpsc::unbounded::<ConnectionCommand>();
                conn_evt_tx.unbounded_send(ConnectionEvent::Connected(conn_cmd_tx)).expect("Connected event send error");
                Self {
                    conn_evt_tx: conn_evt_tx.clone(),
                    conn_cmd_rx,
                }
            }

            fn emulate_receive_request(&self, request: RpcMessage) {
                self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(request.to_frame().unwrap())).unwrap();
            }

            fn emulate_receive_response(&self, from_request: &RpcMessage, result: impl Into<RpcValue>) {
                let mut resp = from_request.prepare_response().unwrap();
                resp.set_result(result);
                self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(resp.to_frame().unwrap())).unwrap();
            }

            fn emulate_receive_signal(&self, path: &str, sig_name: &str, param: Option<RpcValue>) {
                let sig = RpcMessage::new_signal(path, sig_name, param);
                self.conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(sig.to_frame().unwrap())).unwrap();
            }

            async fn expect_send_message(&mut self) -> RpcMessage {
                let Some(ConnectionCommand::SendMessage(msg)) = self.conn_cmd_rx.next().await else {
                    panic!("ConnectionCommand receive error");
                };
                msg
            }
        }

        async fn expect_client_connected(client_events_rx: &mut ClientEventsReceiver) {
            let ClientEvent::Connected = client_events_rx.wait_for_event().await.expect("Client event receive") else {
                panic!("Expected Connected client event");
            };
        }

        async fn expect_client_disconnected(client_events_rx: &mut ClientEventsReceiver) {
            let ClientEvent::Disconnected = client_events_rx.wait_for_event().await.expect("Client event receive") else {
                panic!("Expected Disconnected client event");
            };
        }

        async fn init_connection(
            conn_evt_tx: &Sender<ConnectionEvent>,
            cli_evt_rx: &mut ClientEventsReceiver,
        ) -> ConnectionMock {
            let conn_mock = ConnectionMock::new(conn_evt_tx);
            expect_client_connected(cli_evt_rx).await;
            conn_mock
        }

        pub(super) async fn receive_connected_and_disconnected_events(
            conn_evt_tx: Sender<ConnectionEvent>,
            _cli_cmd_tx: ClientCommandSender,
            mut client_events_rx: ClientEventsReceiver,
        ) {
            {
                let _conn_mock = ConnectionMock::new(&conn_evt_tx);
                expect_client_connected(&mut client_events_rx).await;
            }
            expect_client_disconnected(&mut client_events_rx).await;

            let _conn_mock = ConnectionMock::new(&conn_evt_tx);
            expect_client_connected(&mut client_events_rx).await;
        }

        pub(super) async fn send_message(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;

            cli_cmd_tx.send_message(RpcMessage::new_request(
                    "path/test",
                    "test_method",
                    Some(42.into())))
                .expect("Client command send");

            let msg = conn_mock.expect_send_message().await;

            assert!(msg.is_request());
            assert_eq!(msg.shv_path(), Some("path/test"));
            assert_eq!(msg.method(), Some("test_method"));
            assert_eq!(msg.param(), Some(&42.into()));
        }

        pub(super) async fn send_message_fails(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;

            cli_cmd_tx.send_message(RpcMessage::new_request(
                    "path/test",
                    "test_method",
                    Some(42.into())))
                .expect("Client command send");

            let msg = conn_mock.expect_send_message().await;

            assert!(msg.is_request());
            assert_eq!(msg.shv_path(), Some("path/test"));
            assert_eq!(msg.method(), Some("test_method"));
            assert_eq!(msg.param(), Some(&RpcValue::from(41)));
        }

        async fn receive_rpc_msg(rx: &mut Receiver<RpcFrame>) -> RpcMessage {
            rx.next().await.unwrap().to_rpcmesage().unwrap()
        }

        async fn receive_notification(rx: &mut Subscriber) -> RpcMessage {
            rx.next().await.unwrap().to_rpcmesage().unwrap()
        }

        pub(super) async fn call_method_and_receive_response(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut resp_rx = cli_cmd_tx
                .do_rpc_call("path/to/resource", "get")
                .expect("RpcCall command send");

            let req = conn_mock.expect_send_message().await;
            conn_mock.emulate_receive_response(&req, 42);

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_response());
            assert_eq!(resp.result().unwrap(), &RpcValue::from(42));
        }

        pub(super) async fn call_method_timeouts_when_disconnected(
            _conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut _cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut resp_rx = cli_cmd_tx
                .do_rpc_call("path/to/resource", "get")
                .expect("RpcCall command send");
            receive_rpc_msg(&mut resp_rx).timeout(Duration::from_millis(1000)).await.expect_err("Unexpected method call response");
        }

        async fn check_notification_received(
            notify_rx: &mut Subscriber,
            path: Option<&str>,
            method: Option<&str>,
            param: Option<&RpcValue>,
        ) {
            let received_msg = receive_notification(notify_rx)
                .timeout(Duration::from_millis(3000)).await
                .unwrap_or_else(|_| panic!("Notification for path `{:?}`, signal `{:?}`, param `{:?}` not received", &path, &method, &param));
            assert!(received_msg.is_signal());
            assert_eq!(received_msg.shv_path(), path);
            assert_eq!(received_msg.method(), method);
            assert_eq!(received_msg.param(), param);
        }

        pub(super) async fn receive_subscribed_notification(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut notify_rx = cli_cmd_tx
                .subscribe("path/to/resource", SIG_CHNG)
                .expect("ClientCommand subscribe send");
            conn_mock.expect_send_message()
                .timeout(Duration::from_millis(1000)).await
                .expect("Subscribe request timeout");

            let mut notify_rx_dup = cli_cmd_tx
                .subscribe("path/to/resource", SIG_CHNG)
                .expect("ClientCommand subscribe send");

            let mut notify_rx_prefix = cli_cmd_tx
                .subscribe("path/to", SIG_CHNG)
                .expect("ClientCommand subscribe send");
            conn_mock.expect_send_message()
                .timeout(Duration::from_millis(1000)).await
                .expect("Subscribe request timeout");

            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(42.into()));
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(43.into()));
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("bar".into()));
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("baz".into()));
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx_dup, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx_prefix, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
        }

        pub(super) async fn do_not_receive_unsubscribed_notification(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut notify_rx = cli_cmd_tx
                .subscribe("path/to/resource", SIG_CHNG)
                .expect("ClientCommand subscribe send");

            conn_mock.expect_send_message()
                .timeout(Duration::from_millis(1000)).await
                .expect("Subscribe request timeout");

            // Path mismatch
            conn_mock.emulate_receive_signal("path/to/resource2", SIG_CHNG, Some(42.into()));
            conn_mock.emulate_receive_signal("path/to/res", SIG_CHNG, Some(42.into()));
            // Signal mismatch
            conn_mock.emulate_receive_signal("path/to/resource", "mntchng", Some(42.into()));

            receive_notification(&mut notify_rx)
                .timeout(Duration::from_millis(1000)).await
                .expect_err("Unexpected notification received");
        }

        pub(super) async fn subscribe_and_unsubscribe(
            conn_evt_tx: Sender<ConnectionEvent>,
            cli_cmd_tx: ClientCommandSender,
            mut cli_evt_rx: ClientEventsReceiver,
        ) {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut notify_rx_1 = cli_cmd_tx
                .subscribe("path/to/resource", SIG_CHNG)
                .expect("ClientCommand subscribe send");

            conn_mock.expect_send_message()
                .timeout(Duration::from_millis(1000)).await
                .expect("Subscribe request timeout");

            let mut notify_rx_2 = cli_cmd_tx
                .subscribe("path/to/resource", SIG_CHNG)
                .expect("ClientCommand subscribe send");

            // Sync with the client task to ensure that the subscription
            // has been registered before emulating signals receive.
            cli_cmd_tx.send_message(RpcMessage::default()).expect("Send sync message");
            conn_mock.expect_send_message()
                .timeout(Duration::from_millis(1000)).await
                .expect("Sync msg timeout");

            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(42.into()));
            check_notification_received(&mut notify_rx_1, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;

            drop(notify_rx_1);
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("bar".into()));
            check_notification_received(&mut notify_rx_2, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;

            drop(notify_rx_2);
            let unsubscribe_req = conn_mock.expect_send_message()
                .timeout(Duration::from_millis(1000)).await
                .expect("Unsubscribe request timeout");
            assert_eq!(unsubscribe_req.shv_path(), Some(BROKER_APP_NODE));
            assert_eq!(unsubscribe_req.method(), Some("unsubscribe"));
            let shvproto::Value::Map(params) = unsubscribe_req.param().expect("Unsubscribe request has param").value() else {
                panic!("Unsubscribe params is not a map");
            };
            assert_eq!(params.get("signal").map(shvproto::RpcValue::as_str), Some(SIG_CHNG));
            assert_eq!(params.get("paths").map(shvproto::RpcValue::as_str), Some("path/to/resource"));
        }

        // Request handling tests
        //
        pub(super) fn make_client_with_handlers() -> Client<()> {
            async fn methods_getter(path: String, _: Option<AppState<()>>) -> Option<Vec<&'static MetaMethod>> {
                if path.is_empty() {
                    Some(PROPERTY_METHODS.iter().collect())
                } else {
                    None
                }
            }

            async fn request_handler(rq: RpcMessage, client_cmd_tx: ClientCommandSender) {
                let mut resp = rq.prepare_response().unwrap();
                match rq.method() {
                    Some(crate::clientnode::METH_LS) => {
                        resp.set_result("ls");
                    },
                    Some(crate::clientnode::METH_GET) => {
                        resp.set_result("get");
                    },
                    Some(crate::clientnode::METH_SET) => {
                        resp.set_result("set");
                    },
                    _ => {
                        resp.set_error(RpcError::new(
                                RpcErrorCode::MethodNotFound,
                                format!("Unknown method '{:?}'", rq.method())));
                    }
                }
                client_cmd_tx.send_message(resp).unwrap();
            }

            let mut client = Client::new(DotAppNode::new("test"));
            client.mount_dynamic("dynamic/sync",
                                 MethodsGetter::new(methods_getter),
                                 RequestHandler::stateless(request_handler));
            client.mount_dynamic("dynamic/async",
                                 MethodsGetter::new(methods_getter),
                                 RequestHandler::stateless(request_handler));
            client.mount_fixed("static",
                                PROPERTY_METHODS.iter(),
                                [Route::new([crate::clientnode::METH_GET, crate::clientnode::METH_SET],
                                            RequestHandler::stateless(request_handler))]);
            client
        }

        async fn recv_request_get_response(conn_mock: &mut ConnectionMock, request: RpcMessage) -> RpcMessage {
            conn_mock.emulate_receive_request(request);
            conn_mock.expect_send_message().await
        }

        pub(super) async fn handle_method_calls(conn_evt_tx: Sender<ConnectionEvent>,
                                         _cli_cmd_tx: ClientCommandSender,
                                         mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;

            {
                // Nonexisting method or path
                let request = RpcMessage::new_request("dynamic/a", "dir", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .result().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound);

                let request = RpcMessage::new_request("dynamic/sync", "bar", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .result().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound);

                let request = RpcMessage::new_request("static/none", "dir", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .result().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound);

                let request = RpcMessage::new_request("static", "foo", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .result().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::MethodNotFound);
            }

            {
                // Access level is missing
                let request = RpcMessage::new_request("dynamic/async", "dir", None);
                let response = recv_request_get_response(&mut conn_mock, request).await
                    .result().expect_err("Response should be Err");
                assert_eq!(response.code, RpcErrorCode::InvalidRequest);
            }

            {
                // Requests to a valid method with sufficient permissions
                let mut request = RpcMessage::new_request("static", "get", None);
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.result().expect("Response should be Ok").as_str(), "get");

                let mut request = RpcMessage::new_request("dynamic/sync", "set", None);
                request.set_access_level(AccessLevel::Service);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.result().expect("Response should be Ok").as_str(), "set");

                let mut request = RpcMessage::new_request("dynamic/async", "get", None);
                request.set_access_level(AccessLevel::Superuser);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.result().expect("Response should be Ok").as_str(), "get");

                let mut request = RpcMessage::new_request("dynamic/async", "dir", None);
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.result().expect("Response should be Ok").as_list().len(), 5);
            }

            {
                // Insufficient permissions
                let mut request = RpcMessage::new_request("static", "set", None);
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.result().expect_err("Response should be Err").code, RpcErrorCode::PermissionDenied);

                let mut request = RpcMessage::new_request("dynamic/sync", "set", None);
                request.set_access_level(AccessLevel::Read);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.result().expect_err("Response should be Err").code, RpcErrorCode::PermissionDenied);

                let mut request = RpcMessage::new_request("dynamic/async", "get", None);
                request.set_access_level(AccessLevel::Browse);
                let response = recv_request_get_response(&mut conn_mock, request).await;
                assert_eq!(response.result().expect_err("Response should be Err").code, RpcErrorCode::PermissionDenied);
            }
        }
    }

    macro_rules! def_test{
        ($name:ident $(#[$attr:meta])* $(,$client:expr)?) => {
            mk_test_fn_args!($name $(#[$attr])* $(,$client)?);
        };
    }

    macro_rules! mk_test_fn_args {
        ($name:ident $(#[$attr:meta])* , $client:expr) => {
            mk_test_fn!($name ($(#[$attr])*) Some($client));
        };
        ($name:ident $(#[$attr:meta])*) => {
            mk_test_fn!($name ($(#[$attr])*) None::<$crate::Client<()>>);
        };
    }

    macro_rules! mk_test_fn {
        ($name:ident ($(#[$attr:meta])*) $client_opt:expr) => {

            #[test]
            $(#[$attr])*
            fn $name() {
                run_test($crate::client::tests::drivers::$name, $client_opt);
            }
        };
    }

    generics_def!(TestDriverBounds <C, F, S> where
                  C: FnOnce(Sender<ConnectionEvent>, ClientCommandSender, ClientEventsReceiver) -> F,
                  F: Future + Send + 'static,
                  F::Output: Send + 'static,
                  S: Sync + Send + 'static,
                  );


    macro_rules! def_tests {
        ($($name:ident $(#[$attr:meta])* $(($client:expr))?),+) => {

            #[cfg(feature = "tokio")]
            mod tokio {
                use super::*;
                use crate::appnodes::DotAppNode;

                $(def_test!($name $(#[$attr])* $(,$client)?);)+

                #[generics(TestDriverBounds)]
                async fn init_client(test_drv: C, custom_client: Option<Client<S>>) {
                    let mut client = if let Some(client) = custom_client {
                        client
                    } else {
                        Client::new(DotAppNode::new("test"))
                    };
                    let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
                    let (join_handle_tx, mut join_handle_rx) = futures::channel::mpsc::unbounded();
                    let init_handler = move |cli_cmd_tx, cli_evt_rx| {
                        let join_test_handle = ::tokio::task::spawn(test_drv(conn_evt_tx, cli_cmd_tx, cli_evt_rx));
                        join_handle_tx.unbounded_send(join_test_handle).unwrap();
                    };
                    client.client_loop(conn_evt_rx, Some(init_handler)).await.expect("Client loop terminated with an error");
                    let join_handle = join_handle_rx.next().await.expect("fetch test join handle");
                    join_handle.await.expect("Test finished with error");
                }

                #[generics(TestDriverBounds)]
                pub fn run_test(test_drv: C, custom_client: Option<Client<S>>) {
                    let _ = simple_logger::init_with_level(Level::Debug);

                    ::tokio::runtime::Builder::new_multi_thread()
                        .build()
                        .unwrap()
                        .block_on(init_client(test_drv, custom_client));
                }
            }

            #[cfg(feature = "async_std")]
            mod async_std {
                use crate::appnodes::DotAppNode;
                use super::*;

                $(def_test!($name $(#[$attr])* $(,$client)?);)+

                #[generics(TestDriverBounds)]
                async fn init_client(test_drv: C, custom_client: Option<Client<S>>) {
                    let mut client = if let Some(client) = custom_client {
                        client
                    } else {
                        Client::new(DotAppNode::new("test"))
                    };
                    let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
                    let (join_handle_tx, mut join_handle_rx) = futures::channel::mpsc::unbounded();
                    let init_handler = move |cli_cmd_tx, cli_evt_rx| {
                        let join_test_handle = ::async_std::task::spawn(test_drv(conn_evt_tx, cli_cmd_tx, cli_evt_rx));
                        join_handle_tx.unbounded_send(join_test_handle).unwrap();
                    };
                    client.client_loop(conn_evt_rx, Some(init_handler)).await.expect("Client loop terminated with an error");
                    let join_handle = join_handle_rx.next().await.expect("fetch test join handle");
                    join_handle.await; //.expect("Test finished with error");
                }

                #[generics(TestDriverBounds)]
                pub fn run_test(test_drv: C, custom_client: Option<Client<S>>) {
                    let _ = simple_logger::init_with_level(Level::Debug);

                    ::async_std::task::block_on(init_client(test_drv, custom_client));
                }
            }
        };
    }

    use drivers::make_client_with_handlers;

    def_tests! {
        receive_connected_and_disconnected_events,
        send_message,
        send_message_fails #[should_panic],
        call_method_timeouts_when_disconnected,
        call_method_and_receive_response,
        receive_subscribed_notification,
        do_not_receive_unsubscribed_notification,
        subscribe_and_unsubscribe,
        handle_method_calls (make_client_with_handlers())
    }

}
