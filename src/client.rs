use crate::connection::{spawn_connection_task, ConnectionCommand, ConnectionEvent};
use crate::shvnode::{find_longest_prefix, process_local_dir_ls, ShvNode, check_request_access, RequestAccessResult};
use async_broadcast::RecvError;
use futures::channel::oneshot;
use futures::stream::FuturesUnordered;
use futures::{select, FutureExt, StreamExt};
use log::*;
use shv::broker::node::{METH_SUBSCRIBE, METH_UNSUBSCRIBE};
use shv::client::ClientConfig;
use shv::metamethod::MetaMethod;
use shv::rpcframe::RpcFrame;
use shv::rpcmessage::{RpcError, RpcErrorCode};
use shv::{make_map, rpcvalue, RpcMessage, RpcMessageMetaTags, RpcValue};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;

pub type Sender<K> = futures::channel::mpsc::UnboundedSender<K>;
pub type Receiver<K> = futures::channel::mpsc::UnboundedReceiver<K>;

type BroadcastReceiver<K> = async_broadcast::Receiver<K>;

type OneshotSender<T> = futures::channel::oneshot::Sender<T>;
type OneshotReceiver<T> = futures::channel::oneshot::Receiver<T>;

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

// pub type HandlerFn<S> = Box<
//     dyn for<'a> Fn(RequestData, Sender<ClientCommand>, &'a mut Option<S>) -> BoxFuture<'_, ()>,
// >;

pub type RequestHandler<S> = Box<dyn Fn(RequestData, Sender<ClientCommand>, &mut Option<S>)>;
pub type MethodsGetter<S> = Box<dyn Fn(&str, OneshotSender<Vec<&MetaMethod>>, &mut Option<S>)>;

pub struct Route<S> {
    pub handler: RequestHandler<S>,
    pub methods: Vec<String>,
}

#[macro_export]
macro_rules! handler {
    ($func:ident) => {
        // Box::new(move |r, s, t| Box::pin($func(r, s, t)))
        Box::new(move |r, s, t| $func(r, s, t))
    };
}

#[macro_export]
macro_rules! handler_stateless {
    ($func:ident) => {
        // Box::new(move |r, s, _t| Box::pin($func(r, s)))
        Box::new(move |r, s, _t| $func(r, s))
    };
}

impl<S> Route<S> {
    pub fn new<I>(methods: I, handler: RequestHandler<S>) -> Self
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
                }
                err => break err,
            }
        }
    }

    pub fn recv_event(&mut self) -> Pin<Box<async_broadcast::Recv<'_, ClientEvent>>> {
        self.0.recv()
    }
}

pub struct Client<S> {
    mounts: BTreeMap<String, ShvNode<'static, S>>,
    app_data: Option<S>,
}

type UnresolvedReqOutput = (RequestData, Result<Vec<&'static MetaMethod>, oneshot::Canceled>);

impl<S> Client<S> {
    pub fn new() -> Self {
        Self {
            mounts: Default::default(),
            app_data: Default::default(),
        }
    }

    pub fn mount_static<P, M, R>(&mut self, path: P, defined_methods: M, routes: R) -> &mut Self
    where
        P: AsRef<str>,
        M: IntoIterator<Item = &'static MetaMethod>,
        R: Into<Vec<Route<S>>>,
    {
        let path = path.as_ref();
        let node = ShvNode::new_static(defined_methods, routes);
        self.mounts.insert(path.into(), node);
        self
    }

    pub fn mount_dynamic<P: AsRef<str>>(&mut self, path: P, methods_getter: MethodsGetter<S>, request_handler: RequestHandler<S>) -> &mut Self {
        let path = path.as_ref();
        let node = ShvNode::new_dynamic(methods_getter, request_handler);
        self.mounts.insert(path.into(), node);
        self
    }

    pub fn with_app_data(&mut self, app_data: S) -> &mut Self {
        self.app_data = Some(app_data);
        self
    }

    async fn run_with_init_opt<H>(
        &mut self,
        config: &ClientConfig,
        init_handler: Option<H>,
    ) -> shv::Result<()>
    where
        H: FnOnce(Sender<ClientCommand>, ClientEventsReceiver),
    {
        let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
        spawn_connection_task(config, conn_evt_tx);
        self.client_loop(conn_evt_rx, init_handler).await
    }

    pub async fn run(&mut self, config: &ClientConfig) -> shv::Result<()> {
        self.run_with_init_opt(
            config,
            Option::<fn(Sender<ClientCommand>, ClientEventsReceiver)>::None,
        )
        .await
    }

    pub async fn run_with_init<H>(&mut self, config: &ClientConfig, handler: H) -> shv::Result<()>
    where
        H: FnOnce(Sender<ClientCommand>, ClientEventsReceiver),
    {
        self.run_with_init_opt(config, Some(handler)).await
    }

    async fn client_loop<H>(
        &mut self,
        mut conn_events_rx: Receiver<ConnectionEvent>,
        init_handler: Option<H>,
    ) -> shv::Result<()>
    where
        H: FnOnce(Sender<ClientCommand>, ClientEventsReceiver),
    {
        let mut pending_rpc_calls: HashMap<i64, Sender<RpcFrame>> = HashMap::new();
        let mut subscriptions: HashMap<String, Sender<RpcFrame>> = HashMap::new();
        let mut unresolved_requests = FuturesUnordered::new();

        let (client_cmd_tx, mut client_cmd_rx) =
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
                client_cmd_result = client_cmd_rx.next().fuse() => match client_cmd_result {
                    Some(client_cmd) => {
                        use ClientCommand::*;
                        match client_cmd {
                            SendMessage{message} => {
                                if let Some(ref conn_cmd_sender) = conn_cmd_sender {
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
                                client_cmd_tx.unbounded_send(SendMessage{ message: request })?;
                            },
                            Subscribe{path, /* methods, */ notifications_sender} => {
                                if subscriptions.insert(path.clone(), notifications_sender).is_some() {
                                    warn!("Path {} has already been subscribed!", &path);
                                }
                                let request = create_subscription_request(&path, SubscriptionRequest::Subscribe);
                                client_cmd_tx
                                    .unbounded_send(SendMessage { message: request })
                                    .expect("Cannot send subscription request through ClientCommand channel");
                            },
                            Unsubscribe{path} => {
                                if let None = subscriptions.remove(&path) {
                                    warn!("No subscription found for path `{}`", &path);
                                }
                                let request = create_subscription_request(&path, SubscriptionRequest::Unsubscribe);
                                client_cmd_tx
                                    .unbounded_send(SendMessage { message: request })
                                    .expect("Cannot send subscription request through ClientCommand channel");
                            },
                        }
                    },
                    None => {
                        panic!("Couldn't get ClientCommand from the channel");
                    },
                },
                conn_event_result = conn_events_rx.next().fuse() => match conn_event_result {
                    Some(conn_event) => {
                        // Ok(conn_event) => {
                        use ConnectionEvent::*;
                        match conn_event {
                            RpcFrameReceived(frame) => {
                                if let Some((rq_data, methods_res_rx)) = self.process_rpc_frame(
                                    frame,
                                    &client_cmd_tx,
                                    &mut pending_rpc_calls,
                                    &mut subscriptions)
                                    .expect("Cannot process RPC frame") {
                                        unresolved_requests.push(Self::mk_unresolved_rq(rq_data, methods_res_rx));
                                    }
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
                        warn!("Connection task terminated, exiting");
                        return Ok(());
                    }
                },
                (req_data, methods_result) = unresolved_requests.select_next_some() => {
                    self.resolve_request(methods_result, req_data, &client_cmd_tx)?;
                }
            }
        }
    }

    async fn mk_unresolved_rq(rq_data: RequestData, methods_res_rx: OneshotReceiver<Vec<&'static MetaMethod>>)
        -> UnresolvedReqOutput {
            (rq_data, methods_res_rx.await)
    }

    fn resolve_request(
        &mut self,
        methods_result: Result<Vec<&MetaMethod>, oneshot::Canceled>,
        request_data: RequestData,
        client_cmd_tx: &Sender<ClientCommand>) -> shv::Result<()>
    {
        match methods_result {
            Ok(methods) => { // Resolved
                match check_request_access(&request_data.request, methods) {
                    RequestAccessResult::Ok => {
                        let node = self.mounts.get(&request_data.mount_path)
                            .expect(&format!("node on path `{}` should exist", &request_data.mount_path));
                        node.process_request(request_data, client_cmd_tx.clone(), &mut self.app_data);
                    },
                    RequestAccessResult::Err(err) => {
                        let mut resp = request_data.request.prepare_response()
                            .expect("should be able to prepare response");
                        warn!("Check request access on path: {}/{}, error: {}",
                              &request_data.request.shv_path().unwrap_or_default(),
                              &request_data.mount_path,
                              err);
                        resp.set_error(err);
                        client_cmd_tx
                            .unbounded_send(ClientCommand::SendMessage { message: resp })?;
                    }
                }
            },
            Err(_) => { // methods_result tx closed before returning the result
                let mut resp = request_data.request.prepare_response()
                    .expect("should be able to prepare response");
                resp.set_error(RpcError::new(
                        RpcErrorCode::MethodCallCancelled,
                        &format!("Could not find any methods on shv path {}/{}",
                              &request_data.request.shv_path().unwrap_or_default(),
                              &request_data.mount_path),
                        ));
                client_cmd_tx
                    .unbounded_send(ClientCommand::SendMessage { message: resp })?;
            }
        }
        Ok(())
    }

    fn process_rpc_frame(
        &mut self,
        frame: RpcFrame,
        client_cmd_tx: &Sender<ClientCommand>,
        pending_rpc_calls: &mut HashMap<i64, Sender<RpcFrame>>,
        subscriptions: &mut HashMap<String, Sender<RpcFrame>>,
    ) -> shv::Result<Option<(RequestData, OneshotReceiver<Vec<&'static MetaMethod>>)>> {
        if frame.is_request() {
            if let Ok(mut rpcmsg) = frame.to_rpcmesage() {
                if let Ok(mut resp) = rpcmsg.prepare_response() {
                    let shv_path = frame.shv_path().unwrap_or_default();
                    let local_result = process_local_dir_ls(&self.mounts, &frame);
                    match local_result {
                        None => {
                            if let Some((mount, path)) =
                                find_longest_prefix(&self.mounts, &shv_path)
                            {
                                rpcmsg.set_shvpath(path);
                                let request_data = RequestData {
                                    mount_path: mount.into(),
                                    request: rpcmsg,
                                };
                                let node = self.mounts.get(mount).unwrap();
                                let mut methods_res_rx = node.methods(path, &mut self.app_data);
                                let methods_res = methods_res_rx.try_recv();
                                if let Ok(None) = methods_res {
                                    // Unresolved, will be resolved asynchronously
                                    return Ok(Some((request_data, methods_res_rx)));
                                }
                                let methods_res = methods_res.and_then(|opt| Ok(opt.expect("should be Some")));
                                self.resolve_request(methods_res, request_data, client_cmd_tx)?;
                            } else {
                                let method = frame.method().unwrap_or_default();
                                resp.set_error(RpcError::new(
                                    RpcErrorCode::MethodNotFound,
                                    &format!("Invalid shv path {}:{}()", shv_path, method),
                                ));
                                client_cmd_tx
                                    .unbounded_send(ClientCommand::SendMessage { message: resp })?;
                            }
                        }
                        Some(result) => {
                            match result {
                                RequestResult::Response(r) => {
                                    resp.set_result(r);
                                    client_cmd_tx.unbounded_send(ClientCommand::SendMessage {
                                        message: resp,
                                    })?;
                                }
                                RequestResult::Error(e) => {
                                    resp.set_error(e);
                                    client_cmd_tx.unbounded_send(ClientCommand::SendMessage {
                                        message: resp,
                                    })?;
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
                    if let Err(_) = response_sender.unbounded_send(frame.clone()) {
                        warn!(
                            "Response channel closed before received response: {}",
                            &frame
                        )
                    }
                }
            }
        } else if frame.is_signal() {
            if let Some(path) = frame.shv_path() {
                if let Some((subscribed_path, _)) = find_longest_prefix(subscriptions, &path) {
                    let notifications_sender = subscriptions.get(subscribed_path).unwrap();
                    let subscribed_path = subscribed_path.to_owned();
                    if let Err(_) = notifications_sender.unbounded_send(frame) {
                        warn!("Notification channel for path `{}` closed while subscription still active. Automatically unsubscribing.", &subscribed_path);
                        subscriptions.remove(&subscribed_path);
                        let request = create_subscription_request(
                            &subscribed_path,
                            SubscriptionRequest::Unsubscribe,
                        );
                        client_cmd_tx
                            .unbounded_send(ClientCommand::SendMessage { message: request })?;
                    }
                }
            }
        }
        Ok(None)
    }
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
        Some(make_map!("methods" => "", "path" => path).into()),
    )
}

#[cfg(test)]
mod tests {
    pub use super::*;
    use futures::Future;
    use generics_alias::*;

    pub mod drivers {
        use super::*;
        use futures_time::future::FutureExt;
        use futures_time::time::Duration;
        use crate::shvnode::SIG_CHNG;

        struct ConnectionMock {
            conn_evt_tx: Sender<ConnectionEvent>,
            conn_cmd_rx: Receiver<ConnectionCommand>,
        }

        impl Drop for ConnectionMock {
            fn drop(&mut self) {
                if let Err(_) = self.conn_evt_tx.unbounded_send(ConnectionEvent::Disconnected) {
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

        async fn init_connection(conn_evt_tx: &Sender<ConnectionEvent>,
                                 cli_evt_rx: &mut ClientEventsReceiver) -> ConnectionMock
        {
            let conn_mock = ConnectionMock::new(conn_evt_tx);
            expect_client_connected(cli_evt_rx).await;
            conn_mock
        }

        pub async fn receive_connected_and_disconnected_events(conn_evt_tx: Sender<ConnectionEvent>,
                                                               _cli_cmd_tx: Sender<ClientCommand>,
                                                               mut client_events_rx: ClientEventsReceiver)
        {
            {
                let _conn_mock = ConnectionMock::new(&conn_evt_tx);
                expect_client_connected(&mut client_events_rx).await;
            }
            expect_client_disconnected(&mut client_events_rx).await;

            let _conn_mock = ConnectionMock::new(&conn_evt_tx);
            expect_client_connected(&mut client_events_rx).await;
        }

        pub async fn send_message(conn_evt_tx: Sender<ConnectionEvent>,
                                  cli_cmd_tx: Sender<ClientCommand>,
                                  mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;

            cli_cmd_tx.unbounded_send(ClientCommand::SendMessage {
                message: RpcMessage::new_request("path/test", "test_method", Some(42.into()))
            }).expect("Client command send");

            let msg = conn_mock.expect_send_message().await;

            assert!(msg.is_request());
            assert_eq!(msg.shv_path(), Some("path/test"));
            assert_eq!(msg.method(), Some("test_method"));
            assert_eq!(msg.param(), Some(&RpcValue::from(42)));
        }

        pub async fn send_message_fails(conn_evt_tx: Sender<ConnectionEvent>,
                                        cli_cmd_tx: Sender<ClientCommand>,
                                        mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;

            cli_cmd_tx.unbounded_send(ClientCommand::SendMessage {
                message: RpcMessage::new_request("path/test", "test_method", Some(42.into()))
            }).expect("Client command send");

            let msg = conn_mock.expect_send_message().await;

            assert!(msg.is_request());
            assert_eq!(msg.shv_path(), Some("path/test"));
            assert_eq!(msg.method(), Some("test_method"));
            assert_eq!(msg.param(), Some(&RpcValue::from(41)));
        }

        fn do_rpc_call(cli_cmd_tx: &Sender<ClientCommand>, path: &str, method: &str, param: Option<RpcValue>) -> Receiver<RpcFrame> {
            let (resp_tx, resp_rx) = futures::channel::mpsc::unbounded();
            cli_cmd_tx.unbounded_send(ClientCommand::RpcCall {
                request: RpcMessage::new_request(path, method, param),
                response_sender: resp_tx,
            }).expect("RpcCall command send");
            resp_rx
        }

        async fn receive_rpc_msg(rx: &mut Receiver<RpcFrame>) -> RpcMessage {
            rx.next().await.unwrap().to_rpcmesage().unwrap()
        }

        pub async fn call_method_and_receive_response(conn_evt_tx: Sender<ConnectionEvent>,
                                                  cli_cmd_tx: Sender<ClientCommand>,
                                                  mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut resp_rx = do_rpc_call(&cli_cmd_tx, "path/to/resource", "get", None);

            let req = conn_mock.expect_send_message().await;
            conn_mock.emulate_receive_response(&req, 42);

            let resp = receive_rpc_msg(&mut resp_rx).await;
            assert!(resp.is_response());
            assert_eq!(resp.result().unwrap(), &RpcValue::from(42));
        }

        pub async fn call_method_timeouts_when_disconnected(_conn_evt_tx: Sender<ConnectionEvent>,
                                                            cli_cmd_tx: Sender<ClientCommand>,
                                                            mut _cli_evt_rx: ClientEventsReceiver)
        {
            let mut resp_rx = do_rpc_call(&cli_cmd_tx, "path/to/resource", "get", None);
            receive_rpc_msg(&mut resp_rx).timeout(Duration::from_millis(3000)).await.expect_err("Unexpected method call response");
        }

        fn do_subscribe(cli_cmd_tx: &Sender<ClientCommand>, path: &str/*, signal_name: &str*/) -> Receiver<RpcFrame> {
            let (notify_tx, notify_rx) = futures::channel::mpsc::unbounded();
            cli_cmd_tx.unbounded_send(ClientCommand::Subscribe {
                path: path.to_string(),
                notifications_sender: notify_tx,
            }).expect("RpcCall command send");
            notify_rx
        }

        async fn check_notification_received(notify_rx: &mut Receiver<RpcFrame>, path: Option<&str>, method: Option<&str>, param: Option<&RpcValue>) {
            let received_msg = receive_rpc_msg(notify_rx).await;
            assert!(received_msg.is_signal());
            assert_eq!(received_msg.shv_path(), path);
            assert_eq!(received_msg.method(), method);
            assert_eq!(received_msg.param(), param);
        }

        pub async fn receive_subscribed_notification(conn_evt_tx: Sender<ConnectionEvent>,
                                                     cli_cmd_tx: Sender<ClientCommand>,
                                                     mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut notify_rx = do_subscribe(&cli_cmd_tx, "path/to/resource");

            let _subscribe_req = conn_mock.expect_send_message().await;

            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(42.into()));
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some(43.into()));
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("bar".into()));
            conn_mock.emulate_receive_signal("path/to/resource", SIG_CHNG, Some("baz".into()));
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&42.into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&43.into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&"bar".into())).await;
            check_notification_received(&mut notify_rx, Some("path/to/resource"), Some(SIG_CHNG), Some(&"baz".into())).await;
        }

        pub async fn do_not_receive_unsubscribed_notification(conn_evt_tx: Sender<ConnectionEvent>,
                                                              cli_cmd_tx: Sender<ClientCommand>,
                                                              mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_mock = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut notify_rx = do_subscribe(&cli_cmd_tx, "path/to/resource");

            let _subscribe_req = conn_mock.expect_send_message().await;

            conn_mock.emulate_receive_signal("path/to/resource2", SIG_CHNG, Some(42.into()));

            receive_rpc_msg(&mut notify_rx)
                .timeout(Duration::from_millis(1000)).await
                .expect_err("Unexpected notification received");
        }

    }

    macro_rules! def_test{
        ($name:ident $(, $client:expr)?) => {
            mk_test_fn_args!($name [ ] $($client)?);
        };
    }

    macro_rules! def_test_failing{
        ($name:ident $(, $client:expr)?) => {
            mk_test_fn_args!($name [ #[should_panic] ] $($client)?);
        };
    }

    macro_rules! mk_test_fn_args {
        ($name:ident [ $(#[$attr:meta])* ] $client:expr) => {
            mk_test_fn!($name [ $(#[$attr])* ] Some($client));
        };
        ($name:ident [ $(#[$attr:meta])* ] ) => {
            mk_test_fn!($name [ $(#[$attr])* ] None::<$crate::Client<()>>);
        };
    }

    macro_rules! mk_test_fn {
        ($name:ident [ $(#[$attr:meta])* ] $client_opt:expr) => {
            #[test]
            $(#[$attr])*
            fn $name() {
                run_test($crate::client::tests::drivers::$name, $client_opt);
            }
        };
    }

    generics_def!(TestDriverBounds <C, F> where
                  C: FnOnce(Sender<ConnectionEvent>, Sender<ClientCommand>, ClientEventsReceiver) -> F,
                  F: Future + Send + 'static,
                  F::Output: Send + 'static,
                  );

    #[cfg(feature = "tokio")]
    pub mod tokio {
        use super::*;

        def_test!(receive_connected_and_disconnected_events);
        def_test!(send_message);
        def_test_failing!(send_message_fails);
        def_test!(call_method_timeouts_when_disconnected);
        def_test!(call_method_and_receive_response);
        def_test!(receive_subscribed_notification);
        def_test!(do_not_receive_unsubscribed_notification);

        #[generics(TestDriverBounds)]
        async fn init_client<S>(test_drv: C, custom_client: Option<Client<S>>) {
            let mut client = if let Some(client) = custom_client { client } else { Client::<S>::new() };
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
        pub fn run_test<S>(test_drv: C, custom_client: Option<Client<S>>) {
            ::tokio::runtime::Builder::new_multi_thread()
                .build()
                .unwrap()
                .block_on(init_client(test_drv, custom_client));
        }
    }

    #[cfg(feature = "async_std")]
    pub mod async_std {
        use super::*;

        def_test!(receive_connected_and_disconnected_events);
        def_test!(send_message);
        def_test_failing!(send_message_fails);
        def_test!(call_method_timeouts_when_disconnected);
        def_test!(call_method_and_receive_response);
        def_test!(receive_subscribed_notification);
        def_test!(do_not_receive_unsubscribed_notification);

        #[generics(TestDriverBounds)]
        async fn init_client<S>(test_drv: C, custom_client: Option<Client<S>>) {
            let mut client = if let Some(client) = custom_client { client } else { Client::<S>::new() };
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
        pub fn run_test<S>(test_drv: C, custom_client: Option<Client<S>>) {
            ::async_std::task::block_on(init_client(test_drv, custom_client));
        }
    }

}
