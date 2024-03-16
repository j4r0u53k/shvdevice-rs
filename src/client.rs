use crate::connection::{spawn_connection_task, ConnectionCommand, ConnectionEvent};
use crate::shvnode::{find_longest_prefix, process_local_dir_ls, ShvNode};
use async_broadcast::RecvError;
use futures::future::BoxFuture;
use futures::{select, FutureExt, StreamExt};
use log::*;
use shv::client::ClientConfig;
use shv::metamethod::MetaMethod;
use shv::rpcframe::RpcFrame;
use shv::rpcmessage::{RpcError, RpcErrorCode};
use shv::{make_map, rpcvalue, RpcMessage, RpcMessageMetaTags, RpcValue};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;

const METH_SUBSCRIBE: &str = "subscribe";
const METH_UNSUBSCRIBE: &str = "unsubscribe";

pub type Sender<K> = futures::channel::mpsc::UnboundedSender<K>;
pub type Receiver<K> = futures::channel::mpsc::UnboundedReceiver<K>;

type BroadcastReceiver<K> = async_broadcast::Receiver<K>;

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

pub type MethodsGetter<T> = Box<dyn Fn(String, Option<Arc<T>>) -> BoxFuture<'static, Option<Vec<&'static MetaMethod>>> + Sync + Send>;
pub type RequestHandler<T> = Box<dyn Fn(RpcMessage, Sender<ClientCommand>, Option<Arc<T>>) -> BoxFuture<'static, ()> + Sync + Send>;

pub struct Route<T> {
    pub handler: RequestHandler<T>,
    pub methods: Vec<String>,
}

#[macro_export]
macro_rules! handler {
    ($func:ident) => {
        Box::new(move |req, tx, data| Box::pin($func(req, tx, data)))
    };
}

#[macro_export]
macro_rules! handler_stateless {
    ($func:ident) => {
        Box::new(move |req, tx, _data| Box::pin($func(req, tx)))
    };
}

#[macro_export]
macro_rules! methods_getter {
    ($func:ident) => {
        Box::new(move |path, data| Box::pin($func(path, data)))
    };
}

impl<T> Route<T> {
    pub fn new<I>(methods: I, handler: RequestHandler<T>) -> Self
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

pub struct Client<T> {
    mounts: BTreeMap<String, ShvNode<'static, T>>,
    app_data: Option<Arc<T>>,
}

pub enum ProcessRequestMode {
    ProcessInCurrentTask,
    ProcessInExtraTask,
}

impl<T: Send + Sync + 'static> Client<T> {
#[allow(clippy::new_without_default)]
    // FIXME: Provide a constructor for plain client, device and so on
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
        R: IntoIterator<Item = Route<T>>,
    {
        let path = path.as_ref();
        let node = ShvNode::new_static(defined_methods, routes);
        self.mounts.insert(path.into(), node);
        self
    }

    pub fn mount_dynamic<P: AsRef<str>>(&mut self, path: P, methods_getter: MethodsGetter<T>, request_handler: RequestHandler<T>, proc_req_mode: ProcessRequestMode) -> &mut Self {
        let path = path.as_ref();
        let node = match proc_req_mode {
            ProcessRequestMode::ProcessInCurrentTask => ShvNode::new_dynamic(methods_getter, request_handler),
            ProcessRequestMode::ProcessInExtraTask => ShvNode::new_dynamic_spawned(methods_getter, request_handler),
        };
        self.mounts.insert(path.into(), node);
        self
    }

    pub fn with_app_data(&mut self, app_data: Arc<T>) -> &mut Self {
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

        let (client_cmd_tx, mut client_cmd_rx) = futures::channel::mpsc::unbounded();
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
                                if subscriptions.remove(&path).is_none() {
                                    warn!("No subscription found for path `{}`", &path);
                                }
                                let request = create_subscription_request(&path, SubscriptionRequest::Unsubscribe);
                                client_cmd_tx
                                    .unbounded_send(SendMessage { message: request })
                                    .expect("Cannot send subscription request through ClientCommand channel");
                            },
                        }
                        next_client_cmd = client_cmd_rx.next().fuse();
                    },
                    None => {
                        panic!("Couldn't get ClientCommand from the channel");
                    },
                },
                conn_event_result = next_conn_event => match conn_event_result {
                    Some(conn_event) => {
                        use ConnectionEvent::*;
                        match conn_event {
                            RpcFrameReceived(frame) => {
                                self.process_rpc_frame(frame, &client_cmd_tx, &mut pending_rpc_calls, &mut subscriptions)
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
                        next_conn_event = conn_events_rx.next().fuse();
                    }
                    None => {
                        warn!("Connection task terminated, exiting");
                        return Ok(());
                    }
                },
            }
        }
    }

    async fn process_rpc_frame(
        &self,
        frame: RpcFrame,
        client_cmd_tx: &Sender<ClientCommand>,
        pending_rpc_calls: &mut HashMap<i64, Sender<RpcFrame>>,
        subscriptions: &mut HashMap<String, Sender<RpcFrame>>,
    ) -> shv::Result<()> {
        if frame.is_request() {
            if let Ok(mut request_msg) = frame.to_rpcmesage() {
                if let Ok(mut resp) = request_msg.prepare_response() {
                    let shv_path = frame.shv_path().unwrap_or_default();
                    let local_result = process_local_dir_ls(&self.mounts, &frame);
                    match local_result {
                        None => {
                            if let Some((mount, path)) = find_longest_prefix(&self.mounts, shv_path) {
                                request_msg.set_shvpath(path);
                                let node = self.mounts.get(mount).unwrap_or_else(|| panic!("A node on path '{mount}' should exist"));
                                node.process_request(request_msg, mount.to_owned(), client_cmd_tx.clone(), &self.app_data).await;
                            } else {
                                let method = frame.method().unwrap_or_default();
                                resp.set_error(RpcError::new(
                                    RpcErrorCode::MethodNotFound,
                                    format!("Invalid shv path {shv_path}:{method}()"),
                                ));
                                client_cmd_tx.unbounded_send(ClientCommand::SendMessage { message: resp })?;
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
                    if response_sender.unbounded_send(frame.clone()).is_err() {
                        warn!(
                            "Response channel closed before received response: {}",
                            &frame
                        )
                    }
                }
            }
        } else if frame.is_signal() {
            if let Some(path) = frame.shv_path() {
                if let Some((subscribed_path, _)) = find_longest_prefix(subscriptions, path) {
                    let notifications_sender = subscriptions.get(subscribed_path).unwrap();
                    let subscribed_path = subscribed_path.to_owned();
                    if notifications_sender.unbounded_send(frame).is_err() {
                        warn!("Notification channel for path `{}` closed while subscription still active. Automatically unsubscribing.", &subscribed_path);
                        subscriptions.remove(&subscribed_path);
                        let request = create_subscription_request(
                            &subscribed_path,
                            SubscriptionRequest::Unsubscribe,
                        );
                        client_cmd_tx.unbounded_send(ClientCommand::SendMessage { message: request })?;
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
        use crate::shvnode::{SIG_CHNG, PROPERTY_METHODS};
        use shv::metamethod::AccessLevel;

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
            assert_eq!(msg.param(), Some(&42.into()));
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

        // Request handling tests
        //
        pub fn make_client_with_handlers() -> Client<()> {
            async fn methods_getter(path: String, _: Option<Arc<()>>) -> Option<Vec<&'static MetaMethod>> {
                if path.is_empty() {
                    Some(PROPERTY_METHODS.iter().collect())
                } else {
                    None
                }
            }
            async fn request_handler(rq: RpcMessage, client_cmd_tx: Sender<ClientCommand>) {
                let mut resp = rq.prepare_response().unwrap();
                match rq.method() {
                    Some(crate::shvnode::METH_LS) => {
                        resp.set_result("ls");
                    },
                    Some(crate::shvnode::METH_GET) => {
                        resp.set_result("get");
                    },
                    Some(crate::shvnode::METH_SET) => {
                        resp.set_result("set");
                    },
                    _ => {
                        resp.set_error(RpcError::new(
                                RpcErrorCode::MethodNotFound,
                                format!("Unknown method '{:?}'", rq.method())));
                    }
                }
                client_cmd_tx.unbounded_send(ClientCommand::SendMessage{ message: resp }).unwrap();
            }
            let mut client = Client::new();
            client.mount_dynamic("dynamic/sync",
                                 methods_getter!(methods_getter),
                                 handler_stateless!(request_handler),
                                 ProcessRequestMode::ProcessInCurrentTask);
            client.mount_dynamic("dynamic/async",
                                 methods_getter!(methods_getter),
                                 handler_stateless!(request_handler),
                                 ProcessRequestMode::ProcessInExtraTask);
            client.mount_static("static",
                                PROPERTY_METHODS.iter(),
                                [Route::new([crate::shvnode::METH_GET, crate::shvnode::METH_SET],
                                            handler_stateless!(request_handler))]);
            client
        }

        async fn recv_request_get_response(conn_mock: &mut ConnectionMock, request: RpcMessage) -> RpcMessage {
            conn_mock.emulate_receive_request(request);
            conn_mock.expect_send_message().await
        }

        pub async fn handle_method_calls(conn_evt_tx: Sender<ConnectionEvent>,
                                         _cli_cmd_tx: Sender<ClientCommand>,
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

    generics_def!(TestDriverBounds <C, F, S> where
                  C: FnOnce(Sender<ConnectionEvent>, Sender<ClientCommand>, ClientEventsReceiver) -> F,
                  F: Future + Send + 'static,
                  F::Output: Send + 'static,
                  S: Sync + Send + 'static,
                  );

    #[cfg(feature = "tokio")]
    pub mod tokio {
        use super::*;
        use super::drivers::make_client_with_handlers;

        def_test!(receive_connected_and_disconnected_events);
        def_test!(send_message);
        def_test_failing!(send_message_fails);
        def_test!(call_method_timeouts_when_disconnected);
        def_test!(call_method_and_receive_response);
        def_test!(receive_subscribed_notification);
        def_test!(do_not_receive_unsubscribed_notification);

        def_test!(handle_method_calls, make_client_with_handlers());

        #[generics(TestDriverBounds)]
        async fn init_client(test_drv: C, custom_client: Option<Client<S>>) {
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
        pub fn run_test(test_drv: C, custom_client: Option<Client<S>>) {
            ::tokio::runtime::Builder::new_multi_thread()
                .build()
                .unwrap()
                .block_on(init_client(test_drv, custom_client));
        }
    }

    #[cfg(feature = "async_std")]
    pub mod async_std {
        use super::*;
        use super::drivers::make_client_with_handlers;

        def_test!(receive_connected_and_disconnected_events);
        def_test!(send_message);
        def_test_failing!(send_message_fails);
        def_test!(call_method_timeouts_when_disconnected);
        def_test!(call_method_and_receive_response);
        def_test!(receive_subscribed_notification);
        def_test!(do_not_receive_unsubscribed_notification);

        def_test!(handle_method_calls, make_client_with_handlers());

        #[generics(TestDriverBounds)]
        async fn init_client(test_drv: C, custom_client: Option<Client<S>>) {
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
        pub fn run_test(test_drv: C, custom_client: Option<Client<S>>) {
            ::async_std::task::block_on(init_client(test_drv, custom_client));
        }
    }

}
