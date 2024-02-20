use crate::connection::{spawn_connection_task, ConnectionCommand, ConnectionEvent};
use crate::shvnode::{find_longest_prefix, process_local_dir_ls, ShvNode};
use async_broadcast::RecvError;
use futures::future::LocalBoxFuture;
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
                                self.process_rpc_frame(frame, &client_cmd_tx,  &mut pending_rpc_calls, &mut subscriptions)
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
                        warn!("Connection task terminated, exiting");
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn process_rpc_frame(
        &mut self,
        frame: RpcFrame,
        client_cmd_tx: &Sender<ClientCommand>,
        pending_rpc_calls: &mut HashMap<i64, Sender<RpcFrame>>,
        subscriptions: &mut HashMap<String, Sender<RpcFrame>>,
    ) -> shv::Result<()> {
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
                                let node = self.mounts.get(mount).unwrap();
                                node.process_request(
                                    RequestData {
                                        mount_path: mount.into(),
                                        request: rpcmsg,
                                    },
                                    client_cmd_tx.clone(),
                                    &mut self.app_data,
                                )
                                .await;
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

    macro_rules! test_def{
        ($name:ident) => {
            #[test]
            fn $name() {
                drivers::run_test(drivers::$name);
            }
        };
    }

    test_def!(wait_for_connect);
    test_def!(send_message);
    test_def!(make_rpc_call);
    test_def!(subscribe_and_notify);

    pub mod drivers {
        use futures::Future;
        use generics_alias::*;
        use crate::shvnode::SIG_CHNG;
        use super::super::*;
        // use futures_time::time::Duration;

        struct ConnectionMock {
            conn_cmd_rx: Receiver<ConnectionCommand>,
            conn_evt_tx: Sender<ConnectionEvent>,
        }

        impl ConnectionMock {
            fn receive_response(&self, from_request: &RpcMessage, result: Option<RpcValue>) {
            }

            fn receive_signal(&self, path: &str, sig_name: &str, param: Option<RpcValue>) {
            }

            async fn get_message_to_send(&mut self) -> RpcMessage {
                let Some(ConnectionCommand::SendMessage(msg)) = self.conn_cmd_rx.next().await else {
                    panic!("ConnectionCommand receive error");
                };
                msg
            }
        }

        async fn init_connection(conn_evt_tx: &Sender<ConnectionEvent>,
                                 cli_evt_rx: &mut ClientEventsReceiver) -> Receiver<ConnectionCommand>
        {
            let (conn_cmd_tx, conn_cmd_rx) = futures::channel::mpsc::unbounded::<ConnectionCommand>();
            conn_evt_tx.unbounded_send(ConnectionEvent::Connected(conn_cmd_tx)).expect("Connection event send error");

            let ClientEvent::Connected = cli_evt_rx.wait_for_event().await.expect("connected event") else {
                panic!("Unexpected client event");
            };
            conn_cmd_rx
        }

        pub async fn wait_for_connect(conn_evt_tx: Sender<ConnectionEvent>,
                                      _cli_cmd_tx: Sender<ClientCommand>,
                                      mut cli_evt_rx: ClientEventsReceiver)
        {
            init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
        }

        pub async fn send_message(conn_evt_tx: Sender<ConnectionEvent>,
                                  cli_cmd_tx: Sender<ClientCommand>,
                                  mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_cmd_rx = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;

            cli_cmd_tx.unbounded_send(ClientCommand::SendMessage {
                message: RpcMessage::new_request("path/test", "test_method", Some(42.into()))
            }).expect("Client command send");

            let Some(ConnectionCommand::SendMessage(msg)) = conn_cmd_rx.next().await else {
                panic!("ConnectionCommand receive error");
            };

            assert!(msg.is_request());
            assert_eq!(msg.shv_path(), Some("path/test"));
            assert_eq!(msg.method(), Some("test_method"));
            assert_eq!(msg.param(), Some(&RpcValue::from(42)));
        }

        fn do_rpc_call(cli_cmd_tx: &Sender<ClientCommand>, path: &str, method: &str, param: Option<RpcValue>) -> Receiver<RpcFrame> {
            let (resp_tx, resp_rx) = futures::channel::mpsc::unbounded();
            cli_cmd_tx.unbounded_send(ClientCommand::RpcCall {
                request: RpcMessage::new_request(path, method, param),
                response_sender: resp_tx,
            }).expect("RpcCall command send");
            resp_rx
        }

        pub async fn make_rpc_call(conn_evt_tx: Sender<ConnectionEvent>,
                                  cli_cmd_tx: Sender<ClientCommand>,
                                  mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_cmd_rx = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut resp_rx = do_rpc_call(&cli_cmd_tx, "path/to/resource", "get", None);

            let Some(ConnectionCommand::SendMessage(req)) = conn_cmd_rx.next().await else {
                panic!("ConnectionCommand receive error");
            };
            let mut resp = req.prepare_response().unwrap();
            resp.set_result(42);
            conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(resp.to_frame().unwrap())).unwrap();

            let resp = resp_rx.next().await.unwrap().to_rpcmesage().unwrap();
            assert!(resp.is_response());
            assert_eq!(resp.result().unwrap(), &RpcValue::from(42));
        }

        fn do_subscribe(cli_cmd_tx: &Sender<ClientCommand>, path: &str/*, signal_name: &str*/) -> Receiver<RpcFrame> {
            let (notify_tx, notify_rx) = futures::channel::mpsc::unbounded();
            cli_cmd_tx.unbounded_send(ClientCommand::Subscribe {
                path: path.to_string(),
                notifications_sender: notify_tx,
            }).expect("RpcCall command send");
            notify_rx
        }

        pub async fn subscribe_and_notify(conn_evt_tx: Sender<ConnectionEvent>,
                                           cli_cmd_tx: Sender<ClientCommand>,
                                           mut cli_evt_rx: ClientEventsReceiver)
        {
            let mut conn_cmd_rx = init_connection(&conn_evt_tx, &mut cli_evt_rx).await;
            let mut notify_rx = do_subscribe(&cli_cmd_tx, "path/to/resource");

            let Some(ConnectionCommand::SendMessage(_req)) = conn_cmd_rx.next().await else {
                panic!("ConnectionCommand receive error");
            };
            let sig_msg = RpcMessage::new_signal("path/to/resource", SIG_CHNG, Some(42.into()));
            conn_evt_tx.unbounded_send(ConnectionEvent::RpcFrameReceived(sig_msg.to_frame().unwrap())).unwrap();

            let received_sig = notify_rx.next().await.unwrap().to_rpcmesage().unwrap();
            assert!(received_sig.is_signal());
            assert_eq!(received_sig.shv_path(), Some("path/to/resource"));
            assert_eq!(received_sig.method(), Some(SIG_CHNG));
            assert_eq!(received_sig.param(), Some(&RpcValue::from(42)));
        }

        generics_def!(TestDriverBounds <C, F> where
                      C: FnOnce(Sender<ConnectionEvent>, Sender<ClientCommand>, ClientEventsReceiver) -> F,
                      F: Future + Send + 'static,
                      F::Output: Send + 'static,
                      );

        #[generics(TestDriverBounds)]
        async fn create_client(test_drv: C) {
            let mut client = Client::<()>::new();
            let (conn_evt_tx, conn_evt_rx) = futures::channel::mpsc::unbounded::<ConnectionEvent>();
            let (join_handle_tx, mut join_handle_rx) = futures::channel::mpsc::unbounded();
            let init_handler = move |cli_cmd_tx, cli_evt_rx| {
                let join_test_handle = tokio::task::spawn(test_drv(conn_evt_tx, cli_cmd_tx, cli_evt_rx));
                join_handle_tx.unbounded_send(join_test_handle).unwrap();
            };
            client.client_loop(conn_evt_rx, Some(init_handler)).await.expect("Client loop terminated with an error");
            let join_handle = join_handle_rx.next().await.expect("fetch test join handle");
            join_handle.await.expect("Test finished with error");
        }

        #[generics(TestDriverBounds)]
        pub fn run_test(test_drv: C) {
            #[cfg(feature = "tokio")]
            tokio::runtime::Builder::new_multi_thread()
                .build()
                .unwrap()
                .block_on(create_client(test_drv));
        }
    }
}
