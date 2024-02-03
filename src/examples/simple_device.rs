use std::sync::Arc;
use std::time::Duration;

use async_std::sync::Mutex;
use clap::Parser;
use log::*;
use shv::metamethod::{Flag, MetaMethod};
use shv::{RpcMessageMetaTags, RpcMessage};
use shv::{client::ClientConfig, util::parse_log_verbosity};
use shvdevice::appnodes::{
    app_device_node_routes, app_node_routes, APP_DEVICE_METHODS, APP_METHODS,
};
use shvdevice::shvnode::SIG_CHNG;
use shvdevice::{RequestData, Route, DeviceCommand, Sender, ShvDevice, DeviceEventsReceiver};
use simple_logger::SimpleLogger;

#[derive(Parser, Debug)]
//#[structopt(name = "device", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "SHV call")]
struct Opts {
    /// Config file path
    #[arg(long)]
    config: Option<String>,
    /// Create default config file if one specified by --config is not found
    #[arg(short, long)]
    create_default_config: bool,
    ///Url to connect to, example tcp://admin@localhost:3755?password=dj4j5HHb, localsocket:path/to/socket
    #[arg(short = 's', long)]
    url: Option<String>,
    #[arg(short = 'i', long)]
    device_id: Option<String>,
    /// Mount point on broker connected to, note that broker might not accept any path.
    #[arg(short, long)]
    mount: Option<String>,
    /// Device tries to reconnect to broker after this interval, if connection to broker is lost.
    /// Example values: 1s, 1h, etc.
    #[arg(short, long)]
    reconnect_interval: Option<String>,
    /// Client should ping broker with this interval. Broker will disconnect device, if ping is not received twice.
    /// Example values: 1s, 1h, etc.
    #[arg(long, default_value = "1m")]
    heartbeat_interval: String,
    /// Verbose mode (module, .)
    #[arg(short, long)]
    verbose: Option<String>,
}

fn init_logger(cli_opts: &Opts) {
    let mut logger = SimpleLogger::new();
    logger = logger.with_level(LevelFilter::Info);
    if let Some(module_names) = &cli_opts.verbose {
        for (module, level) in parse_log_verbosity(&module_names, module_path!()) {
            logger = logger.with_module_level(module, level);
        }
    }
    logger.init().unwrap();
}

fn load_client_config(cli_opts: &Opts) -> shv::Result<ClientConfig> {
    let mut config = if let Some(config_file) = &cli_opts.config {
        ClientConfig::from_file_or_default(config_file, cli_opts.create_default_config)?
    } else {
        Default::default()
    };
    if let Some(url) = &cli_opts.url {
        config.url = url.clone()
    }
    config.device_id = cli_opts.device_id.clone();
    config.mount = cli_opts.mount.clone();
    config.reconnect_interval = cli_opts.reconnect_interval.clone();
    config.heartbeat_interval = cli_opts.heartbeat_interval.clone();
    Ok(config)
}

const METH_GET_DELAYED: &str = "getDelayed";

const DELAY_METHODS: [MetaMethod; 1] = [MetaMethod {
    name: METH_GET_DELAYED,
    flags: Flag::IsGetter as u32,
    access: shv::metamethod::Access::Browse,
    param: "",
    result: "",
    description: "",
}];

struct State(Arc<Mutex<i32>>);

async fn delay_node_process_request(
    req_data: RequestData,
    rpc_command_sender: Sender<DeviceCommand>,
    state: &mut Option<State>,
) {
    let rq = &req_data.request;
    if rq.shv_path().unwrap_or_default().is_empty() {
        assert_eq!(rq.method(), Some(METH_GET_DELAYED));
        let mut counter = state.as_mut().expect("Missing state for delay node")
            .0.lock_arc().await;
        let mut resp = rq.prepare_response().unwrap_or_default();
        async_std::task::spawn(async move {
            let ret_val = {
                *counter += 1;
                *counter
            };
            drop(counter);
            async_std::task::sleep(Duration::from_secs(3)).await;
            resp.set_result(ret_val.into());
            if let Err(e) = rpc_command_sender
                .send(DeviceCommand::SendMessage { message: resp })
                .await
            {
                error!("delay_node_process_request: Cannot send response ({e})");
            }
        });
    }
}

fn delay_node_routes() -> Vec<Route<State>> {
    [Route::new(
        [METH_GET_DELAYED],
        shvdevice::handler!(delay_node_process_request),
    )]
    .into()
}

async fn emit_chng_task(mut dev_evt_rx: DeviceEventsReceiver) -> shv::Result<()> {
    info!("signal task started");
    let dev_cmd_tx = dev_evt_rx.wait_for_init().await;
    info!("signal task got the command channel");

    let mut cnt = 0;
    loop {
        async_std::task::sleep(Duration::from_secs(3)).await;
        let sig = RpcMessage::new_signal("status/delayed", SIG_CHNG, Some(cnt.into()));
        cnt += 1;
        dev_cmd_tx.send(DeviceCommand::SendMessage { message: sig }).await?;
        info!("signal task emits a value: {cnt}");
    }
}

pub(crate) fn main() -> shv::Result<()> {
    let cli_opts = Opts::parse();
    init_logger(&cli_opts);

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");

    let client_config = load_client_config(&cli_opts).expect("Invalid config");

    let counter = State(Arc::new(Mutex::new(-10)));

    let mut device = ShvDevice::new();
    let dev_evt_rx = device.events_receiver();

    async_std::task::spawn(emit_chng_task(dev_evt_rx));

    device
        .mount(".app", APP_METHODS, app_node_routes())
        .mount(".app/device", APP_DEVICE_METHODS, app_device_node_routes())
        .mount("status/delayed", DELAY_METHODS, delay_node_routes())
        .with_state(counter)
        .run(&client_config)
}
