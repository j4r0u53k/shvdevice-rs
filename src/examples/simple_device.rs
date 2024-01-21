use std::time::Duration;

use clap::Parser;
use log::*;
use shv::metamethod::{Flag, MetaMethod};
use shv::RpcMessageMetaTags;
use shv::{client::ClientConfig, util::parse_log_verbosity};
use shvdevice::appnodes::{
    app_device_node_routes, app_node_routes, APP_DEVICE_METHODS, APP_METHODS,
};
use shvdevice::{DeviceState, RequestData, Route, RpcCommand, Sender, ShvDevice};
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

async fn delay_node_process_request(
    req_data: RequestData,
    rpc_command_sender: Sender<RpcCommand>,
    state: DeviceState,
) {
    let rq = &req_data.request;
    if rq.shv_path().unwrap_or_default().is_empty() {
        match rq.method() {
            Some(METH_GET_DELAYED) => {
                let state = state.0.expect("Missing state for delay node");
                let mut locked_state = state.lock_arc().await;
                let counter = locked_state
                    .downcast_mut::<i32>()
                    .expect("Invalid state type for delay node");
                let ret_val = {
                    *counter += 1;
                    *counter
                };
                drop(locked_state);
                let mut resp = rq.prepare_response().unwrap_or_default();
                async_std::task::spawn(async move {
                    async_std::task::sleep(Duration::from_secs(3)).await;
                    resp.set_result(ret_val.into());
                    if let Err(e) = rpc_command_sender
                        .send(RpcCommand::SendMessage { message: resp })
                        .await
                    {
                        error!("delay_node_process_request: Cannot send response ({e})");
                    }
                });
            }
            _ => {}
        }
    }
}

fn delay_node_routes() -> Vec<Route> {
    [Route::new([METH_GET_DELAYED], delay_node_process_request)].into()
}

pub(crate) fn main() -> shv::Result<()> {
    let cli_opts = Opts::parse();
    init_logger(&cli_opts);

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");

    let client_config = load_client_config(&cli_opts).expect("Invalid config");

    let counter = -10;

    let mut device = ShvDevice::new();
    let _device_event_rx = device.event_receiver();
    device
        .mount(".app", APP_METHODS, app_node_routes())
        .mount(".app/device", APP_DEVICE_METHODS, app_device_node_routes())
        .mount("status/delayed", DELAY_METHODS, delay_node_routes())
        .register_state(counter)
        .run(&client_config)
}
