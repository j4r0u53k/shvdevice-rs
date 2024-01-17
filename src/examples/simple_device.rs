
use shvdevice::ShvDevice;
use shvdevice::shvnode::{app_node,app_device_node,delay_node};
use clap::Parser;
use log::*;
use simple_logger::SimpleLogger;
use shv::{client::ClientConfig, util::parse_log_verbosity};

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
    if let Some(url) = &cli_opts.url { config.url = url.clone() }
    config.device_id = cli_opts.device_id.clone();
    config.mount = cli_opts.mount.clone();
    config.reconnect_interval = cli_opts.reconnect_interval.clone();
    config.heartbeat_interval = cli_opts.heartbeat_interval.clone();
    Ok(config)
}

pub(crate) fn main() -> shv::Result<()> {
    let cli_opts = Opts::parse();
    init_logger(&cli_opts);

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");

    let client_config = load_client_config(&cli_opts).expect("Invalid config");

    let counter = -10;

    ShvDevice::default()
        .mount(".app".into(), app_node())
        .mount(".app/device".into(), app_device_node())
        .mount("status/delayed".into(), delay_node())
        .register_state(counter)
        .run(&client_config)
}
