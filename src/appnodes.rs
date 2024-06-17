
use crate::devicenode::{StandaloneNode, METH_PING};
use crate::client::{ClientCommand, RequestHandler, Route, Sender};
use log::error;
use shv::metamethod::{AccessLevel, Flag, MetaMethod};
use shv::{RpcMessageMetaTags, RpcMessage};

pub use shv::RpcValue;

pub const METH_SHV_VERSION_MAJOR: &str = "shvVersionMajor";
pub const METH_SHV_VERSION_MINOR: &str = "shvVersionMinor";
pub const METH_NAME: &str = "name";
pub const METH_VERSION: &str = "version";
pub const METH_SERIAL_NUMBER: &str = "serialNumber";

pub const SHV_VERSION_MAJOR: i32 = 3;
pub const SHV_VERSION_MINOR: i32 = 0;

pub struct AppNode {
    app_name: String,
    shv_version_major: i32,
    shv_version_minor: i32,
}

impl AppNode {
    pub fn new(app_name: impl Into<String>) -> Self {
        Self {
            app_name: app_name.into(),
            shv_version_major: SHV_VERSION_MAJOR,
            shv_version_minor: SHV_VERSION_MINOR,
        }
    }
}

impl StandaloneNode for AppNode {
    fn methods(&self) -> Vec<&MetaMethod> {
        APP_METHODS.iter().collect()
    }

    fn process_request(&self, request: &RpcMessage) -> Option<Result<RpcValue, shv::rpcmessage::RpcError>> {
        match request.method() {
            Some(crate::appnodes::METH_SHV_VERSION_MAJOR) => Some(self.shv_version_major.into()),
            Some(crate::appnodes::METH_SHV_VERSION_MINOR) => Some(self.shv_version_minor.into()),
            Some(crate::appnodes::METH_NAME) => Some(RpcValue::from(&self.app_name)),
            Some(crate::devicenode::METH_PING) => Some(().into()),
            _ => None,
        }.map(Ok)
    }
}

pub const APP_METHODS: [MetaMethod; 4] = [
    MetaMethod {
        name: METH_SHV_VERSION_MAJOR,
        flags: Flag::IsGetter as u32,
        access: AccessLevel::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_SHV_VERSION_MINOR,
        flags: Flag::IsGetter as u32,
        access: AccessLevel::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_NAME,
        flags: Flag::IsGetter as u32,
        access: AccessLevel::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_PING,
        flags: Flag::None as u32,
        access: AccessLevel::Browse,
        param: "",
        result: "",
        description: "",
    },
];

#[macro_export]
macro_rules! app_node {
    ($appname:literal) => {
        {
            async fn process_request(request: RpcMessage, client_cmd_tx: Sender<ClientCommand>) {
                if request.shv_path().unwrap_or_default().is_empty() {
                    let mut resp = request.prepare_response().unwrap_or_default();
                    let resp_value = match request.method() {
                        Some($crate::appnodes::METH_SHV_VERSION_MAJOR) => Some($crate::appnodes::SHV_VERSION_MAJOR.into()),
                        Some($crate::appnodes::METH_SHV_VERSION_MINOR) => Some($crate::appnodes::SHV_VERSION_MINOR.into()),
                        Some($crate::appnodes::METH_NAME) => Some($crate::appnodes::RpcValue::from($appname.to_string())),
                        Some($crate::devicenode::METH_PING) => Some(().into()),
                        _ => None,
                    };
                    if let Some(val) = resp_value {
                        resp.set_result(val);
                        if let Err(e) = client_cmd_tx.unbounded_send($crate::client::ClientCommand::SendMessage { message: resp }) {
                            error!("app_node_process_request: Cannot send response ({e})");
                        }
                    }
                }
            }
            $crate::devicenode::DeviceNode::new_static(
                &$crate::appnodes::APP_METHODS,
                [$crate::client::Route::new(
                    [
                    $crate::appnodes::METH_SHV_VERSION_MAJOR,
                    $crate::appnodes::METH_SHV_VERSION_MINOR,
                    $crate::appnodes::METH_NAME,
                    $crate::devicenode::METH_PING,
                    ],
                    $crate::RequestHandler::stateless(process_request)
                )]
            )

        }
    };
}

// TODO: Use for future implementation of standalone (info) nodes
struct AppDeviceNode {
    pub device_name: &'static str,
    pub version: &'static str,
    pub serial_number: Option<String>,
}

pub const APP_DEVICE_METHODS: [MetaMethod; 3] = [
    MetaMethod {
        name: METH_NAME,
        flags: Flag::IsGetter as u32,
        access: AccessLevel::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_VERSION,
        flags: Flag::IsGetter as u32,
        access: AccessLevel::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_SERIAL_NUMBER,
        flags: Flag::IsGetter as u32,
        access: AccessLevel::Browse,
        param: "",
        result: "",
        description: "",
    },
];

const APP_DEVICE_NODE: AppDeviceNode = AppDeviceNode {
    device_name: "",
    version: "",
    serial_number: None,
};

async fn app_device_node_process_request(request: RpcMessage, client_cmd_tx: Sender<ClientCommand>) {
    if request.shv_path().unwrap_or_default().is_empty() {
        let mut resp = request.prepare_response().unwrap_or_default();
        let resp_value = match request.method() {
            Some(METH_NAME) => Some(RpcValue::from(APP_DEVICE_NODE.device_name)),
            Some(METH_VERSION) => Some(RpcValue::from(APP_DEVICE_NODE.version)),
            Some(METH_SERIAL_NUMBER) => match &APP_DEVICE_NODE.serial_number {
                None => Some(RpcValue::null()),
                Some(sn) => Some(RpcValue::from(sn)),
            },
            _ => None,
        };
        if let Some(val) = resp_value {
            resp.set_result(val);
            if let Err(e) = client_cmd_tx.unbounded_send(ClientCommand::SendMessage { message: resp }) {
                error!("app_device_node_process_request: Cannot send response ({e})");
            }
        }
    }
}

pub fn app_device_node_routes<T>() -> Vec<Route<T>> {
    [Route::new(
        [METH_NAME, METH_VERSION, METH_SERIAL_NUMBER],
        RequestHandler::stateless(app_device_node_process_request),
    )]
    .into()
}
