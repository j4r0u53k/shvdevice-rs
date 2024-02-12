use crate::shvnode::METH_PING;
use crate::client::{RequestData, Route, ClientCommand, Sender};
use log::error;
use shv::metamethod::{Access, Flag, MetaMethod};
use shv::{RpcMessageMetaTags, RpcValue};

const METH_SHV_VERSION_MAJOR: &str = "shvVersionMajor";
const METH_SHV_VERSION_MINOR: &str = "shvVersionMinor";
const METH_NAME: &str = "name";
const METH_VERSION: &str = "version";
const METH_SERIAL_NUMBER: &str = "serialNumber";

struct AppNode {
    pub app_name: &'static str,
    pub shv_version_major: i32,
    pub shv_version_minor: i32,
}

pub const APP_METHODS: [MetaMethod; 4] = [
    MetaMethod {
        name: METH_SHV_VERSION_MAJOR,
        flags: Flag::IsGetter as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_SHV_VERSION_MINOR,
        flags: Flag::IsGetter as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_NAME,
        flags: Flag::IsGetter as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_PING,
        flags: Flag::None as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
];

const APP_NODE: AppNode = AppNode {
    app_name: "",
    shv_version_major: 3,
    shv_version_minor: 0,
};

async fn app_node_process_request(
    req_data: RequestData,
    client_cmd_tx: Sender<ClientCommand>,
) {
    let rq = &req_data.request;
    if rq.shv_path().unwrap_or_default().is_empty() {
        let mut resp = rq.prepare_response().unwrap_or_default();
        let resp_value = match rq.method() {
            Some(METH_SHV_VERSION_MAJOR) => Some(APP_NODE.shv_version_major.into()),
            Some(METH_SHV_VERSION_MINOR) => Some(APP_NODE.shv_version_minor.into()),
            Some(METH_NAME) => Some(RpcValue::from(APP_NODE.app_name)),
            Some(METH_PING) => Some(().into()),
            _ => None,
        };
        if let Some(val) = resp_value {
            resp.set_result(val);
            if let Err(e) = client_cmd_tx
                // .send(DeviceCommand::SendMessage { message: resp })
                // .await
                .unbounded_send(ClientCommand::SendMessage { message: resp })
            {
                error!("app_node_process_request: Cannot send response ({e})");
            }
        }
    }
}

pub fn app_node_routes<S>() -> Vec<Route<S>> {
    [Route::new(
        [
            METH_SHV_VERSION_MAJOR,
            METH_SHV_VERSION_MINOR,
            METH_NAME,
            METH_PING,
        ],
        crate::handler_stateless!(app_node_process_request),
    )]
    .into()
}

struct AppDeviceNode {
    pub device_name: &'static str,
    pub version: &'static str,
    pub serial_number: Option<String>,
}

pub const APP_DEVICE_METHODS: [MetaMethod; 3] = [
    MetaMethod {
        name: METH_NAME,
        flags: Flag::IsGetter as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_VERSION,
        flags: Flag::IsGetter as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_SERIAL_NUMBER,
        flags: Flag::IsGetter as u32,
        access: Access::Browse,
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

async fn app_device_node_process_request(
    req_data: RequestData,
    client_cmd_tx: Sender<ClientCommand>,
) {
    let rq = &req_data.request;
    if rq.shv_path().unwrap_or_default().is_empty() {
        let mut resp = rq.prepare_response().unwrap_or_default();
        let resp_value = match rq.method() {
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
            if let Err(e) = client_cmd_tx
                // .send(DeviceCommand::SendMessage { message: resp })
                // .await
                .unbounded_send(ClientCommand::SendMessage { message: resp })
            {
                error!("app_device_node_process_request: Cannot send response ({e})");
            }
        }
    }
}

pub fn app_device_node_routes<S>() -> Vec<Route<S>> {
    [Route::new(
        [METH_NAME, METH_VERSION, METH_SERIAL_NUMBER],
        crate::handler_stateless!(app_device_node_process_request),
    )]
    .into()
}
