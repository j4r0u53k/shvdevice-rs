use std::any::Any;
use std::collections::{BTreeMap, HashSet};
use std::format;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use futures::Future;
use log::{warn, error, info};
use shv::metamethod::{Flag, MetaMethod};
use shv::{metamethod, RpcMessage, RpcMessageMetaTags, RpcValue, rpcvalue};
use shv::metamethod::Access;
use shv::rpcframe::RpcFrame;
use shv::rpcmessage::{RpcError, RpcErrorCode};

pub const DOT_LOCAL_GRANT: &str = "dot-local";
pub const DOT_LOCAL_DIR: &str = ".local";
pub const DOT_LOCAL_HACK: &str = "dot-local-hack";

pub enum DirParam {
    Brief,
    Full,
    BriefMethod(String),
}
impl From<Option<&RpcValue>> for DirParam {
    fn from(value: Option<&RpcValue>) -> Self {
        match value {
            Some(rpcval) => {
                if rpcval.is_string() {
                    DirParam::BriefMethod(rpcval.as_str().into())
                } else {
                    if rpcval.as_bool() {
                        DirParam::Full
                    } else {
                        DirParam::Brief
                    }
                }
            }
            None => {
                DirParam::Brief
            }
        }
    }
}

fn dir<'a>(methods: impl Iterator<Item=&'a MetaMethod>, param: DirParam) -> RpcValue {
    let mut result = RpcValue::null();
    let mut lst = rpcvalue::List::new();
    for mm in methods {
        match param {
            DirParam::Brief => {
                lst.push(mm.to_rpcvalue(metamethod::DirFormat::IMap));
            }
            DirParam::Full => {
                lst.push(mm.to_rpcvalue(metamethod::DirFormat::Map));
            }
            DirParam::BriefMethod(ref method_name) => {
                if &mm.name == method_name {
                    result = mm.to_rpcvalue(metamethod::DirFormat::IMap);
                    break;
                }
            }
        }
    }
    if result.is_null() {
        lst.into()
    } else {
        result
    }
}

pub enum LsParam {
    List,
    Exists(String),
}
impl From<Option<&RpcValue>> for LsParam {
    fn from(value: Option<&RpcValue>) -> Self {
        match value {
            Some(rpcval) => {
                if rpcval.is_string() {
                    LsParam::Exists(rpcval.as_str().into())
                } else {
                    LsParam::List
                }
            }
            None => {
                LsParam::List
            }
        }
    }
}

pub fn process_local_dir_ls<V>(mounts: &BTreeMap<String, V>, frame: &RpcFrame) -> Option<RequestResult> {
    let method = frame.method().unwrap_or_default();
    if !(method == METH_DIR || method == METH_LS) {
        return None
    }
    let shv_path = frame.shv_path().unwrap_or_default();
    let mut children_on_path = children_on_path(&mounts, shv_path);
    if let Some(children_on_path) = children_on_path.as_mut() {
        if frame.meta.get(DOT_LOCAL_HACK).is_some() {
            children_on_path.insert(0, DOT_LOCAL_DIR.into());
        }
    }
    let mount = find_longest_prefix(mounts, &shv_path);
    let is_mount_point = mount.is_some();
    let is_leaf = match &children_on_path {
        None => { is_mount_point }
        Some(dirs) => { dirs.is_empty() }
    };
    if children_on_path.is_none() && !is_mount_point {
        // path doesn't exist
        return Some(RequestResult::Error(RpcError::new(RpcErrorCode::MethodNotFound, &format!("Invalid shv path: {}", shv_path))))
    }
    if method == METH_DIR && !is_mount_point {
        // dir in the middle of the tree must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let dir = dir(DIR_LS_METHODS.iter().into_iter(), rpcmsg.param().into());
            return Some(RequestResult::Response(dir))
        } else {
            return Some(RequestResult::Error(RpcError::new(RpcErrorCode::InvalidRequest, &format!("Cannot convert RPC frame to Rpc message"))))
        }
    }
    if method == METH_LS && !is_leaf {
        // ls on not-leaf node must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let ls = ls_children_to_result(children_on_path, rpcmsg.param().into());
            return Some(ls)
        } else {
            return Some(RequestResult::Error(RpcError::new(RpcErrorCode::InvalidRequest, &format!("Cannot convert RPC frame to Rpc message"))))
        }
    }
    None
}
fn ls_children_to_result(children: Option<Vec<String>>, param: LsParam) -> RequestResult {
    match param {
        LsParam::List => {
            match children {
                None => {
                    RequestResult::Error(RpcError::new(RpcErrorCode::MethodCallException, "Invalid shv path"))
                }
                Some(dirs) => {
                    let res: rpcvalue::List = dirs.iter().map(|d| RpcValue::from(d)).collect();
                    RequestResult::Response(res.into())
                }
            }
        }
        LsParam::Exists(path) => {
            match children {
                None => {
                    RequestResult::Response(false.into())
                }
                Some(children) => {
                    RequestResult::Response(children.contains(&path).into())
                }
            }
        }
    }
}
pub fn children_on_path<V>(mounts: &BTreeMap<String, V>, path: &str) -> Option<Vec<String>> {
    let mut dirs: Vec<String> = Vec::new();
    let mut unique_dirs: HashSet<String> = HashSet::new();
    let mut dir_exists = false;
    for (key, _) in mounts.range(path.to_string()..) {
        if key.starts_with(path) {
            dir_exists = true;
            if path.is_empty()
                || key.len() == path.len()
                || key.as_bytes()[path.len()] == ('/' as u8)
            {
                if key.len() > path.len() {
                    let dir_rest_start = if path.is_empty() { 0 } else { path.len() + 1 };
                    let mut updirs = key[dir_rest_start..].split('/');
                    if let Some(dir) = updirs.next() {
                        if !unique_dirs.contains(dir) {
                            dirs.push(dir.to_string());
                            unique_dirs.insert(dir.to_string());
                        }
                    }
                }
            }
        } else {
            break;
        }
    }
    if dir_exists {
        Some(dirs)
    } else {
        None
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ls_mounts() {
        let mut mounts = BTreeMap::new();
        mounts.insert("a".into(), ());
        mounts.insert("a/1".into(), ());
        mounts.insert("b/2/C".into(), ());
        mounts.insert("b/2/D".into(), ());
        mounts.insert("b/3/E".into(), ());
        assert_eq!(super::children_on_path(&mounts, ""), Some(vec!["a".to_string(), "b".to_string()]));
        assert_eq!(super::children_on_path(&mounts, "a"), Some(vec!["1".to_string()]));
        assert_eq!(super::children_on_path(&mounts, "b/2"), Some(vec!["C".to_string(), "D".to_string()]));
    }
}
pub fn find_longest_prefix<'a, 'b, V>(map: &'a BTreeMap<String, V>, shv_path: &'b str) -> Option<(&'b str, &'b str)> {
    let mut path = &shv_path[..];
    let mut rest = "";
    loop {
        if map.contains_key(path) {
            return Some((path, rest))
        }
        if path.is_empty() {
            break;
        }
        if let Some(slash_ix) = path.rfind('/') {
            path = &shv_path[..slash_ix];
            rest = &shv_path[(slash_ix + 1)..];
        } else {
            path = "";
            rest = shv_path;
        };
    }
    None
}

type Sender<K> = async_std::channel::Sender<K>;
type Receiver<K> = async_std::channel::Receiver<K>;

#[derive(Clone,Default)]
pub struct DeviceState(Option<Arc<Mutex<Box<dyn Any + Send + Sync>>>>);

impl DeviceState {
    pub fn new<T: Any + Send + Sync>(val: T) -> Self {
        DeviceState(Some(Arc::new(Mutex::new(Box::new(val)))))
    }
}

#[derive(Clone)]
pub struct RequestData {
    pub mount_path: String,
    pub request: RpcMessage,
}

pub enum RpcCommand {
    SendMessage {
        message: RpcMessage,
    },
    RpcCall {
        request: RpcMessage,
        response_sender: Sender<RpcFrame>,
    }
}

pub enum RequestResult {
    Response(RpcValue),
    Error(RpcError),
}

pub enum HandlerOutcome {
    NotHandled,
    Handled,
}

type HandlerFn = Box<dyn Fn(RequestData, Sender<RpcCommand>, DeviceState) -> Pin<Box<dyn Future<Output = HandlerOutcome>>>>;

pub struct ShvNode {
    defined_methods: Vec<MetaMethod>,
    request_handler: HandlerFn,
}

impl ShvNode {
    pub fn new<F, O>(defined_methods: Vec<MetaMethod>, request_handler: F) -> ShvNode
        where
        F: 'static + Fn(RequestData, Sender<RpcCommand>, DeviceState) -> O,
        O: 'static + Future<Output = HandlerOutcome>,
    {
        ShvNode {
            defined_methods,
            request_handler: Box::new(move |r, s, d| Box::pin(request_handler(r, s, d)))
        }
    }

    fn common_methods(&self) -> Vec<& MetaMethod> {
        DIR_LS_METHODS.iter().collect()
    }

    pub fn methods(&self) -> Vec<&MetaMethod> {
        self.common_methods().into_iter().chain(self.defined_methods.iter()).collect()
    }

    pub fn is_request_granted(&self, rq: &RpcMessage) -> bool {
        let shv_path = rq.shv_path().unwrap_or_default();
        if shv_path.is_empty() {
            let level_str = rq.access().unwrap_or_default();
            for level_str in level_str.split(',') {
                if let Some(rq_level) = Access::from_str(level_str) {
                    let method = rq.method().unwrap_or_default();
                    for mm in self.methods().into_iter() {
                        if mm.name == method {
                            return rq_level >= mm.access
                        }
                    }
                }
            }
        }
        false
    }

    pub async fn process_request(&mut self, req_data: RequestData, rpc_command_sender: Sender<RpcCommand>, device_state: DeviceState) {
        match (self.request_handler)(req_data.clone(), rpc_command_sender.clone(), device_state).await {
            HandlerOutcome::NotHandled => {
                self.process_dir_ls(&req_data.request, rpc_command_sender).await;
            }
            HandlerOutcome::Handled => { /* NOP */ }
        }
    }

    async fn process_dir_ls(&mut self, rq: &RpcMessage, rpc_command_sender: Sender<RpcCommand>) {
        let mut resp = rq.prepare_response().unwrap_or_default(); // FIXME: better handling
        match self.dir_ls(rq) {
            RequestResult::Response(val) => {
                resp.set_result(val);
            }
            RequestResult::Error(err) => {
                resp.set_error(err);                }
        }
        if let Err(e) = rpc_command_sender.send(RpcCommand::SendMessage { message: resp }).await {
            error!("process_dir_ls: Cannot send response ({})", e);
        }
    }

    fn dir_ls(&mut self, rq: &RpcMessage) -> RequestResult {
        match rq.method() {
            Some(METH_DIR) => {
                self.process_dir(rq)
            }
            Some(METH_LS) => {
                self.process_ls(rq)
            }
            _ => {
                let errmsg = format!("Unknown method '{}:{}()', path.", rq.shv_path().unwrap_or_default(), rq.method().unwrap_or_default());
                warn!("{}", &errmsg);
                RequestResult::Error(RpcError::new(RpcErrorCode::MethodNotFound, &errmsg))
            }
        }
    }

    fn process_dir(&mut self, rq: &RpcMessage) -> RequestResult {
        let shv_path = rq.shv_path().unwrap_or_default();
        if shv_path.is_empty() {
            let resp = dir(self.methods().into_iter(), rq.param().into());
            RequestResult::Response(resp)
        } else {
            let errmsg = format!("Unknown method '{}:{}()', invalid path.", rq.shv_path().unwrap_or_default(), rq.method().unwrap_or_default());
            warn!("{}", &errmsg);
            RequestResult::Error(RpcError::new(RpcErrorCode::MethodNotFound, &errmsg))
        }
    }

    fn process_ls(&mut self, rq: &RpcMessage) -> RequestResult {
        let shv_path = rq.shv_path().unwrap_or_default();
        if shv_path.is_empty() {
            match LsParam::from(rq.param()) {
                LsParam::List => {
                    RequestResult::Response(rpcvalue::List::new().into())
                }
                LsParam::Exists(_path) => {
                    RequestResult::Response(false.into())
                }
            }
        } else {
            let errmsg = format!("Unknown method '{}:{}()', invalid path.", rq.shv_path().unwrap_or_default(), rq.method().unwrap_or(""));
            warn!("{}", &errmsg);
            RequestResult::Error(RpcError::new(RpcErrorCode::MethodNotFound, &errmsg))
        }
    }
}

pub const METH_DIR: &str = "dir";
pub const METH_LS: &str = "ls";
pub const METH_GET: &str = "get";
pub const METH_SET: &str = "set";
pub const SIG_CHNG: &str = "chng";
pub const METH_SHV_VERSION_MAJOR: &str = "shvVersionMajor";
pub const METH_SHV_VERSION_MINOR: &str = "shvVersionMinor";
pub const METH_NAME: &str = "name";
pub const METH_PING: &str = "ping";
pub const METH_VERSION: &str = "version";
pub const METH_SERIAL_NUMBER: &str = "serialNumber";

pub const DIR_LS_METHODS: [MetaMethod; 2] = [
    MetaMethod { name: METH_DIR, flags: Flag::None as u32, access: Access::Browse, param: "DirParam", result: "DirResult", description: "" },
    MetaMethod { name: METH_LS, flags: Flag::None as u32, access: Access::Browse, param: "LsParam", result: "LsResult", description: "" },
];
pub const PROPERTY_METHODS: [MetaMethod; 3] = [
    MetaMethod { name: METH_GET, flags: Flag::IsGetter as u32, access: Access::Browse, param: "", result: "", description: "" },
    MetaMethod { name: METH_SET, flags: Flag::IsSetter as u32, access: Access::Browse, param: "", result: "", description: "" },
    MetaMethod { name: SIG_CHNG, flags: Flag::IsSignal as u32, access: Access::Browse, param: "", result: "", description: "" },
];


pub fn app_node() -> ShvNode {
    ShvNode::new(APP_METHODS.into(), app_node_process_request)
}

pub struct AppNode {
    pub app_name: &'static str,
    pub shv_version_major: i32,
    pub shv_version_minor: i32,
}

const APP_METHODS: [MetaMethod; 4] = [
    MetaMethod { name: METH_SHV_VERSION_MAJOR, flags: Flag::IsGetter as u32, access: Access::Browse, param: "", result: "", description: "" },
    MetaMethod { name: METH_SHV_VERSION_MINOR, flags: Flag::IsGetter as u32, access: Access::Browse, param: "", result: "", description: "" },
    MetaMethod { name: METH_NAME, flags: Flag::IsGetter as u32, access: Access::Browse, param: "", result: "", description: "" },
    MetaMethod { name: METH_PING, flags: Flag::None as u32, access: Access::Browse, param: "", result: "", description: "" },
];

// TODO: define by a macro
const APP_NODE: AppNode = AppNode {
    app_name: "",
    shv_version_major: 3,
    shv_version_minor: 0,
};

async fn app_node_process_request(req_data: RequestData, rpc_command_sender: Sender<RpcCommand>, _device_state: DeviceState) -> HandlerOutcome {
    let rq = &req_data.request;
    if rq.shv_path().unwrap_or_default().is_empty() {
        let mut resp = rq.prepare_response().unwrap_or_default();
        let resp_value = match rq.method() {
            Some(METH_SHV_VERSION_MAJOR) => {
                Some(APP_NODE.shv_version_major.into())
            }
            Some(METH_SHV_VERSION_MINOR) => {
                Some(APP_NODE.shv_version_minor.into())
            }
            Some(METH_NAME) => {
                Some(RpcValue::from(APP_NODE.app_name))
            }
            Some(METH_PING) => {
                Some(().into())
            }
            _ => None,
        };
        if let Some(val) = resp_value {
            resp.set_result(val);
            if let Err(e) = rpc_command_sender.send(RpcCommand::SendMessage { message: resp }).await {
                error!("app_node_process_request: Cannot send response ({e})");
            }
            return HandlerOutcome::Handled;
        }
    }
    HandlerOutcome::NotHandled
}

pub struct AppDeviceNode {
    pub device_name: &'static str,
    pub version: &'static str,
    pub serial_number: Option<String>,
}

const APP_DEVICE_METHODS: [MetaMethod; 3] = [
    MetaMethod { name: METH_NAME, flags: Flag::IsGetter as u32, access: Access::Browse, param: "", result: "", description: "" },
    MetaMethod { name: METH_VERSION, flags: Flag::IsGetter as u32, access: Access::Browse, param: "", result: "", description: "" },
    MetaMethod { name: METH_SERIAL_NUMBER, flags: Flag::IsGetter as u32, access: Access::Browse, param: "", result: "", description: "" },
];

pub fn app_device_node() -> ShvNode {
    ShvNode::new(APP_DEVICE_METHODS.into(), app_device_node_process_request)
}

// TODO: define by a macro
pub const APP_DEVICE_NODE: AppDeviceNode = AppDeviceNode {
    device_name: "",
    version: "",
    serial_number: None,
};

async fn app_device_node_process_request(req_data: RequestData, rpc_command_sender: Sender<RpcCommand>, _device_state: DeviceState) -> HandlerOutcome {
    let rq = &req_data.request;
    if rq.shv_path().unwrap_or_default().is_empty() {
        let mut resp = rq.prepare_response().unwrap_or_default();
        let resp_value = match rq.method() {
            Some(METH_NAME) => {
                Some(RpcValue::from(APP_DEVICE_NODE.device_name))
            }
            Some(METH_VERSION) => {
                Some(RpcValue::from(APP_DEVICE_NODE.version))
            }
            Some(METH_SERIAL_NUMBER) => {
                match &APP_DEVICE_NODE.serial_number {
                    None => { Some(RpcValue::null()) }
                    Some(sn) => { Some(RpcValue::from(sn)) }
                }
            }
            _ => {
                None
            }
        };
        if let Some(val) = resp_value {
            resp.set_result(val);
            if let Err(e) = rpc_command_sender.send(RpcCommand::SendMessage { message: resp }).await {
                error!("app_device_node_process_request: Cannot send response ({e})");
            }
            return HandlerOutcome::Handled;
        }
    }
    HandlerOutcome::NotHandled
}

const METH_GET_DELAYED: &str = "getDelayed";

const DELAY_METHODS: [MetaMethod; 1] = [
    MetaMethod { name: METH_GET_DELAYED, flags: Flag::IsGetter as u32, access: Access::Browse, param: "", result: "", description: "" },
];

pub fn delay_node() -> ShvNode {
    ShvNode::new(DELAY_METHODS.into(), delay_node_process_request)
}

async fn delay_node_process_request(req_data: RequestData, rpc_command_sender: Sender<RpcCommand>, state: DeviceState) -> HandlerOutcome {
    let rq = &req_data.request;
    if rq.shv_path().unwrap_or_default().is_empty() {
        match rq.method() {
            Some(METH_GET_DELAYED) => {
                let state = state.0.expect("Missing state for delay node");
                let mut locked_state = state.lock().unwrap();
                let counter = locked_state.downcast_mut::<i32>().expect("Invalid state type for delay node");
                let ret_val = { *counter += 1; *counter };
                drop(locked_state);
                let mut resp = rq.prepare_response().unwrap_or_default();
                async_std::task::spawn(async move {
                    async_std::task::sleep(Duration::from_secs(3)).await;
                    resp.set_result(ret_val.into());
                    if let Err(e) = rpc_command_sender.send(RpcCommand::SendMessage { message: resp }).await {
                        error!("delay_node_process_request: Cannot send response ({e})");
                    }
                });
                return HandlerOutcome::Handled;
            }
            _ => { }
        }
    }
    HandlerOutcome::NotHandled
}
