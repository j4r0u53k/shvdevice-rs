// The file originates from https://github.com/silicon-heaven/shv-rs/blob/e740fd301dc65f3412ad1154595bf61ee5632aba/src/shvnode.rs
// struct ShvNode has been adapted to support async process_request accepting RpcCommand channel and a shared state params

use crate::client::{ClientCommand, RequestHandler, RequestData, RequestResult, Route, Sender, MethodsGetter};
use futures::channel::oneshot;
use log::{error, warn};
use shv::metamethod::Access;
use shv::metamethod::{Flag, MetaMethod};
use shv::rpcframe::RpcFrame;
use shv::rpcmessage::{RpcError, RpcErrorCode};
use shv::{metamethod, rpcvalue, RpcMessage, RpcMessageMetaTags, RpcValue};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::format;
use std::rc::Rc;

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
            None => DirParam::Brief,
        }
    }
}

fn dir<'a>(methods: impl Iterator<Item = &'a MetaMethod>, param: DirParam) -> RpcValue {
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
            None => LsParam::List,
        }
    }
}

pub fn process_local_dir_ls<V>(
    mounts: &BTreeMap<String, V>,
    frame: &RpcFrame,
) -> Option<RequestResult> {
    let method = frame.method().unwrap_or_default();
    if !(method == METH_DIR || method == METH_LS) {
        return None;
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
        None => is_mount_point,
        Some(dirs) => dirs.is_empty(),
    };
    if children_on_path.is_none() && !is_mount_point {
        // path doesn't exist
        return Some(RequestResult::Error(RpcError::new(
            RpcErrorCode::MethodNotFound,
            &format!("Invalid shv path: {}", shv_path),
        )));
    }
    if method == METH_DIR && !is_mount_point {
        // dir in the middle of the tree must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let dir = dir(DIR_LS_METHODS.iter().into_iter(), rpcmsg.param().into());
            return Some(RequestResult::Response(dir));
        } else {
            return Some(RequestResult::Error(RpcError::new(
                RpcErrorCode::InvalidRequest,
                &format!("Cannot convert RPC frame to Rpc message"),
            )));
        }
    }
    if method == METH_LS && !is_leaf {
        // ls on not-leaf node must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let ls = ls_children_to_result(children_on_path, rpcmsg.param().into());
            return Some(ls);
        } else {
            return Some(RequestResult::Error(RpcError::new(
                RpcErrorCode::InvalidRequest,
                &format!("Cannot convert RPC frame to Rpc message"),
            )));
        }
    }
    None
}
fn ls_children_to_result(children: Option<Vec<String>>, param: LsParam) -> RequestResult {
    match param {
        LsParam::List => match children {
            None => RequestResult::Error(RpcError::new(
                RpcErrorCode::MethodCallException,
                "Invalid shv path",
            )),
            Some(dirs) => {
                let res: rpcvalue::List = dirs.iter().map(|d| RpcValue::from(d)).collect();
                RequestResult::Response(res.into())
            }
        },
        LsParam::Exists(path) => match children {
            None => RequestResult::Response(false.into()),
            Some(children) => RequestResult::Response(children.contains(&path).into()),
        },
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
    use crate::handler;

    use super::*;

    #[test]
    fn ls_mounts() {
        let mut mounts = BTreeMap::new();
        mounts.insert("a".into(), ());
        mounts.insert("a/1".into(), ());
        mounts.insert("b/2/C".into(), ());
        mounts.insert("b/2/D".into(), ());
        mounts.insert("b/3/E".into(), ());
        assert_eq!(
            super::children_on_path(&mounts, ""),
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            super::children_on_path(&mounts, "a"),
            Some(vec!["1".to_string()])
        );
        assert_eq!(
            super::children_on_path(&mounts, "b/2"),
            Some(vec!["C".to_string(), "D".to_string()])
        );
    }

    fn dummy_handler(_: RequestData, _: Sender<ClientCommand>, _: &mut Option<()>) {}

    #[test]
    fn accept_valid_routes() {
        ShvNode::new_static(&PROPERTY_METHODS,
                            vec![Route::new([METH_GET, METH_SET, METH_LS], handler!(dummy_handler))]);
        ShvNode::new_static(&PROPERTY_METHODS, vec![Route::new([METH_GET], handler!(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_sig_chng_route() {
        ShvNode::new_static(&PROPERTY_METHODS, vec![Route::new([SIG_CHNG], handler!(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_invalid_method_route() {
        ShvNode::new_static(&PROPERTY_METHODS, vec![Route::new(["invalidMethod"], handler!(dummy_handler))]);
    }
}

/// Helper trait for uniform access to some common methods of BTreeMap<String, V> and HashMap<String, V>
pub trait StringMapView<V> {
    fn contains_key_(&self, key: &String) -> bool;
}

impl<V> StringMapView<V> for BTreeMap<String, V> {
    fn contains_key_(&self, key: &String) -> bool {
        self.contains_key(key)
    }
}

impl<V> StringMapView<V> for HashMap<String, V> {
    fn contains_key_(&self, key: &String) -> bool {
        self.contains_key(key)
    }
}

pub fn find_longest_prefix<'a, 'b, V>(
    map: &'a impl StringMapView<V>,
    shv_path: &'b str,
) -> Option<(&'b str, &'b str)> {
    let mut path = &shv_path[..];
    let mut rest = "";
    loop {
        if map.contains_key_(&path.to_string()) {
            return Some((path, rest));
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

type StaticHandlers<S> = BTreeMap<String, Rc<RequestHandler<S>>>;

struct StaticNode<'a, S> {
    methods: Vec<&'a MetaMethod>,
    handlers: BTreeMap<String, Rc<RequestHandler<S>>>,
}

impl<'a, S> StaticNode<'a, S> {
    fn new(methods: impl Iterator<Item = &'a MetaMethod>, routes: Vec<Route<S>>) -> Self {
        let methods: Vec<&MetaMethod> = methods.collect();
        Self {
            methods: methods.clone(),
            handlers: Self::add_routes(&methods, routes),
        }
    }

    fn add_routes(methods: &Vec<&'a MetaMethod>, routes: Vec<Route<S>>) -> StaticHandlers<S> {
        let mut handlers: StaticHandlers<S> = Default::default();
        for route in routes {
            let handler = Rc::new(route.handler);
            for m in &route.methods {
                Self::all_methods(methods)
                    .iter()
                    .find(|dm| dm.name == m && (dm.flags & Flag::IsSignal as u32 == 0u32))
                    .expect("Invalid method {m}");
                handlers.insert(m.clone(), handler.clone());
            }
        }
        handlers
    }

    fn all_methods(defined_methods: &Vec<&'a MetaMethod>) -> Vec<&'a MetaMethod> {
        DIR_LS_METHODS.iter().chain(defined_methods.clone()).collect()
    }

    fn methods(&self) -> Vec<&'a MetaMethod> {
        Self::all_methods(&self.methods)
    }

    fn process_request(
        &self,
        req_data: RequestData,
        client_cmd_sender: Sender<ClientCommand>,
        app_data: &mut Option<S>,
        ) {
        let rq = &req_data.request;
        if let Some(method) = rq.method() {
            if let Some(handler) = self.handlers.get(method) {
                handler(req_data.clone(), client_cmd_sender.clone(), app_data);
            } else {
                self.process_dir_ls(rq, client_cmd_sender);
            }
        }
    }

    fn process_dir_ls(&self, rq: &RpcMessage, client_cmd_sender: Sender<ClientCommand>) {
        let mut resp = rq.prepare_response().unwrap_or_default(); // FIXME: better handling
        match self.dir_ls(rq) {
            RequestResult::Response(val) => {
                resp.set_result(val);
            }
            RequestResult::Error(err) => {
                resp.set_error(err);
            }
        }
        if let Err(e) = client_cmd_sender
            .unbounded_send(ClientCommand::SendMessage { message: resp })
        {
            error!("process_dir_ls: Cannot send response ({e})");
        }
    }

    fn dir_ls(&self, rq: &RpcMessage) -> RequestResult {
        match rq.method() {
            Some(METH_DIR) => self.process_dir(rq),
            Some(METH_LS) => self.process_ls(rq),
            _ => {
                let errmsg = format!(
                    "Unknown method '{}:{}()', path.",
                    rq.shv_path().unwrap_or_default(),
                    rq.method().unwrap_or_default()
                );
                warn!("{}", &errmsg);
                RequestResult::Error(RpcError::new(RpcErrorCode::MethodNotFound, &errmsg))
            }
        }
    }

    fn process_dir(&self, rq: &RpcMessage) -> RequestResult {
        let shv_path = rq.shv_path().unwrap_or_default();
        if shv_path.is_empty() {
            let resp = dir(self.methods().into_iter(), rq.param().into());
            RequestResult::Response(resp)
        } else {
            let errmsg = format!(
                "Unknown method '{}:{}()', invalid path.",
                rq.shv_path().unwrap_or_default(),
                rq.method().unwrap_or_default()
            );
            warn!("{}", &errmsg);
            RequestResult::Error(RpcError::new(RpcErrorCode::MethodNotFound, &errmsg))
        }
    }

    fn process_ls(&self, rq: &RpcMessage) -> RequestResult {
        let shv_path = rq.shv_path().unwrap_or_default();
        if shv_path.is_empty() {
            match LsParam::from(rq.param()) {
                LsParam::List => RequestResult::Response(rpcvalue::List::new().into()),
                LsParam::Exists(_path) => RequestResult::Response(false.into()),
            }
        } else {
            let errmsg = format!(
                "Unknown method '{}:{}()', invalid path.",
                rq.shv_path().unwrap_or_default(),
                rq.method().unwrap_or("")
            );
            warn!("{}", &errmsg);
            RequestResult::Error(RpcError::new(RpcErrorCode::MethodNotFound, &errmsg))
        }
    }
}

struct DynamicNode<S> {
    methods: MethodsGetter<S>,
    handler: RequestHandler<S>,
}

enum ShvNodeInner<'a, S> {
    Static(StaticNode<'a, S>),
    Dynamic(DynamicNode<S>),
}

pub struct ShvNode<'a, S>(ShvNodeInner<'a, S>);

impl<'a, S> ShvNode<'a, S> {
    pub fn new_static(methods: impl IntoIterator<Item = &'a MetaMethod>, routes: impl Into<Vec<Route<S>>>) -> Self {
        Self(ShvNodeInner::Static(StaticNode::new(methods.into_iter(), routes.into())))
    }

    pub fn new_dynamic(methods: MethodsGetter<S>, handler: RequestHandler<S>) -> Self {
        Self(ShvNodeInner::Dynamic(DynamicNode { methods, handler }))
    }

    pub fn methods(&self, path: &str, app_data: &mut Option<S>) -> oneshot::Receiver<Vec<&'a MetaMethod>> {
        let (res_tx, res_rx) = oneshot::channel();
        match &self.0 {
            ShvNodeInner::Static(node) => {
                res_tx.send(node.methods()).unwrap();
            },
            ShvNodeInner::Dynamic(node) => {
                (node.methods)(path, res_tx, app_data);
            }
        }
        res_rx
    }

    pub fn process_request(
        &self,
        req_data: RequestData,
        client_cmd_sender: Sender<ClientCommand>,
        app_data: &mut Option<S>,
        ) {
        match &self.0 {
            ShvNodeInner::Static(node) => {
                node.process_request(req_data, client_cmd_sender, app_data);
            },
            ShvNodeInner::Dynamic(node) => {
                (node.handler)(req_data, client_cmd_sender, app_data);
            }
        }
    }
}

pub enum RequestAccessResult {
    Ok,
    // Err(RpcErrorCode, String),
    Err(RpcError)
}

pub fn check_request_access<'a>(rq: &RpcMessage, methods: impl IntoIterator<Item = &'a MetaMethod>) -> RequestAccessResult {
    let level_str = rq.access().unwrap_or_default();
    for level_str in level_str.split(',') {
        if let Some(rq_level) = Access::from_str(level_str) {
            let method = rq.method().unwrap_or_default();
            for mm in methods {
                if mm.name == method {
                    return if rq_level >= mm.access {
                        RequestAccessResult::Ok
                    } else {
                        RequestAccessResult::Err(
                            RpcError::new(
                                RpcErrorCode::PermissionDenied,
                                format!("Insufficient permissions. Method '{}' called with  access level '{:?}', required '{:?}'",
                                        method,
                                        rq_level,
                                        mm.access)
                                )
                            )
                    }
                }
            }
            return RequestAccessResult::Err(
                RpcError::new(
                    RpcErrorCode::MethodNotFound,
                    format!("Method {method} does not exist on path {}", rq.shv_path().unwrap_or_default())
                    )
                );
        }
    }
    RequestAccessResult::Err(
        RpcError::new(
            RpcErrorCode::PermissionDenied,
            format!("Request with undefined access level")
            )
        )
}


pub const METH_DIR: &str = "dir";
pub const METH_LS: &str = "ls";
pub const METH_GET: &str = "get";
pub const METH_SET: &str = "set";
pub const SIG_CHNG: &str = "chng";
pub const METH_PING: &str = "ping";

pub const DIR_LS_METHODS: [MetaMethod; 2] = [
    MetaMethod {
        name: METH_DIR,
        flags: Flag::None as u32,
        access: Access::Browse,
        param: "DirParam",
        result: "DirResult",
        description: "",
    },
    MetaMethod {
        name: METH_LS,
        flags: Flag::None as u32,
        access: Access::Browse,
        param: "LsParam",
        result: "LsResult",
        description: "",
    },
];
pub const PROPERTY_METHODS: [MetaMethod; 3] = [
    MetaMethod {
        name: METH_GET,
        flags: Flag::IsGetter as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_SET,
        flags: Flag::IsSetter as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: SIG_CHNG,
        flags: Flag::IsSignal as u32,
        access: Access::Browse,
        param: "",
        result: "",
        description: "",
    },
];
