// The file originates from https://github.com/silicon-heaven/shv-rs/blob/e740fd301dc65f3412ad1154595bf61ee5632aba/src/shvnode.rs
// struct ShvNode has been adapted to support async process_request accepting RpcCommand channel and a shared state params

use crate::client::{ClientCommand, RequestHandler, RequestResult, Route, Sender, MethodsGetter};
use crate::runtime::spawn_task;
use log::{error, warn};
use shv::metamethod::Access;
use shv::metamethod::{Flag, MetaMethod};
use shv::rpcframe::RpcFrame;
use shv::rpcmessage::{RpcError, RpcErrorCode};
use shv::{metamethod, rpcvalue, RpcMessage, RpcMessageMetaTags, RpcValue};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::format;
use std::rc::Rc;
use std::sync::Arc;

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
                } else if rpcval.as_bool() {
                    DirParam::Full
                } else {
                    DirParam::Brief
                }
            }
            None => DirParam::Brief,
        }
    }
}

pub fn dir<'a>(methods: impl Iterator<Item = &'a MetaMethod>, param: DirParam) -> RpcValue {
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
                if mm.name == method_name {
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
    let mut children_on_path = children_on_path(mounts, shv_path);
    if let Some(children_on_path) = children_on_path.as_mut() {
        if frame.meta.get(DOT_LOCAL_HACK).is_some() {
            children_on_path.insert(0, DOT_LOCAL_DIR.into());
        }
    }
    let mount = find_longest_prefix(mounts, shv_path);
    let is_mount_point = mount.is_some();
    let is_leaf = match &children_on_path {
        None => is_mount_point,
        Some(dirs) => dirs.is_empty(),
    };
    if children_on_path.is_none() && !is_mount_point {
        // path doesn't exist
        return Some(RequestResult::Error(RpcError::new(
            RpcErrorCode::MethodNotFound,
            format!("Invalid shv path: {}", shv_path),
        )));
    }
    if method == METH_DIR && !is_mount_point {
        // dir in the middle of the tree must be resolved locally
        if let Ok(rpcmsg) = frame.to_rpcmesage() {
            let dir = dir(DIR_LS_METHODS.iter(), rpcmsg.param().into());
            return Some(RequestResult::Response(dir));
        } else {
            return Some(RequestResult::Error(RpcError::new(
                RpcErrorCode::InvalidRequest,
                "Cannot convert RPC frame to RPC message".to_string(),
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
                "Cannot convert RPC frame to RPC message".to_string(),
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
                let res: rpcvalue::List = dirs.iter().map(RpcValue::from).collect();
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
    for (key, _) in mounts.range(path.to_owned()..) {
        if key.starts_with(path) {
            if path.is_empty() || (key.len() > path.len() && key.as_bytes()[path.len()] == (b'/')) {
                dir_exists = true;
                let dir_rest_start = if path.is_empty() { 0 } else { path.len() + 1 };
                let mut updirs = key[dir_rest_start..].split('/');
                if let Some(dir) = updirs.next() {
                    if !unique_dirs.contains(dir) {
                        dirs.push(dir.to_string());
                        unique_dirs.insert(dir.to_string());
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

/// Helper trait for uniform access to some common methods of BTreeMap<String, V> and HashMap<String, V>
pub trait StringMapView<V> {
    fn contains_key_(&self, key: &str) -> bool;
}

impl<V> StringMapView<V> for BTreeMap<String, V> {
    fn contains_key_(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

impl<V> StringMapView<V> for HashMap<String, V> {
    fn contains_key_(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

pub fn find_longest_prefix<'a, V>(
    map: &impl StringMapView<V>,
    shv_path: &'a str,
) -> Option<(&'a str, &'a str)> {
    let mut path = shv_path;
    let mut rest = "";
    loop {
        if map.contains_key_(path) {
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

type StaticNodeHandlers<T> = BTreeMap<String, Rc<RequestHandler<T>>>;

struct StaticNode<'a, T> {
    methods: Vec<&'a MetaMethod>,
    handlers: StaticNodeHandlers<T>,
}

impl<'a, T> StaticNode<'a, T> {
    fn new(methods: impl IntoIterator<Item = &'a MetaMethod>, routes: impl IntoIterator<Item = Route<T>>) -> Self {
        let methods = DIR_LS_METHODS.iter().chain(methods).collect::<Vec<&MetaMethod>>();
        let handlers = Self::add_routes(&methods, routes);
        Self {
            methods,
            handlers,
        }
    }

    fn add_routes(methods: &[&'a MetaMethod], routes: impl IntoIterator<Item = Route<T>>) -> StaticNodeHandlers<T> {
        if let Some(dup_method) = methods.iter().enumerate().find_map(|(i,mm)| methods[i+1..].iter().find(|m| m.name == mm.name)) {
            panic!("Duplicate method '{}' in a static node definition", dup_method.name);
        }
        let mut handlers: StaticNodeHandlers<T> = Default::default();
        fn is_signal(method: &MetaMethod) -> bool {
            method.flags & (Flag::IsSignal as u32) != 0u32
        }
        for route in routes {
            let handler = Rc::new(route.handler);
            for m in &route.methods {
                methods
                    .iter()
                    .find(|dm| dm.name == m && !is_signal(dm))
                    .expect("Invalid method {m}");
                handlers.insert(m.clone(), handler.clone());
            }
        }
        if let Some(unhandled_method) =
            methods.iter().find(|mm| {
                !is_signal(mm) &&
                ![METH_LS, METH_DIR].contains(&mm.name) &&
                !handlers.contains_key(mm.name)
            })
        {
            panic!("No handler found for method '{}' of a static node", unhandled_method.name);
        }
        handlers
    }

    fn methods(&self) -> &[&'a MetaMethod] {
        &self.methods
    }

    async fn process_request(&self, request: RpcMessage, client_cmd_tx: Sender<ClientCommand>, app_data: &Option<Arc<T>>) {
        let method = request.method().expect("method should be set and valid after request access check");
        if let Some(handler) = self.handlers.get(method) {
            handler(request, client_cmd_tx, app_data.clone()).await;
        } else {
            self.process_dir_ls(&request, &client_cmd_tx);
        }
    }

    fn process_dir_ls(&self, rq: &RpcMessage, client_cmd_sender: &Sender<ClientCommand>) {
        let mut resp = rq.prepare_response().unwrap_or_default(); // FIXME: better handling
        match self.dir_ls(rq) {
            RequestResult::Response(val) => {
                resp.set_result(val);
            }
            RequestResult::Error(err) => {
                resp.set_error(err);
            }
        }
        if let Err(e) = client_cmd_sender.unbounded_send(ClientCommand::SendMessage { message: resp }) {
            error!("process_dir_ls: Cannot send response ({e})");
        }
    }

    fn dir_ls(&self, rq: &RpcMessage) -> RequestResult {
        let shv_path = rq.shv_path().unwrap_or_default();
        assert!(shv_path.is_empty(), "shv_path of a request on a static node should be empty after permitted access check");
        match rq.method() {
            Some(METH_DIR) => {
                let resp = dir(self.methods().iter().copied(), rq.param().into());
                RequestResult::Response(resp)
            },
            Some(METH_LS) => {
                match LsParam::from(rq.param()) {
                    LsParam::List => RequestResult::Response(rpcvalue::List::new().into()),
                    LsParam::Exists(_path) => RequestResult::Response(false.into()),
                }
            },
            _ => {
                panic!("BUG: Unhandled method '{}:{}()' should have been caught in the node constructor",
                    rq.shv_path().unwrap_or_default(),
                    rq.method().unwrap_or_default()
                );
            }
        }
    }
}

struct DynamicNode<T> {
    methods: MethodsGetter<T>,
    handler: RequestHandler<T>,
    spawned: bool,
}

enum ShvNodeInner<'a, T> {
    Static(StaticNode<'a, T>),
    Dynamic(Arc<DynamicNode<T>>),
}

pub struct ShvNode<'a, T>(ShvNodeInner<'a, T>);

impl<'a, T: Sync + Send + 'static> ShvNode<'a, T> {
    pub fn new_static(methods: impl IntoIterator<Item = &'a MetaMethod>, routes: impl IntoIterator<Item = Route<T>>) -> Self {
        Self(ShvNodeInner::Static(StaticNode::new(methods, routes)))
    }

    pub fn new_dynamic(methods: MethodsGetter<T>, handler: RequestHandler<T>) -> Self {
        Self(ShvNodeInner::Dynamic(Arc::new(DynamicNode { methods, handler, spawned: false })))
    }

    pub fn new_dynamic_spawned(methods: MethodsGetter<T>, handler: RequestHandler<T>) -> Self {
        Self(ShvNodeInner::Dynamic(Arc::new(DynamicNode { methods, handler, spawned: true })))
    }

    pub async fn process_request(&self, request: RpcMessage, mount_path: String, client_cmd_tx: Sender<ClientCommand>, app_data: &Option<Arc<T>>) {
        match &self.0 {
            ShvNodeInner::Static(node) => {
                let methods = if request.shv_path().unwrap_or_default().is_empty() {
                    node.methods()
                } else {
                    // Static nodes do not have any own children. Any child nodes
                    // would be resolved in generic function `process_local_dir_ls()`.
                    &[]
                }.iter().copied(); // && to &
                if resolve_request_access(&request, &mount_path, &client_cmd_tx, methods) {
                    node.process_request(request, client_cmd_tx, app_data).await;
                }
            },
            ShvNodeInner::Dynamic(node) => {
                let app_data = app_data.clone();
                let shv_path = request.shv_path().unwrap_or_default().to_owned();
                if node.spawned {
                    let node = node.clone();
                    spawn_task(async move {
                        let methods = (node.methods)(shv_path, app_data.clone()).await;
                        if resolve_request_access(&request, &mount_path, &client_cmd_tx, methods) {
                            (node.handler)(request, client_cmd_tx, app_data).await;
                        }
                    });
                } else {
                    let methods = (node.methods)(shv_path, app_data.clone()).await;
                    if resolve_request_access(&request, &mount_path, &client_cmd_tx, methods) {
                        (node.handler)(request, client_cmd_tx, app_data).await;
                    }
                }
            }
        }
    }
}

fn resolve_request_access<'a>(request: &RpcMessage, mount_path: &String, client_cmd_tx: &Sender<ClientCommand>, methods: impl IntoIterator<Item = &'a MetaMethod>) -> bool {

    let shv_path = request.shv_path().unwrap_or_default();
    let check_request_access = || {
        let method = request.method().unwrap_or_default();
        let Some(mm) = methods.into_iter().find(|mm| mm.name == method) else {
            return Err(RpcError::new(RpcErrorCode::MethodNotFound,
                                     format!("Unknown method on path '{mount_path}/{shv_path}:{method}()'")));
        };
        let rq_level_str = request.access().unwrap_or_default();
        let Some(rq_level) = Access::from_str(rq_level_str) else {
            return Err(RpcError::new(RpcErrorCode::InvalidRequest, "Undefined access level"));
        };
        if rq_level >= mm.access {
            Ok(())
        } else {
            Err(RpcError::new(
                    RpcErrorCode::PermissionDenied,
                    format!("Insufficient permissions. \
                            Method '{mount_path}/{shv_path}:{method}()' \
                            called with access level '{:?}', required '{:?}'",
                            rq_level,
                            mm.access)
                    )
               )
        }
    };

    let Err(err) = check_request_access() else {
        return true;
    };
    let mut resp = request.prepare_response()
        .expect("should be able to prepare response");
    warn!("Check request access on path: {}/{}, error: {}",
          mount_path,
          request.shv_path().unwrap_or_default(),
          err);
    resp.set_error(err);
    let _ = client_cmd_tx.unbounded_send(ClientCommand::SendMessage { message: resp });
    false
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
        access: Access::Read,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_SET,
        flags: Flag::IsSetter as u32,
        access: Access::Write,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: SIG_CHNG,
        flags: Flag::IsSignal as u32,
        access: Access::Read,
        param: "",
        result: "",
        description: "",
    },
];

#[cfg(test)]
mod tests {
    use crate::handler;

    use super::*;

    #[test]
    fn ls_mounts() {
        let mut mounts = BTreeMap::new();
        mounts.insert("a".into(), ());
        mounts.insert("a/1".into(), ());
        mounts.insert("a/123".into(), ());
        mounts.insert("a/xyz".into(), ());
        mounts.insert("b/2/C".into(), ());
        mounts.insert("b/2/D".into(), ());
        mounts.insert("b/3/E".into(), ());
        assert_eq!(
            super::children_on_path(&mounts, ""),
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            super::children_on_path(&mounts, "a"),
            Some(vec!["1".to_string(), "123".to_string(), "xyz".to_string()])
        );
        assert_eq!(
            super::children_on_path(&mounts, "a/1"),
            None
        );
        assert_eq!(
            super::children_on_path(&mounts, "a/xy"),
            None
        );
        assert_eq!(
            super::children_on_path(&mounts, "b/2"),
            Some(vec!["C".to_string(), "D".to_string()])
        );
    }

    async fn dummy_handler(_: RpcMessage, _: Sender<ClientCommand>, _: Option<Arc<()>>) {}

    #[test]
    fn accept_valid_routes() {
        ShvNode::new_static(&PROPERTY_METHODS,
                            vec![Route::new([METH_GET, METH_SET, METH_LS], handler!(dummy_handler))]);
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

    #[test]
    #[should_panic]
    fn reject_unhandled_method() {
        ShvNode::new_static(&PROPERTY_METHODS, vec![Route::new([METH_GET], handler!(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_duplicate_method() {
        let duplicate_methods= PROPERTY_METHODS.iter().chain(DIR_LS_METHODS.iter());
        ShvNode::new_static(duplicate_methods, vec![Route::new([METH_GET, METH_SET, METH_LS], handler!(dummy_handler))]);
    }
}
