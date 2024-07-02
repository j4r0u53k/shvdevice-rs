// The file originates from https://github.com/silicon-heaven/shv-rs/blob/e740fd301dc65f3412ad1154595bf61ee5632aba/src/shvnode.rs
// struct ShvNode has been adapted to support async process_request accepting RpcCommand channel and a shared state params

use crate::client::{RequestHandler, ClientCommandSender, MethodsGetter, AppState};
use crate::runtime::spawn_task;
use log::{error, debug};
use shvrpc::rpcframe::RpcFrame;
use shvrpc::{metamethod, RpcMessage, RpcMessageMetaTags};
use shvproto::rpcvalue;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::format;
use std::rc::Rc;
use std::sync::Arc;
// Reexport for use in the macros
pub use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
pub use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
pub use shvproto::{RpcValue, Value};

enum DirParam {
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

fn dir<'a>(methods: impl IntoIterator<Item = &'a MetaMethod>, param: DirParam) -> RpcValue {
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

pub(crate) enum LsParam {
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


pub(crate) enum RequestResult {
    Response(RpcValue),
    Error(RpcError),
}

pub(crate) fn process_local_dir_ls<V>(
    mounts: &BTreeMap<String, V>,
    frame: &RpcFrame,
) -> Option<RequestResult> {
    let method = frame.method().unwrap_or_default();
    if !(method == METH_DIR || method == METH_LS) {
        return None;
    }
    let shv_path = frame.shv_path().unwrap_or_default();
    let mount = find_longest_prefix(mounts, shv_path);
    let is_mount_point = mount.is_some();
    let children_on_path = children_on_path(mounts, shv_path);
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
pub(crate) fn children_on_path<V>(mounts: &BTreeMap<String, V>, path: &str) -> Option<Vec<String>> {
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
pub(crate) trait StringMapView<V> {
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

pub(crate) fn find_longest_prefix<'a, V>(
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

pub struct Route<T> {
    pub handler: RequestHandler<T>,
    pub methods: Vec<String>,
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

type StaticNodeHandlers<T> = BTreeMap<String, Rc<RequestHandler<T>>>;

struct FixedNode<'a, T> {
    methods: Vec<&'a MetaMethod>,
    handlers: StaticNodeHandlers<T>,
}

impl<'a, T> FixedNode<'a, T> {
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
            if route.methods.iter().any(|m| m == METH_DIR) {
                panic!("Custom implementation of 'dir', which is handled by the library");
            }
            let handler = Rc::new(route.handler);
            route.methods.iter().for_each(|m| {
                methods
                    .iter()
                    .find(|dm| dm.name == m && !is_signal(dm))
                    .unwrap_or_else(|| panic!("Invalid method {m}"));
                handlers.insert(m.clone(), handler.clone());
            });
        }
        if let Some(unhandled_method) = methods.iter().find(|mm| !is_signal(mm)
                                                            && ![METH_DIR, METH_LS].contains(&mm.name)
                                                            && !handlers.contains_key(mm.name))
        {
            panic!("No handler found for method '{}' of a static node", unhandled_method.name);
        }
        handlers
    }
}

struct DynamicNode<T> {
    methods: MethodsGetter<T>,
    handler: RequestHandler<T>,
}

pub trait ConstantNode {
    fn methods(&self) -> Vec<&MetaMethod>;
    fn process_request(&self, request: &RpcMessage) -> Option<Result<RpcValue, RpcError>>;
}

// NOTE: Implementing Steady and Dynamic nodes using async trait would allow to
// remove Constant variant. Steady node would have only one handler for the whole node.

enum NodeVariant<'a, T> {
    Fixed(FixedNode<'a, T>),
    Dynamic(Arc<DynamicNode<T>>),
    Constant(Box<dyn ConstantNode>),
}

pub struct ClientNode<'a, T>(NodeVariant<'a, T>);

impl<'a, T: Sync + Send + 'static> ClientNode<'a, T> {
    pub fn fixed(methods: impl IntoIterator<Item = &'a MetaMethod>, routes: impl IntoIterator<Item = Route<T>>) -> Self {
        Self(NodeVariant::Fixed(FixedNode::new(methods, routes)))
    }

    pub fn dynamic(methods: MethodsGetter<T>, handler: RequestHandler<T>) -> Self {
        Self(NodeVariant::Dynamic(Arc::new(DynamicNode { methods, handler })))
    }

    // NOTE: Not included in the public API. Constant nodes are meant
    // for implementation of special nodes like .app and .device and
    // should not be needed outside of the library.
    pub(crate) fn constant<N>(node: N) -> Self
    where
        N: ConstantNode + 'static,
    {
        Self(NodeVariant::Constant(Box::new(node)))
    }

    pub(crate) async fn process_request(&self, request: RpcMessage, mount_path: String, client_cmd_tx: ClientCommandSender, app_state: &Option<AppState<T>>) {
        match &self.0 {
            NodeVariant::Fixed(node) => {
                let methods = if request.shv_path().unwrap_or_default().is_empty() {
                    node.methods.as_slice()
                } else {
                    // Static nodes do not have any own children. Any child nodes are
                    // resolved on the mounts tree level in `process_local_dir_ls()`.
                    &[]
                };
                if resolve_request_access(&request, &mount_path, &client_cmd_tx, methods) {
                    let Some(method) = request.method() else {
                        panic!("BUG: Request method should be Some after access check.");
                    };
                    if method == self::METH_DIR {
                        let result = dir(methods.iter().copied(), request.param().into());
                        send_response(request, client_cmd_tx, Ok(result));
                    } else if let Some(handler) = node.handlers.get(method) {
                        spawn_task(handler.0(request, client_cmd_tx, app_state.clone()));
                    } else if method == self::METH_LS {
                        let result = default_ls(request.param());
                        send_response(request, client_cmd_tx, Ok(result));
                    } else {
                        panic!("BUG: Unhandled method '{mount_path}:{method}()' should have been caught in the node constructor");
                    }
                }
            },
            NodeVariant::Dynamic(node) => {
                let app_state = app_state.clone();
                let shv_path = request.shv_path().unwrap_or_default().to_owned();
                let node = node.clone();
                spawn_task(async move {
                    let methods = node.methods.0(shv_path, app_state.clone()).await
                        .map_or_else(
                            Vec::new,
                            |m| DIR_LS_METHODS.iter().chain(m).collect());
                    if resolve_request_access(&request, &mount_path, &client_cmd_tx, &methods) {
                        match request.method() {
                            Some(self::METH_DIR) => {
                                let result = dir(methods.into_iter(), request.param().into());
                                send_response(request, client_cmd_tx, Ok(result));
                            }
                            Some(_) =>
                                node.handler.0(request, client_cmd_tx, app_state).await,
                            _ =>
                                panic!("BUG: Request method should be Some after access check."),
                        };
                    }
                });
            },
            NodeVariant::Constant(node) => {
                let methods = if request.shv_path().unwrap_or_default().is_empty() {
                    DIR_LS_METHODS.iter().chain(node.methods()).collect()
                } else {
                    // Static nodes do not have any own children. Any child nodes are
                    // resolved on the mounts tree level in `process_local_dir_ls()`.
                    vec![]
                };
                if resolve_request_access(&request, &mount_path, &client_cmd_tx, &methods) {
                    let Some(method) = request.method() else {
                        panic!("BUG: Request method should be Some after access check.");
                    };
                    if method == self::METH_DIR {
                        let result = dir(methods.iter().copied(), request.param().into());
                        send_response(request, client_cmd_tx, Ok(result));
                    } else if let Some(result) = node.process_request(&request) {
                        send_response(request, client_cmd_tx, result);
                    } else if method == self::METH_LS {
                        let result = default_ls(request.param());
                        send_response(request, client_cmd_tx, Ok(result));
                    } else {
                        panic!("BUG: Unhandled method '{mount_path}:{method}()' should have been caught in the node constructor");
                    }
                }
            },
        }
    }
}

fn resolve_request_access(request: &RpcMessage, mount_path: &String, client_cmd_tx: &ClientCommandSender, methods: &[&MetaMethod]) -> bool {

    let shv_path = request.shv_path().unwrap_or_default();
    let check_request_access = || {
        let method = request.method().unwrap_or_default();
        let full_path = if shv_path.is_empty() {
            mount_path
        } else {
            &format!("{mount_path}/{shv_path}")
        };
        let Some(mm) = methods.iter().find(|mm| mm.name == method) else {
            return Err(RpcError::new(RpcErrorCode::MethodNotFound,
                                     format!("Unknown method on path '{full_path}:{method}()'")));
        };
        let Some(rq_level) = request.access_level() else {
            return Err(RpcError::new(RpcErrorCode::InvalidRequest, "Undefined access level"));
        };
        if rq_level >= mm.access as i32 {
            Ok(())
        } else {
            Err(RpcError::new(
                    RpcErrorCode::PermissionDenied,
                    format!("Insufficient permissions. \
                            Method '{full_path}:{method}()' \
                            called with access level {:?}, required {} ({:?})",
                            rq_level,
                            mm.access as i32,
                            mm.access,
                            )
                    )
               )
        }
    };

    let Err(err) = check_request_access() else {
        return true;
    };
    let mut resp = request.prepare_response()
        .expect("should be able to prepare response");
    debug!("Check request access on path `{}` / `{}`, error: {}",
          mount_path,
          shv_path,
          err);
    resp.set_error(err);
    let _ = client_cmd_tx.send_message(resp);
    false
}

pub fn send_response(request: RpcMessage, client_cmd_tx: ClientCommandSender, result: Result<RpcValue, RpcError>) {
    match request.prepare_response() {
        Err(err) => {
            error!("Cannot prepare response. Error: {err}, request: {request}");
        }
        Ok(mut resp) => {
            match result {
                Ok(result) => resp.set_result(result),
                Err(err) => resp.set_error(err),
            };
            if let Err(e) = client_cmd_tx.send_message(resp) {
                error!("Cannot send response. Error: {e}, request: {request}");
            }
        }
    }
}

pub fn default_ls(rq_param: Option<&RpcValue>) -> RpcValue {
    match LsParam::from(rq_param) {
        LsParam::List => rpcvalue::List::new().into(),
        LsParam::Exists(_path) => false.into(),
    }
}


pub const METH_DIR: &str = "dir";
pub const METH_LS: &str = "ls";
pub const METH_GET: &str = "get";
pub const METH_SET: &str = "set";
pub const SIG_CHNG: &str = "chng";
pub const METH_PING: &str = "ping";

pub(crate) const DIR_LS_METHODS: [MetaMethod; 2] = [
    MetaMethod {
        name: METH_DIR,
        flags: Flag::None as u32,
        access: AccessLevel::Browse,
        param: "DirParam",
        result: "DirResult",
        description: "",
    },
    MetaMethod {
        name: METH_LS,
        flags: Flag::None as u32,
        access: AccessLevel::Browse,
        param: "LsParam",
        result: "LsResult",
        description: "",
    },
];
pub const PROPERTY_METHODS: [MetaMethod; 3] = [
    MetaMethod {
        name: METH_GET,
        flags: Flag::IsGetter as u32,
        access: AccessLevel::Read,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: METH_SET,
        flags: Flag::IsSetter as u32,
        access: AccessLevel::Write,
        param: "",
        result: "",
        description: "",
    },
    MetaMethod {
        name: SIG_CHNG,
        flags: Flag::IsSignal as u32,
        access: AccessLevel::Read,
        param: "",
        result: "",
        description: "",
    },
];

// Generator for fixed nodes

#[macro_export]
macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + $crate::count!($($xs)*));
}

#[macro_export]
macro_rules! fixed_node {
    ($fn_name:ident $(<$T:ty>)? ($request:ident, $client_cmd_tx:ident $(, $app_state:ident)?) {
        $($method:tt [$($flags:ident)|+, $access:ident] $(($param:ident : $type:ident))? => $body:block)+
    }) => {

        {
            const METHODS: [$crate::clientnode::MetaMethod; $crate::count!($($method)+)] = [
                $(MetaMethod {
                    name: $method,
                    flags: $($crate::clientnode::Flag::$flags as u32)|+,
                    access: $crate::clientnode::AccessLevel::$access,
                    param: "",
                    result: "",
                    description: "",
                },)+
            ];

            async fn $fn_name($request: RpcMessage, $client_cmd_tx: ClientCommandSender $(, $app_state: Option<AppState<$T>>)?) {

                if $request.shv_path().unwrap_or_default().is_empty() {
                    let mut __resp = $request.prepare_response().unwrap_or_default();
                    let __client_cmd_tx_clone = $client_cmd_tx.clone();
                    $(let $app_state = $app_state.expect("Application state should be Some");)?
                    let resp_value: Option<std::result::Result<$crate::clientnode::RpcValue, $crate::clientnode::RpcError>> = match $request.method() {

                        $(Some($method) => {
                            $crate::method_handler!($(($param : $type))? @$request@ $body)
                        })+

                        _ => Some(Err($crate::clientnode::RpcError::new(
                                $crate::clientnode::RpcErrorCode::MethodNotFound,
                                format!("Invalid method: {:?}", $request.method())))
                        )
                    };

                    if let Some(val) = resp_value {
                        if let Ok(res) = val {
                            __resp.set_result(res);
                        } else if let Err(err) = val {
                            __resp.set_error(err);
                        }

                        if let Err(e) = __client_cmd_tx_clone.send_message(__resp) {
                            error!("{}: Cannot send response ({e})", stringify!($fn_name));
                        }
                    }
                };
            }

            $crate::clientnode::ClientNode::fixed(
                &METHODS,
                [Route::new(
                    [$($method),+],
                    $crate::request_handler!($fn_name $(,$app_state)?),
                )]
            )
        }
    }
}

#[macro_export]
macro_rules! request_handler {
    ($fn_name:ident) => {
        RequestHandler::stateless($fn_name)
    };
    ($fn_name:ident, $app_state:ident) => {
        RequestHandler::stateful($fn_name)
    };
}

#[macro_export]
macro_rules! method_handler {
    (($param:ident : $type:ident) @$request:ident@ $body:block) => {
        {
            let request_param = $request.param().unwrap_or_default();

            if let $crate::clientnode::Value::$type($param) = request_param.value() {
                $body
            } else {
                Some(Err($crate::clientnode::RpcError::new(
                        $crate::clientnode::RpcErrorCode::InvalidParam,
                        format!("Expected parameter type `{}`, got `{}`",
                            stringify!($type),
                            request_param.type_name()
                        )))
                )
            }
        }
    };
    (@$request:ident@ $body:block) => {
        $body
    };
}

// Usage example:
//
//  let node = fixed_node!{
//         device_handler<i32>(request, client_cmd_tx, app_state) {
//             "name" [IsGetter, Browse] (param: Int) => {
//                 println!("param: {}", param);
//                 app_state.map(|v| { println!("app_state: {}", *v); });
//                 Some(Ok(RpcValue::from("name result")))
//             }
//             "version" [IsGetter, Browse] => {
//                 Some(Ok(RpcValue::from(42)))
//             }
//         }
//     }
// }
//
// Type in < > and app_state parameres are optional.
//
// TODO: documentation
//


#[cfg(test)]
mod tests {
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

    async fn dummy_handler(_: RpcMessage, _: ClientCommandSender, _: Option<AppState<()>>) {}

    #[test]
    fn accept_valid_routes() {
        ClientNode::fixed(&PROPERTY_METHODS,
                            vec![Route::new([METH_GET, METH_SET, METH_LS], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    fn accept_valid_routes_without_ls() {
        ClientNode::fixed(&PROPERTY_METHODS,
                            vec![Route::new([METH_GET, METH_SET], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_sig_chng_route() {
        ClientNode::fixed(&PROPERTY_METHODS,
                            vec![Route::new([METH_GET, METH_SET, METH_LS, SIG_CHNG], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_custom_dir_handler() {
        ClientNode::fixed(&PROPERTY_METHODS,
                            vec![Route::new([METH_GET, METH_SET, METH_DIR], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_invalid_method_route() {
        ClientNode::fixed(&PROPERTY_METHODS, vec![Route::new(["invalidMethod"], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_unhandled_method() {
        ClientNode::fixed(&PROPERTY_METHODS, vec![Route::new([METH_GET], RequestHandler::stateful(dummy_handler))]);
    }

    #[test]
    #[should_panic]
    fn reject_duplicate_method() {
        let duplicate_methods = PROPERTY_METHODS.iter().chain(DIR_LS_METHODS.iter());
        ClientNode::fixed(duplicate_methods, vec![Route::new([METH_GET, METH_SET, METH_LS], RequestHandler::stateful(dummy_handler))]);
    }
}
