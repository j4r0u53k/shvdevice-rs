
// Generator for fixed nodes
//
// Usage example:
//
// The optional parameter type at methods definitions can be any type for
// which `TryFrom<&RpcValue, Error=String>` is implemented.
//
//  let node = fixed_node!{
//         device_handler<i32>(request, client_cmd_tx, app_state) {
//             "name" [IsGetter, Browse] (param: i32) => {
//                 println!("param: {}", param);
//                 app_state.map(|v| { println!("app_state: {}", *v); });
//                 Some(Ok(RpcValue::from("name result")))
//             }
//             "echo" [IsGetter, Browse] (param: &str) => {
//                 println!("param: {}", param);
//                 Some(Ok(param.into()))
//             }
//             "setTable" [IsGetter, Browse] (table: MyTable) => {
//                 println!("set table: {:?}", table);
//                 Some(Ok(table.num_items()))
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

#[macro_export]
macro_rules! fixed_node {
    ($fn_name:ident $(<$T:ty>)? ($request:ident, $client_cmd_tx:ident $(, $app_state:ident)?) {
        $($method:tt [$($flags:ident)|+, $access:ident] $(($param:ident : $type:ty))? => $body:block)+
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
                    $(let $app_state = $app_state.expect("Application state should be Some");)?

                    async fn handler($request: RpcMessage, #[allow(unused)] $client_cmd_tx: ClientCommandSender $(, $app_state: AppState<$T>)?)
                    -> Option<std::result::Result<$crate::clientnode::RpcValue, $crate::clientnode::RpcError>> {
                        match $request.method() {

                            $(Some($method) => {
                                $crate::method_handler!($(($param : $type))? $method @ $request @ $body)
                            })+

                            _ => Some(Err($crate::clientnode::RpcError::new(
                                        $crate::clientnode::RpcErrorCode::MethodNotFound,
                                        format!("Invalid method: {:?}", $request.method())))
                            )
                        }
                    }

                    if let Some(val) = handler($request, $client_cmd_tx.clone() $(, $app_state)?).await {
                        if let Ok(res) = val {
                            __resp.set_result(res);
                        } else if let Err(err) = val {
                            __resp.set_error(err);
                        }

                        if let Err(e) = $client_cmd_tx.send_message(__resp) {
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
    (($param:ident : $type:ty) $method:tt @ $request:ident @ $body:block) => {
        {
            let request_param = $request.param().unwrap_or_default();

             match <$type>::try_from(request_param) {
                 Ok($param) => $body,
                 Err(err) => {
                     Some(Err($crate::clientnode::RpcError::new(
                                 $crate::clientnode::RpcErrorCode::InvalidParam,
                                 format!("Wrong parameter for `{}`: {}",
                                     $method,
                                     err
                                 )))
                     )
                 }
            }
        }
    };
    ($method:tt @ $request:ident @ $body:block) => {
        $body
    };
}

#[macro_export]
macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + $crate::count!($($xs)*));
}
