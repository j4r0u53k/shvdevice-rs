
pub enum Runtime {
    #[cfg(feature = "async_std")]
    AsyncStd,
    #[cfg(feature = "tokio")]
    Tokio,
    Unknown,
}

pub fn current_task_runtime() -> Runtime {
    #[cfg(feature = "async_std")]
    if ::async_std::task::try_current().is_some() {
        return Runtime::AsyncStd;
    }
    #[cfg(feature = "tokio")]
    if ::tokio::runtime::Handle::try_current().is_ok() {
        return Runtime::Tokio;
    }
    Runtime::Unknown
}

pub fn spawn_task<F>(f: F)
where
    F: futures::Future + Send + 'static,
    F::Output: Send + 'static,
{

    match current_task_runtime() {
        #[cfg(feature = "tokio")]
        Runtime::Tokio => { tokio::spawn(f); },
        #[cfg(feature = "async_std")]
        Runtime::AsyncStd => { async_std::task::spawn(f); },
        _ => panic!("Could not find suitable async runtime"),
    };
}
