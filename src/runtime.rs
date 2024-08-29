#[cfg(not(any(feature = "tokio", feature = "async_std")))]
compile_error!("No async runtime selected. At least one of `tokio`, `async_std` features must be enabled.");

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

pub enum TaskHandle<F: futures::Future + Send + 'static> {
    #[cfg(feature = "tokio")]
    Tokio(tokio::task::JoinHandle<F::Output>),
    #[cfg(feature = "async_std")]
    AsyncStd(async_std::task::JoinHandle<F::Output>),

}

impl<F: futures::Future + Send + 'static> TaskHandle<F> {
    pub async fn cancel(self) {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(handle) => { handle.abort(); }
            #[cfg(feature = "async_std")]
            Self::AsyncStd(handle) => { handle.cancel().await; }
        }
    }
}

pub fn spawn_task<F>(f: F) -> TaskHandle<F>
where
    F: futures::Future + Send + 'static,
    F::Output: Send + 'static,
{

    match current_task_runtime() {
        #[cfg(feature = "tokio")]
        Runtime::Tokio => { TaskHandle::Tokio(tokio::spawn(f)) },
        #[cfg(feature = "async_std")]
        Runtime::AsyncStd => { TaskHandle::AsyncStd(async_std::task::spawn(f)) },
        _ => panic!("Could not find suitable async runtime"),
    }
}
