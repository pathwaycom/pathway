use std::io;

use tokio::runtime::Runtime as TokioRuntime;

pub fn create_async_tokio_runtime() -> Result<TokioRuntime, io::Error> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// For clients whose heartbeats or deferred acknowledgements must keep running
/// between `block_on` calls, including while a connector waits on backpressure.
/// The connector owns this runtime for the lifetime of its client.
pub fn create_async_tokio_runtime_with_background_worker(
    thread_name: &str,
) -> Result<TokioRuntime, io::Error> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .thread_name(thread_name)
        .build()
}
