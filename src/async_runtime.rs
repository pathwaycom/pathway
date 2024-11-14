use std::io;

use tokio::runtime::Runtime as TokioRuntime;

pub fn create_async_tokio_runtime() -> Result<TokioRuntime, io::Error> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}
