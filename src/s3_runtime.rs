// Copyright © 2026 Pathway

use tokio::runtime::{Builder, Runtime};

// rust-s3's tokio backend keeps a pooled `hyper::Client` inside each `Bucket`,
// but that pool only survives across requests if the *same* runtime drives
// every call — a pooled connection's driver task lives on the runtime that
// created it. So every S3-touching struct (the connector scanner, the delta
// object downloader, the S3 persistence backend) owns one of these runtimes for
// its lifetime and reuses it across calls, which is what lets DNS + TLS be
// resolved once and the connection reused.
//
// Crucially the runtime is NOT a process-wide singleton. Pathway runs pipelines
// in forked children (`multiprocessing` in tests, `pathway spawn` in
// production), and `fork()` in a process that has live tokio worker threads is
// unsafe: the child keeps only the forking thread, so any lock those threads
// held (glibc malloc arenas, tokio internals) stays frozen forever and the
// child deadlocks or segfaults on its first allocation. Tying each runtime to a
// struct that is created — and dropped — inside the process that actually uses
// it keeps a forking process free of lingering S3 worker threads: a streaming
// reader builds its runtime in the child, and an in-process (static) reader
// drops its runtime when the dataflow is torn down, before the next fork.
pub(crate) fn build_s3_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("pathway-s3")
        .build()
        .expect("failed to build the S3 tokio runtime")
}
