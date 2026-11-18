// Copyright © 2026 Pathway

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use pathway_engine::async_runtime::create_async_tokio_runtime_with_background_worker;

#[test]
fn test_background_runtime_drives_tasks_while_connector_is_blocked() {
    let runtime =
        create_async_tokio_runtime_with_background_worker("pathway-runtime-test").unwrap();
    let handle = runtime.handle().clone();
    let (sender, receiver) = mpsc::channel();

    // Checkpoint completion queues ACKs from another thread. Connectors may
    // meanwhile be blocked on a full input channel or waiting for output rows,
    // outside block_on. Client timers and background tasks must still run.
    let task = thread::spawn(move || {
        handle.spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            sender.send(()).unwrap();
        })
    })
    .join()
    .unwrap();

    receiver
        .recv_timeout(Duration::from_secs(10))
        .expect("background work must progress without another connector call");
    runtime.block_on(task).unwrap();
}
