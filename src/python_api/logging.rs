// Copyright © 2024 Pathway

#![deny(unsafe_op_in_unsafe_fn)]

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;

use crossbeam_channel as channel;
use log::{Level, LevelFilter, Log, Metadata, Record, SetLoggerError};
use pyo3::Python;
use pyo3_log::{Logger as PyLogger, ResetHandle};

use super::threads::PythonThreadState;

struct OwnedMetadata {
    level: Level,
    target: String,
}

impl OwnedMetadata {
    fn as_metadata(&self) -> Metadata {
        Metadata::builder()
            .level(self.level)
            .target(&self.target)
            .build()
    }
}

impl From<&Metadata<'_>> for OwnedMetadata {
    fn from(metadata: &Metadata<'_>) -> Self {
        Self {
            level: metadata.level(),
            target: metadata.target().to_owned(),
        }
    }
}

struct OwnedRecord {
    msg: String,
    metadata: OwnedMetadata,
    module_path: Option<Cow<'static, str>>,
    file: Option<Cow<'static, str>>,
    line: Option<u32>,
}

impl OwnedRecord {
    fn with<R>(&self, logic: impl FnOnce(Record) -> R) -> R {
        let mut builder = Record::builder();
        builder.metadata(self.metadata.as_metadata());
        match &self.module_path {
            Some(Cow::Borrowed(s)) => builder.module_path_static(Some(s)),
            other => builder.module_path(other.as_deref()),
        };
        match &self.file {
            Some(Cow::Borrowed(s)) => builder.file_static(Some(s)),
            other => builder.file(other.as_deref()),
        };
        builder.line(self.line);
        logic(builder.args(format_args!("{}", self.msg)).build())
    }
}

impl From<&Record<'_>> for OwnedRecord {
    fn from(record: &Record<'_>) -> Self {
        Self {
            msg: record.args().to_string(),
            metadata: record.metadata().into(),
            module_path: record
                .module_path_static()
                .map(Cow::Borrowed)
                .or_else(|| record.module_path().map(|s| Cow::Owned(s.to_owned()))),
            file: record
                .file_static()
                .map(Cow::Borrowed)
                .or_else(|| record.file().map(|s| Cow::Owned(s.to_owned())))
                .or(Some(Cow::Borrowed("<none>"))),
            line: record.line(),
        }
    }
}

enum Message {
    Record(OwnedRecord),
    Flush(channel::Sender<()>),
}

struct Logger {
    inner: Arc<PyLogger>,
    sender: OnceLock<channel::Sender<Message>>,
}

impl Logger {
    fn sender(&self) -> &channel::Sender<Message> {
        self.sender.get_or_init(|| {
            let inner = self.inner.clone();
            let (sender, receiver) = channel::unbounded();
            let _thread = {
                thread::Builder::new()
                    .name("pathway:logger".to_owned())
                    .spawn(move || {
                        let thread_state = PythonThreadState::new();
                        loop {
                            match receiver.recv() {
                                Ok(Message::Record(record)) => {
                                    record.with(|record| inner.log(&record));
                                }
                                Ok(Message::Flush(ack_sender)) => {
                                    inner.flush();
                                    ack_sender.send(()).unwrap_or(());
                                }
                                Err(channel::RecvError) => break,
                            };
                        }
                        drop(thread_state);
                    })
                    .expect("logger thread creation should not fail")
            };
            sender
        })
    }

    pub fn new(inner: Arc<PyLogger>) -> Self {
        let sender = OnceLock::new();
        Self { inner, sender }
    }

    pub fn install(self) -> Result<ResetHandle, SetLoggerError> {
        let reset_handle = self.inner.reset_handle();
        let logger = Box::leak(Box::new(self));
        log::set_logger(logger)?;
        fork_hack::enable(logger);
        log::set_max_level(LevelFilter::Debug); // XXX: `pyo3_log::Logger` does not allow us to get the level
        Ok(reset_handle)
    }
}

impl Default for Logger {
    fn default() -> Self {
        let logger =
            PyLogger::default().filter_target("opentelemetry_sdk".to_owned(), LevelFilter::Warn);
        Self::new(Arc::new(logger))
    }
}

impl Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        // `pyo3_log::Logger` does not take the GIL when running `enabled`
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        if !self.inner.enabled(record.metadata()) {
            return;
        }
        self.sender()
            .send(Message::Record(record.into()))
            .expect("sending the log record should not fail");
    }

    fn flush(&self) {
        let Some(sender) = self.sender.get() else {
            // No need to flush anything if nothing was ever sent
            return;
        };
        Python::with_gil(|py| {
            py.allow_threads(|| {
                let (ack_sender, ack_receiver) = channel::bounded(1);
                if let Ok(()) = sender.send(Message::Flush(ack_sender)) {
                    ack_receiver.recv().unwrap_or(());
                }
            });
        });
    }
}

pub fn init() -> ResetHandle {
    Logger::default()
        .install()
        .expect("initializing the logger should not fail")
}

/// Running non-trivial code after a `fork()` in a multithreading program is not really allowed, but
/// this is what the `multiprocessing` module does.
/// What happens in practice is that threads other than the main one disappear.
/// We register a function to be called after fork in the child that resets the sending channel to
/// uninitialized to try to make it work (next send will recreate the logging thread) – otherwise
/// there is no logging thread that will ever read the sent events and waiting for flush never
/// ends.
mod fork_hack {
    use std::ptr;
    use std::sync::atomic::{AtomicPtr, Ordering};
    use std::sync::OnceLock;

    use super::Logger;

    static LOGGER: AtomicPtr<Logger> = AtomicPtr::new(ptr::null_mut());

    unsafe extern "C" fn child_handler() {
        let logger = LOGGER.load(Ordering::SeqCst);
        let new_sender = OnceLock::new();
        // SAFETY: we are running early after fork, no other code should be accessing the logger
        unsafe {
            ptr::write(ptr::addr_of_mut!((*logger).sender), new_sender);
        }
    }

    pub fn enable(logger: &'static Logger) {
        let logger = ptr::addr_of!(*logger).cast_mut();
        let old_logger = LOGGER.swap(logger, Ordering::SeqCst);
        assert!(old_logger.is_null());

        // SAFETY: `child_handler` is an appropriate handler for `pthread_atfork`
        unsafe {
            libc::pthread_atfork(None, None, Some(child_handler));
        }
    }
}
