use std::borrow::Cow;
use std::sync::Arc;
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
        let file = if cfg!(debug_assertions) {
            record
                .file_static()
                .map(Cow::Borrowed)
                .or_else(|| record.file().map(|s| Cow::Owned(s.to_owned())))
        } else {
            None
        };
        let line = if cfg!(debug_assertions) {
            record.line()
        } else {
            None
        };
        Self {
            msg: record.args().to_string(),
            metadata: record.metadata().into(),
            module_path: record
                .module_path_static()
                .map(Cow::Borrowed)
                .or_else(|| record.module_path().map(|s| Cow::Owned(s.to_owned()))),
            file: file.or(Some(Cow::Borrowed("<none>"))),
            line,
        }
    }
}

enum Message {
    Record(OwnedRecord),
    Flush(channel::Sender<()>),
}

struct Logger {
    inner: Arc<PyLogger>,
    sender: channel::Sender<Message>,
}

impl Logger {
    pub fn new(inner: Arc<PyLogger>) -> Self {
        let (sender, receiver) = channel::unbounded();
        let _thread = {
            let inner = inner.clone();
            thread::Builder::new()
                .name("pathway:logger".to_owned())
                .spawn(move || {
                    let thread_state = PythonThreadState::new();
                    loop {
                        match receiver.recv() {
                            Ok(Message::Record(record)) => record.with(|record| inner.log(&record)),
                            Ok(Message::Flush(sender)) => {
                                inner.flush();
                                sender.send(()).unwrap_or(());
                            }
                            Err(channel::RecvError) => break,
                        };
                    }
                    drop(thread_state);
                })
                .expect("logger thread creation should not fail")
        };
        Self { inner, sender }
    }

    pub fn install(self) -> Result<ResetHandle, SetLoggerError> {
        let reset_handle = self.inner.reset_handle();
        log::set_boxed_logger(Box::new(self))?;
        log::set_max_level(LevelFilter::Debug); // XXX: `pyo3_log::Logger` does not allow us to get the level
        Ok(reset_handle)
    }
}

impl Default for Logger {
    fn default() -> Self {
        Self::new(Arc::new(PyLogger::default()))
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
        self.sender
            .send(Message::Record(record.into()))
            .expect("sending the log record should not fail");
    }

    fn flush(&self) {
        Python::with_gil(|py| {
            py.allow_threads(|| {
                let (sender, receiver) = channel::bounded(1);
                if let Ok(()) = self.sender.send(Message::Flush(sender)) {
                    receiver.recv().unwrap_or(());
                }
            });
        });
    }
}

pub fn init() {
    Logger::default()
        .install()
        .expect("initializing the logger should not fail");
}
