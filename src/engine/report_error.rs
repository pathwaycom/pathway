use std::panic::{catch_unwind, panic_any, AssertUnwindSafe};
use std::{io, thread};

use super::error::{DynError, DynResult, Error, Result, Trace};

pub trait ReportError: Send {
    fn report(&self, error: Error);

    #[track_caller]
    fn report_and_panic(&self, error: impl Into<Error>) -> ! {
        let error = error.into();
        let message = error.to_string();
        self.report(error);
        panic_any(message);
    }

    #[track_caller]
    fn report_and_panic_with_trace(&self, error: impl Into<DynError>, trace: &Trace) -> ! {
        let error = error.into();
        let message = error.to_string();
        self.report(Error::with_trace(error, trace.clone()));
        panic_any(message);
    }
}

pub trait UnwrapWithReporter<T> {
    fn unwrap_with_reporter(self, error_reporter: &impl ReportError) -> T;
    fn unwrap_with_reporter_and_trace(self, error_reporter: &impl ReportError, trace: &Trace) -> T;
}

impl<T> UnwrapWithReporter<T> for DynResult<T> {
    #[track_caller]
    fn unwrap_with_reporter(self, error_reporter: &impl ReportError) -> T {
        self.unwrap_or_else(|err| error_reporter.report_and_panic(err))
    }
    #[track_caller]
    fn unwrap_with_reporter_and_trace(self, error_reporter: &impl ReportError, trace: &Trace) -> T {
        self.unwrap_or_else(|err| error_reporter.report_and_panic_with_trace(err, trace))
    }
}

impl<T> UnwrapWithReporter<T> for Result<T> {
    #[track_caller]
    fn unwrap_with_reporter(self, error_reporter: &impl ReportError) -> T {
        self.unwrap_or_else(|err| error_reporter.report_and_panic(err))
    }
    #[track_caller]
    fn unwrap_with_reporter_and_trace(self, error_reporter: &impl ReportError, trace: &Trace) -> T {
        self.unwrap_or_else(|err| error_reporter.report_and_panic_with_trace(err, trace))
    }
}

impl<T> UnwrapWithReporter<T> for thread::Result<T> {
    #[track_caller]
    fn unwrap_with_reporter(self, error_reporter: &impl ReportError) -> T {
        self.unwrap_or_else(|err| error_reporter.report_and_panic(Error::from_panic_payload(err)))
    }
    #[track_caller]
    fn unwrap_with_reporter_and_trace(self, error_reporter: &impl ReportError, trace: &Trace) -> T {
        self.unwrap_or_else(|err| {
            error_reporter.report_and_panic_with_trace(Error::from_panic_payload(err), trace)
        })
    }
}

pub trait SpawnWithReporter {
    fn spawn_with_reporter(
        self,
        reporter: impl ReportError + 'static,
        f: impl FnOnce() -> DynResult<()> + Send + 'static,
    ) -> io::Result<thread::JoinHandle<()>>;
}

impl SpawnWithReporter for thread::Builder {
    fn spawn_with_reporter(
        self,
        reporter: impl ReportError + 'static,
        f: impl FnOnce() -> DynResult<()> + Send + 'static,
    ) -> io::Result<thread::JoinHandle<()>> {
        self.spawn(move || {
            catch_unwind(AssertUnwindSafe(|| f().unwrap_with_reporter(&reporter)))
                .unwrap_with_reporter(&reporter);
        })
    }
}
