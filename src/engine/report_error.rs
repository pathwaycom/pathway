// Copyright © 2024 Pathway

#![allow(clippy::module_name_repetitions)]

use std::panic::{catch_unwind, panic_any, AssertUnwindSafe};
use std::{io, thread};

use super::error::{DataError, DataResult, DynError, DynResult, Error, Result, Trace};

pub trait ReportError: Send {
    fn report(&self, error: Error);
}

impl ReportError for Box<dyn ReportError> {
    fn report(&self, error: Error) {
        self.as_ref().report(error);
    }
}

pub trait ReportErrorExt: ReportError {
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

    fn with_extra<E>(self, extra: E) -> ErrorReporterWithExtra<Self, E>
    where
        Self: Sized,
    {
        ErrorReporterWithExtra {
            reporter: self,
            extra,
        }
    }
}

impl<T: ReportError> ReportErrorExt for T {}

pub struct ErrorReporterWithExtra<R, E> {
    reporter: R,
    extra: E,
}

impl<R, E> ReportError for ErrorReporterWithExtra<R, E>
where
    R: ReportError,
    E: Send,
{
    fn report(&self, error: Error) {
        self.reporter.report(error);
    }
}

impl<R, E> ErrorReporterWithExtra<R, E> {
    pub fn get(&self) -> &E {
        &self.extra
    }

    pub fn get_mut(&mut self) -> &mut E {
        &mut self.extra
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
    fn spawn_with_reporter<R: ReportError + 'static>(
        self,
        reporter: R,
        f: impl FnOnce(&mut R) -> DynResult<()> + Send + 'static,
    ) -> io::Result<thread::JoinHandle<()>>;
}

impl SpawnWithReporter for thread::Builder {
    fn spawn_with_reporter<R>(
        self,
        mut reporter: R,
        f: impl FnOnce(&mut R) -> DynResult<()> + Send + 'static,
    ) -> io::Result<thread::JoinHandle<()>>
    where
        R: ReportError + 'static,
    {
        self.spawn(move || {
            catch_unwind(AssertUnwindSafe(|| {
                f(&mut reporter).unwrap_with_reporter(&reporter);
            }))
            .unwrap_with_reporter(&reporter);
        })
    }
}

pub trait LogError {
    fn log_error(&self, error: DataError);
    fn log_error_with_trace(&self, error: DynError, trace: &Trace);
}

impl<T: ReportError> LogError for T {
    fn log_error(&self, error: DataError) {
        self.report_and_panic(error);
    }

    fn log_error_with_trace(&self, error: DynError, trace: &Trace) {
        self.report_and_panic_with_trace(error, trace);
    }
}

pub trait UnwrapWithErrorLogger<T> {
    fn unwrap_or_log(self, error_logger: &(impl LogError + ?Sized), default: T) -> T
    where
        Self: Sized,
    {
        self.unwrap_or_else_log(error_logger, || default)
    }
    fn unwrap_or_else_log<F: FnOnce() -> T>(
        self,
        error_logger: &(impl LogError + ?Sized),
        op: F,
    ) -> T;
    fn unwrap_or_log_with_trace(
        self,
        error_logger: &(impl LogError + ?Sized),
        trace: &Trace,
        default: T,
    ) -> T;
    fn ok_with_logger(self, error_logger: &(impl LogError + ?Sized)) -> Option<T>;
}

impl<T> UnwrapWithErrorLogger<T> for DynResult<T> {
    #[track_caller]
    fn unwrap_or_else_log<F: FnOnce() -> T>(
        self,
        error_logger: &(impl LogError + ?Sized),
        op: F,
    ) -> T {
        self.unwrap_or_else(|err| {
            error_logger.log_error(err.into());
            op()
        })
    }
    #[track_caller]
    fn unwrap_or_log_with_trace(
        self,
        error_logger: &(impl LogError + ?Sized),
        trace: &Trace,
        default: T,
    ) -> T {
        self.unwrap_or_else(|err| {
            error_logger.log_error_with_trace(err, trace);
            default
        })
    }
    fn ok_with_logger(self, error_logger: &(impl LogError + ?Sized)) -> Option<T> {
        Some(self).transpose().unwrap_or_log(error_logger, None)
    }
}

impl<T> UnwrapWithErrorLogger<T> for DataResult<T> {
    #[track_caller]
    fn unwrap_or_else_log<F: FnOnce() -> T>(
        self,
        error_logger: &(impl LogError + ?Sized),
        op: F,
    ) -> T {
        self.unwrap_or_else(|err| {
            error_logger.log_error(err);
            op()
        })
    }
    #[track_caller]
    fn unwrap_or_log_with_trace(
        self,
        error_logger: &(impl LogError + ?Sized),
        trace: &Trace,
        default: T,
    ) -> T {
        self.unwrap_or_else(|err| {
            error_logger.log_error_with_trace(err.into(), trace);
            default
        })
    }
    fn ok_with_logger(self, error_logger: &(impl LogError + ?Sized)) -> Option<T> {
        Some(self).transpose().unwrap_or_log(error_logger, None)
    }
}
