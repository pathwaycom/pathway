use std::time::Duration;

use log::error;
use rand::{rng, Rng};

const DEFAULT_SLEEP_INITIAL_DURATION: Duration = Duration::from_secs(1);
const DEFAULT_SLEEP_BACKOFF_FACTOR: f64 = 1.2;
const DEFAULT_JITTER: Duration = Duration::from_millis(800);

#[allow(clippy::module_name_repetitions)]
pub struct RetryConfig {
    sleep_duration: Duration,
    backoff_factor: f64,
    jitter: Duration,
}

impl RetryConfig {
    pub fn new(sleep_duration: Duration, backoff_factor: f64, jitter: Duration) -> Self {
        Self {
            sleep_duration,
            backoff_factor,
            jitter,
        }
    }

    pub fn sleep_after_error(&mut self) {
        std::thread::sleep(self.sleep_duration);
        self.advance_backoff();
    }

    pub async fn sleep_after_error_async(&mut self) {
        tokio::time::sleep(self.sleep_duration).await;
        self.advance_backoff();
    }

    fn advance_backoff(&mut self) {
        self.sleep_duration = self.sleep_duration.mul_f64(self.backoff_factor)
            + rng().random_range(Duration::ZERO..self.jitter);
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_SLEEP_INITIAL_DURATION,
            DEFAULT_SLEEP_BACKOFF_FACTOR,
            DEFAULT_JITTER,
        )
    }
}

/// Retry `func` up to `max_retries` times when the error matches
/// `should_retry`.  Errors for which `should_retry` returns `false` propagate
/// immediately so permanent failures (bad SQL syntax, missing tables, auth
/// errors, …) don't waste backoff on a guaranteed-fail rerun.
pub fn execute_with_retries_if<T, E, F, P>(
    mut func: F,
    mut should_retry: P,
    mut retry_config: RetryConfig,
    max_retries: usize,
) -> Result<T, E>
where
    E: std::fmt::Debug,
    F: FnMut() -> Result<T, E>,
    P: FnMut(&E) -> bool,
{
    let mut exec_result = func();
    for _ in 0..max_retries {
        match exec_result {
            Ok(_) => return exec_result,
            Err(ref e) if !should_retry(e) => return exec_result,
            Err(_) => {}
        }
        retry_config.sleep_after_error();
        exec_result = func();
    }
    if let Err(ref e) = exec_result {
        error!("Operation failed after {max_retries} retries: {e:?}");
    }

    exec_result
}

/// Retry `func` up to `max_retries` times on any error.  Equivalent to
/// [`execute_with_retries_if`] with an always-true predicate.
pub fn execute_with_retries<T, E: std::fmt::Debug>(
    func: impl FnMut() -> Result<T, E>,
    retry_config: RetryConfig,
    max_retries: usize,
) -> Result<T, E> {
    execute_with_retries_if(func, |_| true, retry_config, max_retries)
}
