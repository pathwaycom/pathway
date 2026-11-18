use std::time::Duration;

use log::{error, warn};
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
        std::thread::sleep(self.next_delay());
    }

    /// The delay to apply before the next attempt, advancing the schedule.
    /// For the callers that drive the sleeping themselves (e.g. in small
    /// quanta) while reusing the backoff-with-jitter schedule.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.sleep_duration;
        self.advance_backoff();
        delay
    }

    pub async fn sleep_after_error_async(&mut self) {
        tokio::time::sleep(self.sleep_duration).await;
        self.advance_backoff();
    }

    fn advance_backoff(&mut self) {
        // Saturating: a long retry streak must level off, not panic —
        // `Duration::mul_f64` overflows after a few hundred advances at the
        // default factor, and the indefinitely-retrying callers (e.g. the
        // schema-registry lookups) reach that in a couple of hours of
        // outage.
        let multiplied =
            Duration::try_from_secs_f64(self.sleep_duration.as_secs_f64() * self.backoff_factor)
                .unwrap_or(Duration::MAX);
        self.sleep_duration =
            multiplied.saturating_add(rng().random_range(Duration::ZERO..self.jitter));
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
    for n_attempt in 0..max_retries {
        match exec_result {
            Ok(_) => return exec_result,
            Err(ref e) if !should_retry(e) => return exec_result,
            // A retried attempt that eventually succeeds leaves no trace
            // otherwise, which hides a flaky backend behind a later failure.
            Err(ref e) => warn!("Attempt {n_attempt} failed, retrying: {e:?}"),
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

/// Async sibling of [`execute_with_retries_if`].  Use this when the work
/// runs inside an existing async context (so `tokio::time::sleep` is
/// available and `std::thread::sleep` would stall the runtime) and the
/// closure needs to `.await`.  Relies on `AsyncFnMut` (stable since
/// Rust 1.85) so the closure can borrow across iterations.
pub async fn execute_with_retries_if_async<T, E, F, P>(
    mut func: F,
    mut should_retry: P,
    mut retry_config: RetryConfig,
    max_retries: usize,
) -> Result<T, E>
where
    E: std::fmt::Debug,
    F: AsyncFnMut() -> Result<T, E>,
    P: FnMut(&E) -> bool,
{
    let mut exec_result = func().await;
    for n_attempt in 0..max_retries {
        match exec_result {
            Ok(_) => return exec_result,
            Err(ref e) if !should_retry(e) => return exec_result,
            // A retried attempt that eventually succeeds leaves no trace
            // otherwise, which hides a flaky backend behind a later failure.
            Err(ref e) => warn!("Attempt {n_attempt} failed, retrying: {e:?}"),
        }
        retry_config.sleep_after_error_async().await;
        exec_result = func().await;
    }
    if let Err(ref e) = exec_result {
        error!("Operation failed after {max_retries} retries: {e:?}");
    }
    exec_result
}

/// Async sibling of [`execute_with_retries`] — retries on any error.
pub async fn execute_with_retries_async<T, E: std::fmt::Debug>(
    func: impl AsyncFnMut() -> Result<T, E>,
    retry_config: RetryConfig,
    max_retries: usize,
) -> Result<T, E> {
    execute_with_retries_if_async(func, |_| true, retry_config, max_retries).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_advancement_saturates_instead_of_panicking() {
        let mut config = RetryConfig::default();
        // A couple of hours of an outage at the default schedule used to
        // overflow `Duration::mul_f64`; the schedule must level off instead.
        for _ in 0..10_000 {
            let _ = config.next_delay();
        }
        assert!(config.next_delay() >= Duration::from_secs(1));
    }
}
