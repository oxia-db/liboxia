//! Retry loop for the client's long-running background tasks (shard-assignment
//! watcher, session keep-alive, notification and sequence-update listeners).
//!
//! This replaces the unmaintained `backoff` crate. Unlike `backoff`'s default
//! policy, there is **no** maximum elapsed time: a background loop retries until
//! its [`CancellationToken`] is cancelled, so a transient outage never silently
//! kills the loop. The delay resets after any attempt that ran long enough to
//! look healthy, so a stream that worked for a while before a hiccup reconnects
//! promptly instead of waiting out a maxed-out backoff.

use crate::errors::OxiaError;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const INITIAL_DELAY: Duration = Duration::from_millis(100);
const MAX_DELAY: Duration = Duration::from_secs(30);
/// An attempt that ran at least this long is treated as progress and resets the
/// backoff, so long-lived streams reconnect quickly after an occasional error.
const RESET_AFTER: Duration = Duration::from_secs(10);

/// The failure mode of a single attempt of a retried operation.
pub(crate) enum RetryError {
    /// A transient failure; retry after a backoff delay.
    Transient(OxiaError),
    /// A permanent failure; stop the loop without retrying.
    Fatal(OxiaError),
}

impl RetryError {
    /// Marks `err` as transient (will be retried). Usable as a `map_err` fn.
    pub(crate) fn transient(err: OxiaError) -> Self {
        RetryError::Transient(err)
    }

    /// Marks `err` as permanent (stops the loop). Usable as a `map_err` fn.
    pub(crate) fn fatal(err: OxiaError) -> Self {
        RetryError::Fatal(err)
    }
}

struct Backoff {
    current: Duration,
}

impl Backoff {
    fn new() -> Self {
        Backoff {
            current: INITIAL_DELAY,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(MAX_DELAY);
        delay
    }

    fn reset(&mut self) {
        self.current = INITIAL_DELAY;
    }
}

/// Runs `op` repeatedly until it returns `Ok(())` (finished gracefully) or a
/// [`RetryError::Fatal`], or `token` is cancelled.
///
/// [`RetryError::Transient`] results are retried with unbounded exponential
/// backoff. Cancellation interrupts the delay between attempts; the operation
/// itself is expected to observe `token` (typically via `tokio::select!`) to
/// interrupt a live attempt.
pub(crate) async fn retry_until_cancelled<F, Fut>(token: &CancellationToken, what: &str, mut op: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), RetryError>>,
{
    let mut backoff = Backoff::new();
    loop {
        let started = Instant::now();
        let result = op().await;
        let healthy = started.elapsed() >= RESET_AFTER;
        match result {
            Ok(()) => return,
            Err(RetryError::Fatal(err)) => {
                warn!("Background task '{what}' stopped permanently: {err}");
                return;
            }
            Err(RetryError::Transient(err)) => {
                if healthy {
                    backoff.reset();
                }
                let delay = backoff.next_delay();
                warn!("Background task '{what}' failed, retrying in {delay:?}: {err}");
                tokio::select! {
                    _ = token.cancelled() => return,
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_caps_and_resets() {
        let mut b = Backoff::new();
        assert_eq!(b.next_delay(), Duration::from_millis(100));
        assert_eq!(b.next_delay(), Duration::from_millis(200));
        assert_eq!(b.next_delay(), Duration::from_millis(400));
        // Keep growing until it saturates at MAX_DELAY.
        for _ in 0..20 {
            b.next_delay();
        }
        assert_eq!(b.next_delay(), MAX_DELAY);
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_millis(100));
    }

    #[tokio::test]
    async fn stops_on_cancellation_during_delay() {
        let token = CancellationToken::new();
        token.cancel();
        // Every attempt fails transiently; with the token already cancelled the
        // loop must return instead of retrying forever.
        retry_until_cancelled(&token, "test", || async {
            Err(RetryError::transient(OxiaError::Timeout))
        })
        .await;
    }

    #[tokio::test]
    async fn stops_on_fatal() {
        let token = CancellationToken::new();
        retry_until_cancelled(&token, "test", || async {
            Err(RetryError::fatal(OxiaError::SessionExpired))
        })
        .await;
    }
}
