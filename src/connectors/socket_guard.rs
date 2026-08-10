// Copyright © 2026 Pathway

//! Shared socket-level guards for every network connector.
//!
//! Every connector ultimately talks to its server through a TCP socket, and a
//! TCP connection can die *silently*: a half-open connection after a peer
//! crash, a NAT/conntrack entry evicted on a loaded host, a middlebox that
//! starts dropping packets. Both ends still consider the connection
//! established, but no byte will ever arrive again. A connector waiting on
//! such a socket with the OS defaults blocks forever (reads) or for tens of
//! minutes (unacknowledged writes) — with nothing logged and no retry
//! machinery running, the pipeline just stops.
//!
//! This module is the single source of truth for the guards that turn such a
//! stall into an ordinary transient error within a bounded time:
//!
//! * [`TCP_CONNECT_TIMEOUT`] — cap on establishing a connection;
//! * [`harden_socket`] — kernel-level liveness probing (TCP keepalive) and a
//!   cap on how long sent data may stay unacknowledged (`TCP_USER_TIMEOUT`),
//!   applicable to *any* TCP socket, blocking or non-blocking;
//! * [`StallGuard`] — a wrapper for async byte streams that errors out when
//!   the wire stays completely silent during a pending read/write for longer
//!   than the profile allows: the async-world equivalent of a blocking
//!   socket's receive timeout (which non-blocking sockets ignore);
//! * [`connect_tcp_guarded`] — bounded connect + hardening + [`StallGuard`]
//!   in one call, for connectors that hand a raw TCP stream to their client
//!   library;
//! * [`http_client_builder`] / [`blocking_http_client_builder`] — `reqwest`
//!   builders with a connect timeout and a total per-request timeout, for the
//!   HTTP-based connectors;
//! * [`SilenceProfile`] — how much wire silence a role tolerates before the
//!   flow is declared dead (readers stream continuously, so they get a
//!   tighter bound than writers, whose single statement can legitimately keep
//!   the server silent while it executes).
//!
//! The timeouts here are deliberately conservative: they fire only on
//! *silence*, never on slow-but-flowing data, so a healthy long operation is
//! not at risk. The values are intentionally uniform across connectors —
//! tune them here, never per connector. See also `mysql.rs`, which applies the same policy through
//! its driver's native options (the driver owns its sockets, so the stream
//! wrapper does not apply there).

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use log::warn;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::{sleep_until, Instant, Sleep};

/// Cap on establishing a TCP connection. Without it, a connect whose SYN
/// packets are lost retries at kernel pace for ~127 s before erroring.
/// Establishing a connection never legitimately takes this long.
pub const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// TCP keepalive: probe an idle flow after 10 s of silence, then every 5 s,
/// and declare it dead after 3 unanswered probes (≈25 s to detection).
/// Keepalive fires only when the peer's *kernel* stops acknowledging, so a
/// healthy server that is merely slow to answer at the protocol level never
/// trips it.
pub const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(10);
pub const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);
pub const TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Cap on how long sent-but-unacknowledged data may sit in the kernel's
/// retransmit queue before the connection is declared dead
/// (`TCP_USER_TIMEOUT`). Without it a blackholed flow with in-flight data
/// keeps retransmitting for ~15 minutes. Linux-only; elsewhere the other
/// guards cover.
pub const TCP_USER_TIMEOUT: Duration = Duration::from_secs(30);

/// Total per-request cap for HTTP-based connectors. HTTP is request-response,
/// so unlike the silence-based guards this bounds the *whole* operation —
/// generous enough for a bulk write of a full batch on a loaded server.
pub const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_mins(1);

/// How much complete wire silence a connector role tolerates during one
/// pending read or write before the flow is declared dead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SilenceProfile {
    /// Reader-side operations (metadata queries, snapshot row streams,
    /// change-log tailing) deliver bytes continuously; half a minute with no
    /// traffic means the flow is dead.
    Reader,
    /// Writer-side: while the server executes one submitted statement it
    /// legitimately sends nothing back, so the bound is more generous.
    Writer,
}

impl SilenceProfile {
    #[must_use]
    pub fn max_silence(self) -> Duration {
        match self {
            SilenceProfile::Reader => Duration::from_secs(30),
            SilenceProfile::Writer => Duration::from_secs(45),
        }
    }
}

/// Apply the kernel-level guards (keepalive + `TCP_USER_TIMEOUT`) to any
/// TCP socket, blocking or non-blocking. `SockRef` borrows the socket by
/// file descriptor, so this works on `std`, `tokio` and library-owned
/// streams alike as long as the raw socket is reachable.
pub fn harden_socket(sock: &SockRef) -> io::Result<()> {
    let keepalive = TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_IDLE)
        .with_interval(TCP_KEEPALIVE_INTERVAL);
    #[cfg(not(target_os = "windows"))]
    let keepalive = keepalive.with_retries(TCP_KEEPALIVE_RETRIES);
    sock.set_tcp_keepalive(&keepalive)?;
    #[cfg(target_os = "linux")]
    sock.set_tcp_user_timeout(Some(TCP_USER_TIMEOUT))?;
    Ok(())
}

/// Establish a TCP connection with every guard applied: the connect itself is
/// bounded by [`TCP_CONNECT_TIMEOUT`], the socket gets the kernel-level
/// guards, and the returned stream is wrapped in a [`StallGuard`] so a silent
/// flow errors out instead of parking the task forever.
pub async fn connect_tcp_guarded(
    addr: impl tokio::net::ToSocketAddrs,
    profile: SilenceProfile,
) -> io::Result<StallGuard<TcpStream>> {
    let stream = tokio::time::timeout(TCP_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "connecting took longer than {}s",
                    TCP_CONNECT_TIMEOUT.as_secs()
                ),
            )
        })??;
    stream.set_nodelay(true)?;
    harden_socket(&SockRef::from(&stream))?;
    Ok(StallGuard::new(stream, profile))
}

/// Async byte-stream wrapper that bounds *wire silence*.
///
/// Non-blocking sockets ignore the classic receive/send timeouts
/// (`SO_RCVTIMEO`/`SO_SNDTIMEO`), so an async client library waiting for
/// bytes simply parks its task — forever, if the flow is dead. This wrapper
/// re-creates the blocking-world semantics at the stream level: while a read
/// (or write) is *pending*, a timer runs; any progress — even a single byte —
/// resets it; if the timer fires first, the pending operation fails with
/// `TimedOut`. A slow-but-flowing transfer therefore never trips it; only
/// complete silence does.
///
/// The error surfaces inside the client library as an ordinary I/O error on
/// its connection, which every library already treats as "connection broken"
/// — feeding directly into the connector's existing reconnect-and-retry
/// machinery.
///
/// The timers are boxed once at construction and re-armed in place, so the
/// wrapper stays `Unpin` (client libraries commonly require that of their
/// streams) and steady-state polling allocates nothing.
pub struct StallGuard<S> {
    inner: S,
    max_silence: Duration,
    read_timer: Pin<Box<Sleep>>,
    read_armed: bool,
    write_timer: Pin<Box<Sleep>>,
    write_armed: bool,
}

impl<S> StallGuard<S> {
    pub fn new(inner: S, profile: SilenceProfile) -> Self {
        let far_future = Instant::now() + Duration::from_hours(24);
        Self {
            inner,
            max_silence: profile.max_silence(),
            read_timer: Box::pin(sleep_until(far_future)),
            read_armed: false,
            write_timer: Box::pin(sleep_until(far_future)),
            write_armed: false,
        }
    }
}

/// Poll one direction with the silence bound: delegate to the inner stream;
/// on progress disarm the timer, on `Pending` arm it (first time only) and
/// fail with `TimedOut` once it fires.
fn poll_with_silence_bound<T>(
    inner_poll: Poll<io::Result<T>>,
    timer: &mut Pin<Box<Sleep>>,
    armed: &mut bool,
    max_silence: Duration,
    cx: &mut Context<'_>,
    direction: &str,
) -> Poll<io::Result<T>> {
    match inner_poll {
        Poll::Ready(result) => {
            *armed = false;
            Poll::Ready(result)
        }
        Poll::Pending => {
            if !*armed {
                timer.as_mut().reset(Instant::now() + max_silence);
                *armed = true;
            }
            if timer.as_mut().poll(cx).is_ready() {
                *armed = false;
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "no {direction} progress on the connection for {}s: \
                         the flow looks dead, reconnecting",
                        max_silence.as_secs()
                    ),
                )));
            }
            Poll::Pending
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for StallGuard<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        poll_with_silence_bound(
            Pin::new(&mut this.inner).poll_read(cx, buf),
            &mut this.read_timer,
            &mut this.read_armed,
            this.max_silence,
            cx,
            "read",
        )
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for StallGuard<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        poll_with_silence_bound(
            Pin::new(&mut this.inner).poll_write(cx, buf),
            &mut this.write_timer,
            &mut this.write_armed,
            this.max_silence,
            cx,
            "write",
        )
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        poll_with_silence_bound(
            Pin::new(&mut this.inner).poll_flush(cx),
            &mut this.write_timer,
            &mut this.write_armed,
            this.max_silence,
            cx,
            "flush",
        )
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Shutdown is best-effort teardown; bounding it like a write keeps a
        // dead flow from stalling connection cleanup.
        let this = self.get_mut();
        poll_with_silence_bound(
            Pin::new(&mut this.inner).poll_shutdown(cx),
            &mut this.write_timer,
            &mut this.write_armed,
            this.max_silence,
            cx,
            "shutdown",
        )
    }
}

/// `reqwest` builder for async HTTP-based connectors with the shared policy
/// applied: bounded connect and a total per-request cap. `reqwest`'s own
/// socket defaults (keepalive, `TCP_USER_TIMEOUT`) already handle dead flows;
/// the request timeout closes the remaining "TCP alive, application silent"
/// case.
pub fn http_client_builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .connect_timeout(TCP_CONNECT_TIMEOUT)
        .timeout(HTTP_REQUEST_TIMEOUT)
}

/// Blocking-flavor twin of [`http_client_builder`].
pub fn blocking_http_client_builder() -> reqwest::blocking::ClientBuilder {
    reqwest::blocking::Client::builder()
        .connect_timeout(TCP_CONNECT_TIMEOUT)
        .timeout(HTTP_REQUEST_TIMEOUT)
}

/// Log a warning for configurations whose transport cannot be guarded from
/// our side (the client library owns the socket and exposes no timeout
/// controls past connection setup). Keep such cases rare and documented.
pub fn warn_unguardable_transport(connector: &str, details: &str) {
    warn!(
        "{connector}: {details}; a silently dead network connection may stall \
         this connector for tens of minutes before the operating system gives \
         up on it"
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::{SilenceProfile, StallGuard};

    /// A read pending on a live-but-silent connection must fail with
    /// `TimedOut` once the silence bound elapses, instead of waiting forever.
    #[test]
    fn read_on_silent_connection_times_out() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let client = TcpStream::connect(addr).await.unwrap();
            // Keep the server side open but silent: the flow is alive at the
            // TCP level, yet no data will ever arrive.
            let (_server, _) = listener.accept().await.unwrap();

            let guarded = StallGuard::new(client, SilenceProfile::Reader);
            tokio::pin!(guarded);
            tokio::time::pause();
            let mut buf = [0_u8; 16];
            let read = guarded.read(&mut buf);
            tokio::pin!(read);
            // Nothing should resolve before the bound...
            tokio::select! {
                biased;
                result = &mut read => panic!("read resolved too early: {result:?}"),
                () = tokio::time::sleep(Duration::from_secs(29)) => {}
            }
            // ...and right after it the read must fail with TimedOut.
            tokio::time::advance(Duration::from_secs(2)).await;
            let result = read.await;
            let error = result.expect_err("silent flow must produce an error");
            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        });
    }

    /// Data trickling in slowly must NOT trip the guard: it bounds silence,
    /// not total duration.
    #[test]
    fn slow_but_flowing_transfer_is_not_interrupted() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let client = TcpStream::connect(addr).await.unwrap();
            let (mut server, _) = listener.accept().await.unwrap();

            let writer = tokio::spawn(async move {
                // Total transfer takes 4 × 20 s = 80 s of wall-clock — far
                // beyond the 30 s reader bound — but every gap stays under it.
                for chunk in [b"aa", b"bb", b"cc", b"dd"] {
                    tokio::time::sleep(Duration::from_secs(20)).await;
                    server.write_all(chunk).await.unwrap();
                }
                drop(server);
            });

            tokio::time::pause();
            let guarded = StallGuard::new(client, SilenceProfile::Reader);
            tokio::pin!(guarded);
            let mut received = Vec::new();
            let mut buf = [0_u8; 16];
            loop {
                let n = guarded
                    .read(&mut buf)
                    .await
                    .expect("a flowing transfer must not be interrupted");
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&buf[..n]);
            }
            assert_eq!(received, b"aabbccdd");
            writer.await.unwrap();
        });
    }
}
