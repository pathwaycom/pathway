// Copyright © 2026 Pathway

//! Behavioral tests for the generalized polling reader
//! (`pathway_engine::connectors::data_storage::polling`).
//!
//! The reader is exercised through its public [`Reader`] interface only: a
//! controllable in-memory [`MockSource`] stands in for a real timestamped store,
//! and each test drives `read()` / `seek()` and asserts which rows are delivered.
//! This covers the four guarantees of the overlap-polling algorithm — no missed
//! rows, no duplicates, settled-state compaction, and correct resume after
//! restart — without reaching into the reader's private state.
//!
//! The reader derives its notion of "current time" from the largest timestamp it
//! has seen in the data, not from a wall clock, so these tests control the live
//! edge purely by the timestamps they stage — there is no clock to advance.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pathway_engine::connectors::data_storage::{
    ConnectorMode, LiveState, PolledRow, PollingDataSource, PollingReader, ReadError, ReadResult,
    Reader, ReaderContext, StorageType,
};
use pathway_engine::connectors::offset::{OffsetKey, OffsetValue};
use pathway_engine::persistence::frontier::OffsetAntichain;

/// A cloneable handle to a [`MockSource`]'s rows. The reader takes ownership of
/// the `MockSource`, so the test keeps a clone of this handle to stage new rows
/// between polls. `Arc`/`Mutex` keep it `Send`, as [`PollingDataSource`] requires.
#[derive(Clone)]
struct MockHandle {
    rows: Arc<Mutex<Vec<PolledRow>>>,
    /// Number of `fetch` calls the reader has made — lets tests assert that the
    /// fingerprint fast path skips the re-read when the source is unchanged.
    fetches: Arc<AtomicU64>,
}

impl MockHandle {
    fn new() -> Self {
        Self {
            rows: Arc::new(Mutex::new(Vec::new())),
            fetches: Arc::new(AtomicU64::new(0)),
        }
    }

    fn insert(&self, id: &str, timestamp: i64) {
        self.rows.lock().unwrap().push(PolledRow {
            id: id.to_string(),
            timestamp,
            raw: id.as_bytes().to_vec(),
        });
    }

    fn fetch_count(&self) -> u64 {
        self.fetches.load(Ordering::SeqCst)
    }
}

/// An in-memory source. `fetch` returns up to `limit` rows at or after the
/// watermark, ordered `(timestamp, id)` ascending — exactly the contract a real
/// source (e.g. an Elasticsearch range query with `size = limit`) provides.
struct MockSource {
    handle: MockHandle,
}

impl PollingDataSource for MockSource {
    fn fetch(&mut self, min_watermark: i64, limit: usize) -> Result<Vec<PolledRow>, ReadError> {
        self.handle.fetches.fetch_add(1, Ordering::SeqCst);
        let mut result: Vec<PolledRow> = self
            .handle
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.timestamp >= min_watermark)
            .cloned()
            .collect();
        result.sort_by_key(|row| (row.timestamp, row.id.clone()));
        result.truncate(limit);
        Ok(result)
    }

    fn live_state(
        &mut self,
        max_transaction_duration: i64,
    ) -> Result<Option<LiveState>, ReadError> {
        let rows = self.handle.rows.lock().unwrap();
        let Some(max_timestamp) = rows.iter().map(|row| row.timestamp).max() else {
            return Ok(None);
        };
        let lower_bound = max_timestamp.saturating_sub(max_transaction_duration);
        let overlap_count = rows
            .iter()
            .filter(|row| row.timestamp >= lower_bound)
            .count() as u64;
        Ok(Some(LiveState {
            max_timestamp,
            overlap_count,
        }))
    }

    fn storage_type(&self) -> StorageType {
        StorageType::ElasticSearch
    }

    fn offset_key(&self) -> OffsetKey {
        OffsetKey::ElasticSearch
    }

    fn short_description(&self) -> Cow<'static, str> {
        "MockSource".into()
    }
}

/// `max_transaction_duration` for the streaming tests below: a row is considered
/// settled once it is more than this far behind the newest timestamp seen so far,
/// i.e. the watermark settles to `max_observed_timestamp - MAX_TXN_DURATION`.
const MAX_TXN_DURATION: i64 = 1000;

/// A `base_limit` large enough that the non-pagination tests always fetch their
/// whole (tiny) data set in a single page.
const LARGE_LIMIT: usize = 1024;

fn make_reader_with_limit(
    handle: &MockHandle,
    mode: ConnectorMode,
    base_limit: usize,
) -> PollingReader<MockSource> {
    PollingReader::new(
        MockSource {
            handle: handle.clone(),
        },
        mode,
        MAX_TXN_DURATION,
        base_limit,
        // Tiny interval: the tests only call `collect_block` when the next poll
        // is guaranteed to deliver rows, so this sleep is never actually hit.
        Duration::from_millis(1),
    )
}

fn make_reader(handle: &MockHandle, mode: ConnectorMode) -> PollingReader<MockSource> {
    make_reader_with_limit(handle, mode, LARGE_LIMIT)
}

/// Drive `read()` through exactly one `NewSource … FinishedSource` block (one
/// poll) and return the ids delivered in it, in order. Must only be called when
/// the upcoming poll is expected to produce at least one row — otherwise a
/// streaming reader would poll-and-sleep indefinitely.
fn collect_block(reader: &mut PollingReader<MockSource>) -> Vec<String> {
    let mut ids = Vec::new();
    loop {
        match reader.read().unwrap() {
            ReadResult::Data(ReaderContext::RawBytes(_, raw), _) => {
                ids.push(String::from_utf8(raw).unwrap());
            }
            ReadResult::FinishedSource { .. } | ReadResult::Finished => break,
            ReadResult::NewSource(_) => {}
            other => panic!("unexpected read result: {other:?}"),
        }
    }
    ids
}

/// Drive a static reader to completion, returning every id it delivers across all
/// of its (possibly many) bounded blocks, plus the number of blocks it took.
fn drain_static(reader: &mut PollingReader<MockSource>) -> (Vec<String>, usize) {
    let mut ids = Vec::new();
    let mut blocks = 0;
    loop {
        match reader.read().unwrap() {
            ReadResult::Data(ReaderContext::RawBytes(_, raw), _) => {
                ids.push(String::from_utf8(raw).unwrap());
            }
            ReadResult::NewSource(_) => blocks += 1,
            ReadResult::FinishedSource { .. } => {}
            ReadResult::Finished => break,
            other => panic!("unexpected read result: {other:?}"),
        }
    }
    (ids, blocks)
}

#[test]
fn test_static_read_delivers_all_rows_once() {
    let handle = MockHandle::new();
    handle.insert("a", 1);
    handle.insert("b", 2);
    handle.insert("c", 3);
    let mut reader = make_reader(&handle, ConnectorMode::Static);

    let mut delivered = Vec::new();
    loop {
        match reader.read().unwrap() {
            ReadResult::Data(ReaderContext::RawBytes(_, raw), _) => {
                delivered.push(String::from_utf8(raw).unwrap());
            }
            ReadResult::Finished => break,
            _ => {}
        }
    }
    delivered.sort();
    assert_eq!(delivered, vec!["a", "b", "c"]);
}

#[test]
fn test_streaming_overlap_deduplicates() {
    let handle = MockHandle::new();
    handle.insert("a", 100);
    handle.insert("b", 200);
    // The newest timestamp (200) minus MAX_TXN_DURATION (1000) is below both
    // rows, so the watermark stays under them and they remain in the overlap
    // window — exactly the live-edge case where deduplication matters.
    let mut reader = make_reader(&handle, ConnectorMode::Streaming);

    let mut first = collect_block(&mut reader);
    first.sort();
    assert_eq!(first, vec!["a", "b"]);

    // A new row arrives inside the still-open overlap window. The next poll
    // re-reads a, b and c, but a and b were already delivered and must be
    // suppressed — only c is delivered.
    handle.insert("c", 250);
    assert_eq!(collect_block(&mut reader), vec!["c"]);
}

#[test]
fn test_streaming_late_arrival_is_not_missed() {
    let handle = MockHandle::new();
    handle.insert("a", 100);
    let mut reader = make_reader(&handle, ConnectorMode::Streaming);

    assert_eq!(collect_block(&mut reader), vec!["a"]);

    // A slow concurrent writer commits a row with an *older* timestamp than one
    // already delivered — the case the overlap window exists for. It is still at
    // or above the (un-advanced) watermark, so it must be picked up.
    handle.insert("late", 50);
    assert_eq!(collect_block(&mut reader), vec!["late"]);
}

#[test]
fn test_streaming_settled_rows_are_not_redelivered() {
    let handle = MockHandle::new();
    handle.insert("a", 100);
    handle.insert("b", 6000);
    // The newest timestamp 6000 pushes the watermark to 6000 - 1000 = 5000, so
    // `a` (100) settles immediately while `b` (6000) stays in the overlap. Both
    // are delivered on this first poll (neither was seen before).
    let mut reader = make_reader(&handle, ConnectorMode::Streaming);

    let mut first = collect_block(&mut reader);
    first.sort();
    assert_eq!(first, vec!["a", "b"]);

    // `old` lands below the settled watermark (it could never have appeared under
    // the max_transaction_duration assumption): the next fetch starts at the
    // watermark, so `old` is never even read. `c` is fresh and must be delivered,
    // while the still-pending `b` must not be redelivered.
    handle.insert("old", 50);
    handle.insert("c", 7000);
    assert_eq!(collect_block(&mut reader), vec!["c"]);
}

#[test]
fn test_resume_after_seek_deduplicates_pending_rows() {
    let handle = MockHandle::new();
    handle.insert("a", 100); // delivered & persisted before the "restart"
    handle.insert("b", 150); // new since the checkpoint
    let mut reader = make_reader(&handle, ConnectorMode::Streaming);

    // Restore state as if `a` had been delivered and recorded in the overlap.
    let mut frontier = OffsetAntichain::new();
    frontier.advance_offset(
        OffsetKey::ElasticSearch,
        OffsetValue::PollingWatermark {
            watermark: i64::MIN,
            entries_read: 1,
            pending: vec![("a".to_string(), 100)],
        },
    );
    reader.seek(&frontier).unwrap();

    // Only the genuinely new row is delivered; `a` is suppressed by the restored
    // pending set.
    assert_eq!(collect_block(&mut reader), vec!["b"]);
}

#[test]
fn test_static_pagination_delivers_each_row_once_across_pages() {
    // Ten distinct timestamps with a base limit of three forces the reader to page
    // through the backlog in several bounded blocks rather than one big fetch.
    let handle = MockHandle::new();
    for i in 0..10 {
        handle.insert(&format!("r{i}"), i64::from(i));
    }
    let mut reader = make_reader_with_limit(&handle, ConnectorMode::Static, 3);

    let (mut delivered, blocks) = drain_static(&mut reader);
    delivered.sort();
    let mut expected: Vec<String> = (0..10).map(|i| format!("r{i}")).collect();
    expected.sort();
    assert_eq!(delivered, expected);
    // It genuinely paged: more than one block was produced, so no single minibatch
    // held the whole backlog.
    assert!(blocks > 1, "expected multiple pages, got {blocks}");
}

#[test]
fn test_static_single_timestamp_larger_than_limit_grows_until_exhausted() {
    // Five rows share one timestamp while the base limit is two. A fixed limit
    // would truncate that timestamp forever; the reader must grow the limit until
    // the whole bucket is read.
    let handle = MockHandle::new();
    for id in ["a", "b", "c", "d", "e"] {
        handle.insert(id, 100);
    }
    let mut reader = make_reader_with_limit(&handle, ConnectorMode::Static, 2);

    let (mut delivered, _) = drain_static(&mut reader);
    delivered.sort();
    assert_eq!(delivered, vec!["a", "b", "c", "d", "e"]);
}

#[test]
fn test_streaming_overlap_window_wider_than_limit_still_delivers_new_rows() {
    // The newest timestamp is 100, so safe_ts = 100 - 1000 < 0 and all rows sit in
    // the still-open overlap window. That window (three rows) is wider than the
    // base limit of two, so reaching the not-yet-delivered tail of it requires the
    // reader to grow the limit past the already-delivered prefix.
    let handle = MockHandle::new();
    handle.insert("a", 100);
    handle.insert("b", 100);
    handle.insert("c", 100);
    let mut reader = make_reader_with_limit(&handle, ConnectorMode::Streaming, 2);

    let mut first = collect_block(&mut reader);
    first.sort();
    assert_eq!(first, vec!["a", "b", "c"]);

    // A fourth row lands in the same overlap window. The next poll re-reads the
    // whole window, suppresses a/b/c, and delivers only d.
    handle.insert("d", 100);
    assert_eq!(collect_block(&mut reader), vec!["d"]);
}

/// Collect blocks from a streaming reader until at least `n` rows have been
/// delivered. Only safe when at least `n` rows are pending (otherwise the reader
/// would poll-and-sleep). With a small base limit a single logical drain can span
/// several blocks, so this accumulates across them.
fn collect_at_least(reader: &mut PollingReader<MockSource>, n: usize) -> Vec<String> {
    let mut ids = Vec::new();
    while ids.len() < n {
        ids.extend(collect_block(reader));
    }
    ids
}

#[test]
fn test_streaming_partial_window_fetch_never_re_emits() {
    // Overlap window wider than the base limit (so a single fetch can never return
    // all of it), plus an out-of-order late arrival below already-delivered rows —
    // the combination that, with a rebuilt-from-fetch pending set, would drop the
    // higher already-delivered ids and re-emit them. Because `pending` is now
    // maintained incrementally (add on deliver, evict on settle), no id is ever
    // delivered twice.
    let handle = MockHandle::new();
    // Wide window (MAX_TXN_DURATION = 1000): everything below stays unsettled.
    for (id, ts) in [("b", 200), ("c", 300), ("d", 400), ("e", 500)] {
        handle.insert(id, ts);
    }
    let mut reader = make_reader_with_limit(&handle, ConnectorMode::Streaming, 2);

    let mut all = collect_at_least(&mut reader, 4); // b, c, d, e (across several blocks)

    // A late row with the *lowest* timestamp arrives, then higher ones — exactly
    // the sequence that used to drop b..e from the dedup set and re-emit them.
    handle.insert("late", 100);
    all.extend(collect_at_least(&mut reader, 1));
    handle.insert("f", 600);
    all.extend(collect_at_least(&mut reader, 1));
    handle.insert("g", 700);
    all.extend(collect_at_least(&mut reader, 1));

    // Nothing delivered twice.
    let unique: std::collections::HashSet<&String> = all.iter().collect();
    assert_eq!(all.len(), unique.len(), "an id was re-emitted: {all:?}");
    all.sort();
    assert_eq!(all, vec!["b", "c", "d", "e", "f", "g", "late"]);
}

#[test]
fn test_streaming_skips_refetch_while_overlap_window_unchanged() {
    let handle = MockHandle::new();
    handle.insert("a", 100);
    let mut reader = make_reader(&handle, ConnectorMode::Streaming);

    // First poll drains the source and records the live-edge fingerprint.
    assert_eq!(collect_block(&mut reader), vec!["a"]);
    let fetches_after_drain = handle.fetch_count();

    // While the source is unchanged the reader must keep polling but never
    // re-fetch — the fingerprint matches, so the overlap re-read is skipped. A
    // quiescent streaming reader never returns from `read()`, so observe it from
    // another thread: it stays parked on the fingerprint check.
    let h = handle.clone();
    let join = std::thread::spawn(move || collect_block(&mut reader));
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        h.fetch_count(),
        fetches_after_drain,
        "an unchanged overlap window must not be re-fetched",
    );

    // A new row changes the fingerprint, which wakes the reader: it re-fetches,
    // delivers the new row, and the helper thread returns.
    h.insert("b", 200);
    let delivered = join.join().unwrap();
    assert_eq!(delivered, vec!["b"]);
    assert!(handle.fetch_count() > fetches_after_drain);
}

#[test]
fn test_streaming_detects_late_in_window_arrival_below_max() {
    let handle = MockHandle::new();
    handle.insert("a", 100);
    handle.insert("b", 200); // establishes the maximum timestamp
    let mut reader = make_reader(&handle, ConnectorMode::Streaming);

    let mut first = collect_block(&mut reader);
    first.sort();
    assert_eq!(first, vec!["a", "b"]);

    // A row arrives *inside* the overlap window but *below* the maximum timestamp
    // — the late-commit case the window exists for. It changes the window's row
    // count without changing the maximum, so the `(max, overlap_count)`
    // fingerprint still detects it and the reader delivers it. (A fingerprint that
    // counted only rows at the maximum would miss this and skip forever.)
    handle.insert("mid", 150);
    assert_eq!(collect_block(&mut reader), vec!["mid"]);
}

/// Cold start, append-only, a single timestamp holding 20x the base limit of rows.
/// The reader must grow the limit (base -> 4x -> 16x -> 64x) until the whole bucket
/// is read in one shot, delivering every row exactly once. In streaming mode the
/// bucket additionally sits in the overlap window (safe_ts = 1000 - MAX_TXN_DURATION
/// < 1000), so the same growth must happen at the live edge.
fn assert_single_hot_timestamp_delivered_once(mode: ConnectorMode) {
    let base_limit = 4usize;
    let n = base_limit * 20; // 80 rows, all at timestamp 1000
    let handle = MockHandle::new();
    for i in 0..n {
        handle.insert(&format!("h{i}"), 1000);
    }
    let mut reader = make_reader_with_limit(&handle, mode, base_limit);

    let delivered = if mode.is_polling_enabled() {
        collect_at_least(&mut reader, n)
    } else {
        drain_static(&mut reader).0
    };
    let unique: std::collections::HashSet<&String> = delivered.iter().collect();
    assert_eq!(delivered.len(), n, "every row delivered exactly once");
    assert_eq!(unique.len(), n, "no duplicates");
}

#[test]
fn test_static_cold_start_single_timestamp_20x_limit_delivers_each_once() {
    assert_single_hot_timestamp_delivered_once(ConnectorMode::Static);
}

#[test]
fn test_streaming_cold_start_single_timestamp_20x_limit_delivers_each_once() {
    assert_single_hot_timestamp_delivered_once(ConnectorMode::Streaming);
}
