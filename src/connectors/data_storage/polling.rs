// Copyright © 2026 Pathway

//! Generalized polling input mechanism for sources that lack a change-data-capture
//! (CDC) API.
//!
//! Some sources expose no log of changes that could be tailed for streaming. The
//! only way to ingest from them incrementally is to poll: repeatedly query for
//! "everything that is new", and reconcile the overlap between consecutive
//! queries so that no row is missed and no row is delivered twice.
//!
//! The reconciliation lives entirely in this module. A concrete source only has
//! to implement [`PollingDataSource`] — fetching a *bounded page* of rows at or
//! after a watermark, and reporting a fingerprint of its live edge — and
//! [`PollingReader`] takes care of the watermark bookkeeping, the overlap
//! deduplication, the pagination, the persistent offset, and the [`Reader`]
//! contract. The algorithm is fully generic: it derives its notion of "current
//! time" from the timestamps in the data, so it never assumes `timestamp_column`
//! is expressed in wall-clock units or aligned with this machine's clock.
//!
//! # Applicability and preconditions
//!
//! This is an **append-only** ingestion mechanism. Within the preconditions below
//! it delivers every row exactly once (modulo at-least-once replay across a
//! restart); outside them it can silently lose or re-deliver rows.
//!
//! * **Append-only.** Rows are inserted, never updated in place or deleted (or
//!   such changes need not be propagated). Deduplication is keyed on `id_column`,
//!   so a row that re-appears under the same id is treated as already-seen and is
//!   not re-delivered, and a deletion is never observed.
//! * **Unique, canonicalizable ids.** Two distinct rows must never canonicalize to
//!   the same `id_column` string — e.g. a numeric `1` and a string `"1"` collide,
//!   and one is dropped as a false duplicate.
//! * **Trustworthy timestamps with bounded lateness.** A row must become visible
//!   no later than when the maximum timestamp reaches `its_timestamp +
//!   max_transaction_duration`. A single row with a timestamp far in the future
//!   (clock skew, bad data) jumps the maximum — and hence the watermark — past all
//!   normal rows, silently dropping everything indexed afterwards. Validate
//!   timestamps upstream.
//!
//! Correctness still holds but cost grows when:
//!
//! * The overlap window (`write_rate * max_transaction_duration` rows) is large:
//!   every in-window row is re-read on each poll until it settles, so read traffic
//!   is roughly `max_transaction_duration / poll_interval` times the ingest
//!   volume, and the dedup set and persisted offset are proportional to the window.
//! * All live data falls within a single `max_transaction_duration` window (e.g. a
//!   bounded bulk load whose timestamps are all close together): nothing settles,
//!   so the dedup set and offset grow to the size of the whole dataset.
//! * A single timestamp value holds more rows than fit in memory: with no
//!   secondary id cursor the reader must read that whole bucket as one batch.
//!
//! # Algorithm
//!
//! The reader keeps two pieces of persistent state:
//!
//! * `watermark` — the highest `timestamp_column` value strictly below which every
//!   row has already been delivered and will never be re-read.
//! * `pending` — the already-delivered rows that still fall inside the overlap
//!   window (`timestamp_column >= watermark`), each kept as an `(id, timestamp)`
//!   pair. It is maintained *incrementally*: an id is inserted when it is
//!   delivered and removed only once its timestamp drops below the watermark. It
//!   is never rebuilt from a single fetch, so a fetch that returns only part of
//!   the window cannot drop an already-delivered id (which would re-emit it). Ids
//!   below the watermark are never re-fetched, so they are never stored.
//!
//! ## Bounded fetches
//!
//! A naive "fetch everything at or after the watermark" is unusable: on a cold
//! start it would pull the entire store into one minibatch. Instead each fetch is
//! capped at a `limit`, and the reader pages through the backlog one bounded block
//! at a time. A page that comes back **shorter** than the requested limit means
//! the source is drained up to the live edge; a **full** page means there is more
//! to read and the reader keeps paging immediately (no sleep) until it drains.
//!
//! Because the only cursor into the source is `(watermark, limit)` — there is no
//! secondary id cursor — the largest timestamp present in a full page may be
//! *truncated*: more rows could share that exact timestamp just beyond the limit.
//! The reader therefore never trusts the top timestamp bucket of a full page; it
//! delivers only rows strictly below it and re-reads that bucket on the next page.
//!
//! If a *single timestamp value* holds more rows than the limit (or the live-edge
//! overlap window does), a fixed limit would make no progress: the same truncated
//! page would come back forever. When a full page yields neither a deliverable new
//! row nor any watermark advance, the reader grows the limit by a constant factor
//! and refetches from the same watermark, repeating until the page either spans
//! more than one timestamp (progress is possible) or comes back short (drained).
//! This growth is unbounded by design — with no id cursor it is the only way to
//! read past a hot timestamp — so a single timestamp's row count must fit in
//! memory.
//!
//! ## Settling the watermark
//!
//! Because a page is bounded, the largest timestamp *in the page* is not the
//! largest timestamp *in the source*. The settled cutoff is therefore derived from
//! [`PollingDataSource::live_state`] — the live maximum queried directly — rather
//! than from the page:
//!
//! 1. `safe_ts = max_timestamp - max_transaction_duration`. By assumption
//!    no concurrent writer can still make a row appear whose timestamp is more than
//!    `max_transaction_duration` behind the newest timestamp already visible
//!    (`max_transaction_duration` bounds the out-of-orderness / transaction
//!    duration), so every row with `timestamp < safe_ts` is settled.
//! 2. The watermark advances to `max(watermark, min(safe_ts, page_boundary))`,
//!    where `page_boundary` is the top timestamp bucket of a full page (or `+∞`
//!    once the page is short and the whole tail is known complete). It never moves
//!    backwards.
//! 3. Newly delivered rows are inserted into `pending`; entries whose timestamp is
//!    now below the watermark are evicted (they have settled and will never be
//!    re-fetched). What remains at or above the watermark is deduplicated against
//!    on the next overlapping poll. `pending` is only added to and evicted from —
//!    never rebuilt from a fetch — so a partial fetch can never lose an entry.
//!
//! ## Skipping unchanged polls
//!
//! At the live edge the watermark parks at `safe_ts` and every poll would re-read
//! the whole overlap window just to discover it is unchanged. To avoid that, the
//! source also reports a [`LiveState`] fingerprint — `(max_timestamp, count of
//! rows in the overlap window)`. Whenever this fingerprint equals the one observed
//! the last time the reader drained, the window is provably identical (the count
//! is monotonic for a fixed maximum, so any new in-window row would have changed
//! it), so the poll returns immediately without fetching. The steady-state cost
//! drops from re-reading the window every poll to a single O(1) fingerprint query.
//! The fingerprint is recomputed, never persisted, so the first poll after a
//! restart always re-establishes the window before the skip can engage.
//!
//! Under the preconditions above this guarantees no missed rows (a late-arriving
//! row has `timestamp >= safe_ts` of every future poll, so the watermark never
//! passes it before it is seen), no
//! duplicates (the overlap is deduplicated by id, and a truncated bucket is
//! re-read rather than half-delivered), bounded memory per minibatch (each page is
//! capped), and correct resume after restart (the persisted `(watermark,
//! pending)` pair is exactly the state needed to continue).

use std::borrow::Cow;
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

use crate::connectors::data_storage::{
    CommitPossibility, ConnectorMode, ReadError, ReadResult, Reader, StorageType,
};
use crate::connectors::metadata::PollingMetadata;
use crate::connectors::offset::{Offset, OffsetKey, OffsetValue};
use crate::connectors::{DataEventType, ReaderContext};
use crate::persistence::frontier::OffsetAntichain;

/// Factor by which the fetch limit grows when a full page makes no forward
/// progress (a single timestamp, or an overlap window, larger than the limit).
const LIMIT_GROWTH_FACTOR: usize = 4;

/// A single row produced by a [`PollingDataSource`].
#[derive(Clone, Debug)]
pub struct PolledRow {
    /// The `id_column` value, canonicalized to a string so it can be used for
    /// deduplication and stored in the persistent offset regardless of the
    /// underlying scalar type.
    pub id: String,
    /// The `timestamp_column` value, interpreted as a monotonically advancing
    /// integer (e.g. epoch milliseconds).
    pub timestamp: i64,
    /// The raw payload of the row, handed to the data-format parser as-is.
    pub raw: Vec<u8>,
}

/// A cheap fingerprint of the source's live edge, used both to compute the
/// settled cutoff and to detect when the overlap window has not changed since the
/// last poll (so the re-read can be skipped entirely).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveState {
    /// The largest `timestamp_column` value present in the source right now.
    pub max_timestamp: i64,
    /// The number of rows in the overlap window, i.e. with
    /// `timestamp_column >= max_timestamp - max_transaction_duration`.
    ///
    /// Together with `max_timestamp` this is a faithful change detector for the
    /// re-read region: on an append-only source, with `max_timestamp` fixed the
    /// count is monotonic, so any new row inside the window strictly increases it;
    /// and when `max_timestamp` moves, that field flags the change. A row landing
    /// strictly inside the window but below the maximum — the very case
    /// `max_transaction_duration` exists for — is therefore still detected, which
    /// counting only rows *at* the maximum would miss.
    pub overlap_count: u64,
}

/// A source that can be polled for rows at or after a watermark.
///
/// The trait is deliberately tiny: everything about overlap reconciliation,
/// deduplication, pagination, persistence, and the [`Reader`] contract lives in
/// [`PollingReader`]. A concrete source only supplies a bounded page of data and
/// a fingerprint of its live edge.
pub trait PollingDataSource: Send {
    /// Fetch at most `limit` rows whose `timestamp_column` is `>= min_watermark`,
    /// ordered by `(timestamp_column, id_column)` ascending.
    ///
    /// Returning fewer than `limit` rows is the signal that the source holds no
    /// more rows at or after `min_watermark` right now — i.e. the reader has
    /// drained the backlog up to the live edge. Returning exactly `limit` rows
    /// means there may be more, and the reader will page on.
    fn fetch(&mut self, min_watermark: i64, limit: usize) -> Result<Vec<PolledRow>, ReadError>;

    /// The current [`LiveState`] fingerprint — the largest `timestamp_column` and
    /// the size of the overlap window `[max - max_transaction_duration, max]` —
    /// or `None` if the source is currently empty.
    ///
    /// This is queried independently of [`fetch`](Self::fetch) so the settled
    /// cutoff can be computed from the true live maximum rather than from a
    /// bounded (and possibly truncated) page, and so an unchanged window can be
    /// detected without re-reading it.
    fn live_state(&mut self, max_transaction_duration: i64)
        -> Result<Option<LiveState>, ReadError>;

    /// The storage type reported to the engine for frontier merging.
    fn storage_type(&self) -> StorageType;

    /// The offset key under which this reader's state is persisted.
    fn offset_key(&self) -> OffsetKey;

    fn short_description(&self) -> Cow<'static, str>;
}

/// A [`Reader`] that turns any [`PollingDataSource`] into an incrementally
/// updating Pathway input.
pub struct PollingReader<Source: PollingDataSource> {
    source: Source,
    mode: ConnectorMode,
    max_transaction_duration: i64,
    base_limit: usize,
    poll_interval: Duration,

    watermark: i64,
    /// Every delivered row still inside the overlap window, as `id -> timestamp`.
    /// Maintained *incrementally* — ids are inserted as they are delivered and
    /// removed only once their timestamp falls below the watermark (they settle).
    /// It is never rebuilt from a single fetch, so a fetch that returns only part
    /// of the window cannot drop an already-delivered id and cause a re-emission.
    pending: HashMap<String, i64>,
    entries_read: u64,
    poll_sequence: u64,

    /// The [`LiveState`] observed the last time a poll drained the source up to
    /// the live edge. While the source reports the same fingerprint, the overlap
    /// window is byte-for-byte unchanged, so a re-read would only re-deliver
    /// already-seen rows — the reader skips it. Reset to `None` after a restart so
    /// the first poll always re-establishes the window.
    last_settled_state: Option<LiveState>,

    /// Rows ready to be served by `read()`, in reverse order so `Vec::pop`
    /// returns them in delivery order.
    buffer: Vec<ReadResult>,
    /// In static mode, the reader finishes once a poll drains the source.
    finished: bool,
}

/// The watermark before any row has been read. `i64::MIN` makes the first
/// `timestamp_column >= watermark` query match the entire source.
const INITIAL_WATERMARK: i64 = i64::MIN;

impl<Source: PollingDataSource> PollingReader<Source> {
    pub fn new(
        source: Source,
        mode: ConnectorMode,
        max_transaction_duration: i64,
        base_limit: usize,
        poll_interval: Duration,
    ) -> Self {
        Self {
            source,
            mode,
            max_transaction_duration,
            base_limit: base_limit.max(1),
            poll_interval,
            watermark: INITIAL_WATERMARK,
            pending: HashMap::new(),
            entries_read: 0,
            poll_sequence: 0,
            last_settled_state: None,
            buffer: Vec::new(),
            finished: false,
        }
    }

    fn current_offset(&self) -> Offset {
        let mut pending: Vec<(String, i64)> = self
            .pending
            .iter()
            .map(|(id, timestamp)| (id.clone(), *timestamp))
            .collect();
        pending.sort_unstable();
        (
            self.source.offset_key(),
            OffsetValue::PollingWatermark {
                watermark: self.watermark,
                entries_read: self.entries_read,
                pending,
            },
        )
    }

    /// Wrap a batch of delivered rows in a `NewSource … FinishedSource` block,
    /// stamped with the post-batch offset and ordered so that `Vec::pop` serves
    /// them in sequence. Commits are forbidden inside the block (the metadata
    /// reports `commits_allowed_in_between() == false`), so the whole page lands
    /// in a single minibatch and the persisted offset always matches exactly the
    /// rows that were committed.
    fn build_block(&mut self, rows: Vec<PolledRow>) -> Vec<ReadResult> {
        self.poll_sequence += 1;
        let offset = self.current_offset();
        let mut results = Vec::with_capacity(rows.len() + 2);
        results.push(ReadResult::NewSource(
            PollingMetadata::new(self.poll_sequence).into(),
        ));
        for row in rows {
            results.push(ReadResult::Data(
                ReaderContext::from_raw_bytes(DataEventType::Insert, row.raw),
                offset.clone(),
            ));
        }
        results.push(ReadResult::FinishedSource {
            commit_possibility: CommitPossibility::Possible,
        });
        results.reverse();
        results
    }

    /// Run one poll cycle: fetch a bounded page (growing the limit if a full page
    /// cannot make progress), reconcile the overlap, advance the watermark, and
    /// stage the freshly delivered rows into `self.buffer`.
    ///
    /// Returns `true` when the source is drained up to the live edge (the caller
    /// should then sleep or finish) and `false` when more pages are immediately
    /// available (the caller should keep paging without sleeping).
    fn poll(&mut self) -> Result<bool, ReadError> {
        // In streaming mode, fingerprint the live edge first. If the overlap
        // window is identical to the one observed the last time we fully drained,
        // nothing new can be in it, so skip the re-read entirely — the
        // steady-state fast path. `None` means the source is empty. In static mode
        // there is no live edge: everything is settled (`safe_ts = i64::MAX`).
        let live_state = if self.mode.is_polling_enabled() {
            match self.source.live_state(self.max_transaction_duration)? {
                None => return Ok(true), // empty source — caught up
                Some(state) if self.last_settled_state == Some(state) => {
                    return Ok(true); // window unchanged since last drain — caught up
                }
                Some(state) => Some(state),
            }
        } else {
            None
        };
        let safe_ts = match live_state {
            Some(state) => state
                .max_timestamp
                .saturating_sub(self.max_transaction_duration),
            None => i64::MAX,
        };

        let mut limit = self.base_limit;
        loop {
            let rows = self.source.fetch(self.watermark, limit)?;
            let exhausted = rows.len() < limit;

            // The boundary below which rows are known complete. A full page may
            // have truncated its largest timestamp bucket (more rows could share
            // that timestamp beyond the limit), so that bucket is deferred and
            // re-read on the next page; a short page is complete to the end.
            let page_boundary = if exhausted {
                i64::MAX
            } else {
                rows.last().map_or(i64::MAX, |row| row.timestamp)
            };

            let new_watermark = self.watermark.max(safe_ts.min(page_boundary));
            let advanced = new_watermark > self.watermark;

            // Would this page deliver anything new (a row below the deferred
            // boundary whose id we have not delivered yet)? Checked without
            // mutating `pending`, so growing and refetching cannot double-count.
            let would_emit = rows
                .iter()
                .any(|row| row.timestamp < page_boundary && !self.pending.contains_key(&row.id));

            if !exhausted && !would_emit && !advanced {
                // A full page that delivers nothing new and cannot advance the
                // watermark: a single hot timestamp at the watermark, or an
                // already-delivered overlap window wider than the limit. The only
                // cursor we have is the limit, so widen it and refetch.
                limit = limit.saturating_mul(LIMIT_GROWTH_FACTOR);
                continue;
            }

            // Deliver every row below the deferred boundary not already delivered,
            // recording its timestamp in `pending`. We do not rebuild `pending`
            // from `rows`; we only *add* here and *evict* below. A larger refetch
            // (after growth) is a superset of the smaller ones, so processing the
            // final page is enough and never re-emits a row.
            let mut emitted = Vec::new();
            for row in rows {
                if row.timestamp >= page_boundary {
                    continue; // deferred bucket of a full page — re-read next page
                }
                if !self.pending.contains_key(&row.id) {
                    self.pending.insert(row.id.clone(), row.timestamp);
                    emitted.push(row);
                }
            }

            self.watermark = new_watermark;
            // Evict everything that has settled below the new watermark — those ids
            // will never be re-fetched, so they no longer need deduplication.
            self.pending
                .retain(|_, timestamp| *timestamp >= new_watermark);
            if !emitted.is_empty() {
                self.entries_read += emitted.len() as u64;
                self.buffer = self.build_block(emitted);
            }
            if exhausted {
                // Drained to the live edge: remember the window fingerprint so the
                // next poll can skip the re-read while it stays unchanged. Recording
                // the fingerprint observed at the *start* of this poll is the safe
                // choice — if the window changed mid-poll, the next poll sees a
                // different fingerprint and re-reads rather than skipping.
                self.last_settled_state = live_state;
            }
            return Ok(exhausted);
        }
    }
}

impl<Source: PollingDataSource> Reader for PollingReader<Source> {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(result) = self.buffer.pop() {
                return Ok(result);
            }
            if self.finished {
                return Ok(ReadResult::Finished);
            }

            let drained = self.poll()?;

            // If the buffer is still empty but the source is not drained, more
            // pages are available right now — loop and page again without
            // sleeping. Only an empty-and-drained poll waits or finishes.
            if self.buffer.is_empty() && drained {
                if self.mode.is_polling_enabled() {
                    sleep(self.poll_interval);
                } else {
                    // Static mode: nothing left to read, so we are done.
                    self.finished = true;
                    return Ok(ReadResult::Finished);
                }
            }
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        if let Some(OffsetValue::PollingWatermark {
            watermark,
            entries_read,
            pending,
        }) = frontier.get_offset(&self.source.offset_key())
        {
            self.watermark = *watermark;
            self.entries_read = *entries_read;
            self.pending = pending.iter().cloned().collect();
        }
        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        self.source.short_description()
    }

    fn storage_type(&self) -> StorageType {
        self.source.storage_type()
    }
}
