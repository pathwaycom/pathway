// Copyright © 2026 Pathway

//! The ack frontier handed to a `DeferredAckWorker` after a checkpoint: it
//! must cover exactly what recovery is guaranteed to replay. Recovery replays
//! the snapshot up to the first `AdvanceTime` whose time is not smaller than
//! the finalized threshold and seeks to that event's frontier, so that event's
//! frontier is the exact answer; when it isn't flushed yet, the answer falls
//! back to the last flushed one (conservative: redeliveries, never losses).

use tempfile::tempdir;

use pathway_engine::connectors::{OffsetKey, OffsetValue};
use pathway_engine::engine::{Timestamp, TotalFrontier};
use pathway_engine::persistence::backends::FilesystemKVStorage;
use pathway_engine::persistence::frontier::OffsetAntichain;
use pathway_engine::persistence::input_snapshot::{
    Event as SnapshotEvent, InputSnapshotWriter, SnapshotMode,
};

fn frontier_with_entries_count(entries_read: usize) -> OffsetAntichain {
    let mut frontier = OffsetAntichain::new();
    frontier.advance_offset(
        OffsetKey::Empty,
        OffsetValue::MqttReadEntriesCount(entries_read),
    );
    frontier
}

fn entries_count(frontier: &OffsetAntichain) -> usize {
    match frontier.get_offset(&OffsetKey::Empty) {
        Some(OffsetValue::MqttReadEntriesCount(entries_read)) => *entries_read,
        other => panic!("unexpected offset in the ack frontier: {other:?}"),
    }
}

fn create_snapshot_writer(path: &std::path::Path) -> InputSnapshotWriter {
    let backend = FilesystemKVStorage::new(path).expect("Failed to create FS backend");
    let mut writer = InputSnapshotWriter::new(Box::new(backend), SnapshotMode::Full)
        .expect("Failed to create snapshot writer");
    writer.enable_ack_frontier_tracking();
    writer
}

fn write_time_advancement(writer: &mut InputSnapshotWriter, time: u64, entries_read: usize) {
    writer.write(&SnapshotEvent::AdvanceTime(
        Timestamp(time),
        frontier_with_entries_count(entries_read),
    ));
}

#[test]
fn test_ack_frontier_empty_without_time_advancements() {
    let test_storage = tempdir().expect("Tempdir creation failed");
    let mut writer = create_snapshot_writer(test_storage.path());

    assert!(writer
        .take_ack_frontier(TotalFrontier::At(Timestamp(10)))
        .is_none());

    // A buffered but not yet flushed time advancement must not be
    // acknowledged: its durability is not ensured until the flush.
    write_time_advancement(&mut writer, 2, 100);
    assert!(writer
        .take_ack_frontier(TotalFrontier::At(Timestamp(10)))
        .is_none());
}

#[test]
fn test_ack_frontier_uses_the_replay_terminator() {
    let test_storage = tempdir().expect("Tempdir creation failed");
    let mut writer = create_snapshot_writer(test_storage.path());

    write_time_advancement(&mut writer, 2, 100);
    write_time_advancement(&mut writer, 4, 200);
    write_time_advancement(&mut writer, 6, 300);
    let _flush_futures = writer.flush();

    // Recovery with threshold 4 stops at the AdvanceTime with time 4 and
    // seeks to its frontier, so exactly that frontier is acknowledged.
    let frontier = writer
        .take_ack_frontier(TotalFrontier::At(Timestamp(4)))
        .expect("ack frontier must be present");
    assert_eq!(entries_count(&frontier), 200);

    // The next checkpoint moves the threshold forward; the earlier entries
    // were pruned, and the answer moves to the new terminator.
    let frontier = writer
        .take_ack_frontier(TotalFrontier::At(Timestamp(5)))
        .expect("ack frontier must be present");
    assert_eq!(entries_count(&frontier), 300);
}

#[test]
fn test_ack_frontier_falls_back_to_the_last_flushed_entry() {
    let test_storage = tempdir().expect("Tempdir creation failed");
    let mut writer = create_snapshot_writer(test_storage.path());

    write_time_advancement(&mut writer, 2, 100);
    write_time_advancement(&mut writer, 4, 200);
    let _flush_futures = writer.flush();

    // No flushed AdvanceTime crosses the threshold yet: acknowledge up to the
    // last flushed one. Everything it covers is replayed on recovery.
    let frontier = writer
        .take_ack_frontier(TotalFrontier::At(Timestamp(10)))
        .expect("ack frontier must be present");
    assert_eq!(entries_count(&frontier), 200);

    // The fallback entry is retained: a repeated call must stay at it rather
    // than lose the ack position.
    let frontier = writer
        .take_ack_frontier(TotalFrontier::At(Timestamp(10)))
        .expect("ack frontier must be present");
    assert_eq!(entries_count(&frontier), 200);
}

#[test]
fn test_ack_frontiers_not_recorded_without_tracking() {
    // Sources without a deferred ack worker must not accumulate the frontier
    // record: nothing would ever prune it.
    let test_storage = tempdir().expect("Tempdir creation failed");
    let backend =
        FilesystemKVStorage::new(test_storage.path()).expect("Failed to create FS backend");
    let mut writer = InputSnapshotWriter::new(Box::new(backend), SnapshotMode::Full)
        .expect("Failed to create snapshot writer");

    write_time_advancement(&mut writer, 2, 100);
    let _flush_futures = writer.flush();
    assert!(writer
        .take_ack_frontier(TotalFrontier::At(Timestamp(10)))
        .is_none());
}

#[test]
fn test_ack_frontier_releases_everything_when_the_source_is_done() {
    let test_storage = tempdir().expect("Tempdir creation failed");
    let mut writer = create_snapshot_writer(test_storage.path());

    write_time_advancement(&mut writer, 2, 100);
    write_time_advancement(&mut writer, 4, 200);
    let _flush_futures = writer.flush();

    let frontier = writer
        .take_ack_frontier(TotalFrontier::Done)
        .expect("ack frontier must be present");
    assert_eq!(entries_count(&frontier), 200);
}
