// Copyright © 2026 Pathway

//! The posix-like reader against an object that changes between the scan
//! that tags it and the read of its contents: a local file is created empty
//! and written afterwards, and the two may straddle a scan. The reader must
//! store the contents under the metadata they belong to, so that the next
//! scan does not "detect" a modification and retract rows that never changed.
//! A `WritingScanner` wraps the real filesystem scanner and appends to the
//! file right before each read of it, which is the race made deterministic.

use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use pathway_engine::connectors::data_storage::scanner::{
    FilesystemScanner, PosixLikeScanner, QueuedAction,
};
use pathway_engine::connectors::data_storage::sharding::ShardSelector;
use pathway_engine::connectors::data_storage::{
    ConnectorMode, DataEventType, ReadError, ReadMethod, ReadResult, Reader, ReaderContext,
};
use pathway_engine::connectors::data_tokenize::BufReaderTokenizer;
use pathway_engine::connectors::metadata::{FileLikeMetadata, SourceMetadata};
use pathway_engine::connectors::posix_like::PosixLikeReader;
use pathway_engine::persistence::cached_object_storage::CachedObjectStorage;

struct WritingScanner {
    inner: FilesystemScanner,
    /// What to append to the object right before each of its reads, in order;
    /// once exhausted, the reads see the file as it is.
    pending_writes: VecDeque<&'static [u8]>,
    reads: Arc<AtomicUsize>,
}

impl PosixLikeScanner for WritingScanner {
    fn object_metadata(&mut self, path: &[u8]) -> Result<Option<FileLikeMetadata>, ReadError> {
        self.inner.object_metadata(path)
    }

    fn read_object(&mut self, path: &[u8]) -> Result<Vec<u8>, ReadError> {
        if let Some(bytes) = self.pending_writes.pop_front() {
            let path = String::from_utf8(path.to_vec()).unwrap();
            let mut file = OpenOptions::new().append(true).open(path).unwrap();
            file.write_all(bytes).unwrap();
            file.sync_all().unwrap();
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_object(path)
    }

    fn next_scanner_actions(
        &mut self,
        are_deletions_enabled: bool,
        cached_object_storage: &CachedObjectStorage,
    ) -> Result<Vec<QueuedAction>, ReadError> {
        self.inner
            .next_scanner_actions(are_deletions_enabled, cached_object_storage)
    }

    fn has_pending_actions(&self) -> bool {
        false
    }

    fn short_description(&self) -> String {
        "WritingScanner".to_string()
    }

    fn can_object_change_during_read(&self) -> bool {
        true
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Source { name: String, size: u64 },
    Line(DataEventType, String),
    SourceDone,
}

/// Drives the reader for `count` events. The reader runs in the streaming
/// mode, so every call returns an event: with nothing new to read it would
/// poll forever, which is why the tests create the next file before asking.
fn read_events(reader: &mut PosixLikeReader, count: usize) -> Vec<Event> {
    (0..count)
        .map(|_| match reader.read().unwrap() {
            ReadResult::NewSource(SourceMetadata::FileLike(metadata)) => Event::Source {
                name: metadata.path.rsplit('/').next().unwrap().to_string(),
                size: metadata.size,
            },
            ReadResult::Data(ReaderContext::RawBytes(event, bytes), _) => {
                Event::Line(event, String::from_utf8(bytes).unwrap().trim().to_string())
            }
            ReadResult::FinishedSource { .. } => Event::SourceDone,
            other => panic!("unexpected read result: {other:?}"),
        })
        .collect()
}

fn line(event: DataEventType, text: &str) -> Event {
    Event::Line(event, text.to_string())
}

fn reader_over(
    dir: &tempfile::TempDir,
    pending_writes: Vec<&'static [u8]>,
) -> (PosixLikeReader, Arc<AtomicUsize>) {
    let reads = Arc::new(AtomicUsize::new(0));
    let scanner = WritingScanner {
        inner: FilesystemScanner::new(dir.path().to_str().unwrap(), "*", ShardSelector::new(0, 1))
            .unwrap(),
        pending_writes: pending_writes.into(),
        reads: reads.clone(),
    };
    let reader = PosixLikeReader::new(
        Box::new(scanner),
        Box::new(BufReaderTokenizer::new(ReadMethod::ByLine)),
        ConnectorMode::Streaming,
        false,
        false,
    )
    .unwrap();
    (reader, reads)
}

#[test]
fn test_file_completed_between_the_scan_and_the_read_is_not_reread() {
    // The scan sees `a` empty; by the time it is read, it holds a line.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a"), b"").unwrap();
    let (mut reader, reads) = reader_over(&dir, vec![b"one\n"]);

    // The contents are stored under the metadata they belong to: `a` with its line.
    let events = read_events(&mut reader, 3);
    assert_eq!(
        events,
        vec![
            Event::Source {
                name: "a".to_string(),
                size: 4
            },
            line(DataEventType::Insert, "one"),
            Event::SourceDone,
        ]
    );
    assert_eq!(
        reads.load(Ordering::SeqCst),
        2,
        "one read under the stale tag, one after the change"
    );

    // The next scan must not find `a` modified: it only picks up the new file.
    std::fs::write(dir.path().join("b"), b"two\n").unwrap();
    let events = read_events(&mut reader, 3);
    assert_eq!(
        events,
        vec![
            Event::Source {
                name: "b".to_string(),
                size: 4
            },
            line(DataEventType::Insert, "two"),
            Event::SourceDone,
        ]
    );
    assert_eq!(reads.load(Ordering::SeqCst), 3);
}

#[test]
fn test_file_still_changing_after_the_attempts_is_reread_by_the_next_scan() {
    // `a` grows on each of its first three reads, so the reader can't see it
    // unchanged across a read. It gives up with the last contents under the
    // metadata taken before them, which makes the next scan reread the file.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a"), b"").unwrap();
    let (mut reader, reads) = reader_over(&dir, vec![b"1\n", b"2\n", b"3\n"]);

    let events = read_events(&mut reader, 5);
    assert_eq!(
        events,
        vec![
            Event::Source {
                name: "a".to_string(),
                size: 4 // the metadata taken before the last attempt: two lines
            },
            line(DataEventType::Insert, "1"),
            line(DataEventType::Insert, "2"),
            line(DataEventType::Insert, "3"),
            Event::SourceDone,
        ]
    );
    assert_eq!(reads.load(Ordering::SeqCst), 3);

    // The next scan sees `a` at 6 bytes against the stored 4: the old
    // contents are retracted and the file is read again, now consistently.
    std::fs::write(dir.path().join("b"), b"x\n").unwrap();
    let events = read_events(&mut reader, 13);
    assert_eq!(
        events,
        vec![
            Event::Source {
                name: "a".to_string(),
                size: 4
            },
            line(DataEventType::Delete, "1"),
            line(DataEventType::Delete, "2"),
            line(DataEventType::Delete, "3"),
            Event::SourceDone,
            Event::Source {
                name: "a".to_string(),
                size: 6
            },
            line(DataEventType::Insert, "1"),
            line(DataEventType::Insert, "2"),
            line(DataEventType::Insert, "3"),
            Event::SourceDone,
            Event::Source {
                name: "b".to_string(),
                size: 2
            },
            line(DataEventType::Insert, "x"),
            Event::SourceDone,
        ]
    );
    assert_eq!(reads.load(Ordering::SeqCst), 5);
}
