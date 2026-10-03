// Copyright © 2026 Pathway

use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use pathway_engine::connectors::data_format::{IdentityParser, KeyGenerationPolicy};
use pathway_engine::connectors::data_storage::{
    DataEventType, ReadError, ReadResult, Reader, ReaderContext, StorageType,
};
use pathway_engine::connectors::wakeup;
use pathway_engine::connectors::{Connector, Entry, OffsetKey, OffsetValue, SessionType};
use pathway_engine::persistence::frontier::OffsetAntichain;

use crate::helpers::PanicErrorReporter;

/// A reader over a fixed list of messages that, like a Kafka reader, knows how
/// many of them are already fetched and waiting.
struct BufferedReader {
    messages: Vec<Vec<u8>>,
    next: usize,
    /// How many messages ahead of `next` count as already fetched.
    fetched_ahead: usize,
}

impl Reader for BufferedReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if self.next == self.messages.len() {
            return Ok(ReadResult::Finished);
        }
        let payload = self.messages[self.next].clone();
        let offset = (
            OffsetKey::Empty,
            OffsetValue::KafkaOffset(i64::try_from(self.next).unwrap()),
        );
        self.next += 1;
        Ok(ReadResult::Data(
            ReaderContext::from_raw_bytes(DataEventType::Insert, payload),
            offset,
        ))
    }

    fn has_buffered_data(&mut self) -> bool {
        self.next < self.messages.len() && !self.next.is_multiple_of(self.fetched_ahead)
    }

    fn seek(&mut self, _frontier: &OffsetAntichain) -> Result<(), ReadError> {
        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        "BufferedReader".into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Kafka
    }
}

/// Reads `n_messages` through the connector loop and returns the channel
/// entries as the engine's thread would see them.
fn read_through_connector(n_messages: usize, fetched_ahead: usize) -> Vec<Entry> {
    let mut reader = BufferedReader {
        messages: (0..n_messages)
            .map(|i| format!("message_{i}").into_bytes())
            .collect(),
        next: 0,
        fetched_ahead,
    };
    let mut parser = IdentityParser::new(
        &["data".to_string()],
        false,
        None,
        KeyGenerationPolicy::PreferMessageKey,
        SessionType::Native,
    );
    let (sender, receiver) = crossbeam_channel::unbounded();
    let entries = Arc::new(Mutex::new(Vec::new()));
    let collector = {
        let entries = entries.clone();
        thread::spawn(move || {
            while let Ok(entry) = receiver.recv() {
                entries.lock().unwrap().push(entry);
            }
        })
    };
    let wakeup = wakeup::for_current_thread();
    let reporter = PanicErrorReporter::default();
    Connector::read_realtime_updates(&mut reader, &mut parser, &sender, &wakeup, &reporter, None);
    drop(sender);
    collector.join().unwrap();
    Arc::try_unwrap(entries).unwrap().into_inner().unwrap()
}

/// Every message's rows and offset, in the order the engine's thread sees them.
fn flatten(entries: &[Entry]) -> Vec<(usize, i64)> {
    let mut rows = Vec::new();
    for entry in entries {
        match entry {
            Entry::RealtimeEntries(events, (_, OffsetValue::KafkaOffset(offset)), _) => {
                rows.push((events.len(), *offset));
            }
            Entry::RealtimeEntriesBatch(gathered) => {
                for (events, (_, offset)) in gathered {
                    let OffsetValue::KafkaOffset(offset) = offset else {
                        panic!("unexpected offset {offset:?}");
                    };
                    rows.push((events.len(), *offset));
                }
            }
            Entry::RealtimeEvent(ReadResult::Finished) => {}
            other => panic!("unexpected entry {other:?}"),
        }
    }
    rows
}

#[test]
fn test_buffered_messages_are_batched_in_reading_order() {
    // 10 messages fetched at a time: the connector gathers each fetched run
    // into one channel message and hands it over before the reader would wait.
    let entries = read_through_connector(25, 10);
    let batches: Vec<usize> = entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::RealtimeEntriesBatch(gathered) => Some(gathered.len()),
            Entry::RealtimeEntries(..) => Some(1),
            _ => None,
        })
        .collect();
    assert_eq!(batches, vec![10, 10, 5]);
    let rows = flatten(&entries);
    assert_eq!(rows.len(), 25);
    assert!(rows.iter().all(|(n_events, _)| *n_events == 1));
    let offsets: Vec<i64> = rows.iter().map(|(_, offset)| *offset).collect();
    assert_eq!(offsets, (0..25).collect::<Vec<_>>());
}

#[test]
fn test_reader_without_buffered_data_sends_every_message_on_its_own() {
    let entries = read_through_connector(7, 1);
    assert!(entries
        .iter()
        .all(|entry| !matches!(entry, Entry::RealtimeEntriesBatch(_))));
    let offsets: Vec<i64> = flatten(&entries).iter().map(|(_, o)| *o).collect();
    assert_eq!(offsets, (0..7).collect::<Vec<_>>());
}

#[test]
fn test_batches_are_capped() {
    // Everything is "already fetched": batches are cut at the size cap.
    let entries = read_through_connector(150, 1000);
    let batches: Vec<usize> = entries
        .iter()
        .filter_map(|entry| match entry {
            Entry::RealtimeEntriesBatch(gathered) => Some(gathered.len()),
            Entry::RealtimeEntries(..) => Some(1),
            _ => None,
        })
        .collect();
    assert_eq!(batches.iter().sum::<usize>(), 150);
    assert!(batches.iter().all(|size| *size <= 64), "{batches:?}");
    assert!(batches.len() < 150 / 32, "{batches:?}");
}

/// A reader that yields one message and then waits until released, like a
/// source that has gone quiet. Its peek for buffered data takes a while, as a
/// real one may (the Kafka reader polls the client), so the message reaches
/// the channel well after it was read.
struct QuietReader {
    sent: bool,
    release: crossbeam_channel::Receiver<()>,
}

impl Reader for QuietReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if !self.sent {
            self.sent = true;
            return Ok(ReadResult::Data(
                ReaderContext::from_raw_bytes(DataEventType::Insert, b"lone message".to_vec()),
                (OffsetKey::Empty, OffsetValue::KafkaOffset(0)),
            ));
        }
        let _ = self.release.recv();
        Ok(ReadResult::Finished)
    }

    fn has_buffered_data(&mut self) -> bool {
        thread::sleep(Duration::from_millis(50));
        false
    }

    fn seek(&mut self, _frontier: &OffsetAntichain) -> Result<(), ReadError> {
        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        "QuietReader".into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Kafka
    }
}

#[test]
fn test_engine_thread_is_woken_once_the_rows_are_in_the_channel() {
    // The engine's thread (this one) must not have to wait for its timer to
    // see a message a quiet source produced: after the notification that
    // follows the hand-over, the rows are already in the channel. This thread
    // runs the worker's side of the wake-up protocol: it polls the channel
    // and announces a long park when it finds nothing new.
    let (sender, receiver) = crossbeam_channel::unbounded();
    let (release, released) = crossbeam_channel::bounded::<()>(0);
    let wakeup = wakeup::install_for_current_thread();
    let connector_wakeup = wakeup.clone();
    let connector = thread::spawn(move || {
        let wakeup = connector_wakeup;
        let mut reader = QuietReader {
            sent: false,
            release: released,
        };
        let mut parser = IdentityParser::new(
            &["data".to_string()],
            false,
            None,
            KeyGenerationPolicy::PreferMessageKey,
            SessionType::Native,
        );
        let reporter = PanicErrorReporter::default();
        Connector::read_realtime_updates(
            &mut reader,
            &mut parser,
            &sender,
            &wakeup,
            &reporter,
            None,
        );
    });
    let started = Instant::now();
    let mut entry = None;
    while entry.is_none() && started.elapsed() < Duration::from_secs(5) {
        let seen = wakeup.sent();
        entry = receiver.try_recv().ok();
        if entry.is_none() && wakeup.enter_parked(seen) {
            thread::park_timeout(Duration::from_secs(5));
            wakeup.leave_parked();
        }
    }
    assert!(
        matches!(entry, Some(Entry::RealtimeEntries(..))),
        "the message never reached the channel: {entry:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the engine's thread was not woken after the hand-over and waited out its timer ({:?})",
        started.elapsed()
    );
    release.send(()).unwrap();
    connector.join().unwrap();
}
