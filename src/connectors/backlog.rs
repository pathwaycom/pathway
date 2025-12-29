use std::collections::VecDeque;

use timely::dataflow::ProbeHandle;

use crate::engine::Timestamp;

#[derive(Debug)]
pub struct BacklogEntry {
    timestamp: Timestamp,
    n_events: usize,
}

impl BacklogEntry {
    pub fn new(timestamp: Timestamp) -> Self {
        Self {
            timestamp,
            n_events: 1,
        }
    }

    pub fn on_event(&mut self) {
        self.n_events += 1;
    }
}

#[derive(Debug, Default)]
pub struct BacklogTracker {
    backlog: VecDeque<BacklogEntry>,
    total_in_fly: usize,
}

impl BacklogTracker {
    pub fn new() -> Self {
        Self {
            backlog: VecDeque::new(),
            total_in_fly: 0,
        }
    }

    pub fn on_event(&mut self, timestamp: Timestamp) {
        self.total_in_fly += 1;
        match self.backlog.back_mut() {
            Some(entry) if entry.timestamp == timestamp => entry.on_event(),
            _ => self.backlog.push_back(BacklogEntry::new(timestamp)),
        }
    }

    pub fn backlog_size(&self) -> usize {
        self.total_in_fly
    }

    pub fn last_timestamp_with_data(&self) -> Option<Timestamp> {
        self.backlog.back().map(|entry| entry.timestamp)
    }

    pub fn advance_with_probe(&mut self, output_probe: &ProbeHandle<Timestamp>) {
        while let Some(group) = self.backlog.front() {
            if output_probe.less_equal(&group.timestamp) {
                break;
            }
            self.total_in_fly -= group.n_events;
            self.backlog.pop_front();
        }
    }
}
