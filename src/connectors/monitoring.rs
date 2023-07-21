use std::collections::VecDeque;
use std::time::{Duration, Instant};

use log::{info, warn};
use pyo3::pyclass;

#[derive(Debug, Clone, Copy)]
#[pyclass]
pub struct ConnectorStats {
    #[pyo3(get, set)]
    pub num_messages_from_start: usize,
    #[pyo3(get, set)]
    pub num_messages_in_last_minute: usize,
    #[pyo3(get, set)]
    pub num_messages_recently_committed: usize,
    #[pyo3(get, set)]
    pub finished: bool,
}

struct ConnectorLogger {
    name: String,
    previously_reported_messages: usize,
    reported_messages: usize,
    reported_minibatches: usize,
    last_reported_timestamp: Option<Instant>,
}

const MIN_TIME_ADVANCED_REPORTS_FREQUENCY: Duration = Duration::from_secs(5);
impl ConnectorLogger {
    fn new(name: String) -> Self {
        Self {
            name,
            previously_reported_messages: 0,
            reported_messages: 0,
            reported_minibatches: 0,
            last_reported_timestamp: None,
        }
    }

    fn report_stats(&mut self, current_timestamp: Instant) {
        info!(
            "{}: {} entries ({} minibatch(es)) have been sent to the engine",
            self.name, self.reported_messages, self.reported_minibatches
        );
        self.last_reported_timestamp = Some(current_timestamp);
        self.previously_reported_messages = self.reported_messages;
        self.reported_messages = 0;
        self.reported_minibatches = 0;
    }

    fn on_commit(&mut self, current_timestamp: Instant, n_minibatch_messages: usize) {
        self.reported_messages += n_minibatch_messages;
        self.reported_minibatches += 1;

        if let Some(last_reported_timestamp) = self.last_reported_timestamp {
            let time_elapsed = current_timestamp.checked_duration_since(last_reported_timestamp);
            if let Some(time_elapsed) = time_elapsed {
                if time_elapsed >= MIN_TIME_ADVANCED_REPORTS_FREQUENCY
                    && (self.previously_reported_messages > 0 || self.reported_messages > 0)
                {
                    self.report_stats(current_timestamp);
                }
            } else {
                warn!(
                    "{}: Time went backwards, unable to log reader stats",
                    self.name
                );
            }
        } else {
            self.report_stats(current_timestamp);
        }
    }

    fn on_finished(&mut self) {
        // Closing the data source implicitly advances time
        let current_timestamp = Instant::now();
        if self.reported_minibatches > 0 {
            self.report_stats(current_timestamp);
        }
        warn!("{}: Closing the data source", self.name);
    }
}

pub struct ConnectorMonitor {
    name: String,
    stats: ConnectorStats,
    last_minute_queue: VecDeque<(usize, Instant)>,
    current_num_messages: usize,
    logger: ConnectorLogger,
}

impl ConnectorMonitor {
    pub fn new(name: String) -> Self {
        ConnectorMonitor {
            name: name.clone(),
            stats: ConnectorStats {
                num_messages_from_start: 0,
                num_messages_in_last_minute: 0,
                num_messages_recently_committed: 0,
                finished: false,
            },
            last_minute_queue: VecDeque::new(),
            current_num_messages: 0,
            logger: ConnectorLogger::new(name),
        }
    }

    pub fn increment(&mut self) {
        self.current_num_messages += 1;
    }

    pub fn finish(&mut self) {
        self.stats.finished = true;
        self.logger
            .on_commit(Instant::now(), self.current_num_messages);
        self.logger.on_finished();
    }

    pub fn commit(&mut self) {
        self.stats.num_messages_recently_committed = self.current_num_messages;
        let now = Instant::now();
        while let Some(elem) = self.last_minute_queue.front() {
            if now.duration_since(elem.1) < Duration::from_secs(60) {
                break;
            }
            self.stats.num_messages_in_last_minute -= elem.0;
            self.last_minute_queue.pop_front();
        }
        self.stats.num_messages_in_last_minute += self.current_num_messages;
        self.last_minute_queue
            .push_back((self.current_num_messages, now));
        self.stats.num_messages_from_start += self.current_num_messages;
        self.logger.on_commit(now, self.current_num_messages);
        self.current_num_messages = 0;
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_stats(&self) -> ConnectorStats {
        self.stats
    }
}
