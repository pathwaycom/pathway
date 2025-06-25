use std::{cell::RefCell, collections::HashMap, rc::Rc, time::SystemTime};

use once_cell::unsync::Lazy;
use pyo3::pyclass;
use timely::dataflow::ProbeHandle;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::{
    connectors::monitoring::{ConnectorMonitor, ConnectorStats},
    engine::Timestamp,
};

#[derive(Debug, Clone, Copy)]
#[pyclass]
pub struct OperatorStats {
    #[pyo3(get, set)]
    pub time: Option<Timestamp>,
    #[pyo3(get, set)]
    pub lag: Option<u64>,
    #[pyo3(get, set)]
    pub done: bool,
}

impl OperatorStats {
    pub fn latency(&self, now: SystemTime) -> Option<u64> {
        if let Some(time) = self.time {
            let duration = u64::try_from(
                now.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap();
            if duration < time.0 {
                Some(0)
            } else {
                Some(duration - time.0)
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct ProberStats {
    #[pyo3(get, set)]
    pub input_stats: OperatorStats,
    #[pyo3(get, set)]
    pub output_stats: OperatorStats,
    #[pyo3(get, set)]
    pub operators_stats: HashMap<usize, OperatorStats>,
    #[pyo3(get, set)]
    pub connector_stats: Vec<(String, ConnectorStats)>,
    #[pyo3(get)]
    pub row_counts: HashMap<usize, CountStats>,
}

#[derive(Debug, Default, Clone, Copy)]
#[pyclass]
pub struct CountStats {
    #[pyo3(get)]
    total_rows: isize,
    #[pyo3(get)]
    current_rows: isize,
}

impl CountStats {
    pub fn update(&mut self, diff: isize) {
        self.total_rows += 1;
        self.current_rows += diff;
    }

    pub fn get_insertions(&self) -> isize {
        (self.current_rows + self.total_rows) / 2
    }

    pub fn get_deletions(&self) -> isize {
        (self.total_rows - self.current_rows) / 2
    }
}

pub struct OperatorProbe<T: TimelyTimestamp> {
    pub frontier: ProbeHandle<T>,
    pub counter: Rc<RefCell<CountStats>>,
}

impl<T: TimelyTimestamp> Default for OperatorProbe<T> {
    fn default() -> Self {
        Self {
            frontier: ProbeHandle::default(),
            counter: Rc::new(RefCell::new(CountStats::default())),
        }
    }
}

pub struct Prober {
    input_time: Option<Timestamp>,
    input_time_changed: Option<SystemTime>,
    output_time: Option<Timestamp>,
    output_time_changed: Option<SystemTime>,
    intermediate_probes_required: bool,
    run_callback_every_time: bool,
    stats: HashMap<usize, OperatorStats>,
    callback: Box<dyn FnMut(ProberStats)>,
}

impl Prober {
    pub fn new(
        callback: Box<dyn FnMut(ProberStats)>,
        intermediate_probes_required: bool,
        run_callback_every_time: bool,
    ) -> Self {
        Self {
            input_time: None,
            input_time_changed: None,
            output_time: None,
            output_time_changed: None,
            intermediate_probes_required,
            run_callback_every_time,
            stats: HashMap::new(),
            callback,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn create_stats(
        probe: &ProbeHandle<Timestamp>,
        input_time: Option<Timestamp>,
    ) -> OperatorStats {
        let frontier = probe.with_frontier(|frontier| frontier.as_option().copied());
        if let Some(timestamp) = frontier {
            OperatorStats {
                time: if timestamp.0 > 0 {
                    Some(timestamp)
                } else {
                    None
                },
                lag: input_time.map(|input_time_unwrapped| {
                    if input_time_unwrapped >= timestamp {
                        input_time_unwrapped.0 - timestamp.0
                    } else {
                        0
                    }
                }),
                done: false,
            }
        } else {
            OperatorStats {
                time: None,
                lag: None,
                done: true,
            }
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn update(
        &mut self,
        input_probe: &ProbeHandle<Timestamp>,
        output_probe: &ProbeHandle<Timestamp>,
        intermediate_probes: &HashMap<usize, OperatorProbe<Timestamp>>,
        connector_monitors: &[Rc<RefCell<ConnectorMonitor>>],
    ) {
        let now = Lazy::new(SystemTime::now);

        let mut changed = false;

        let new_input_time = input_probe.with_frontier(|frontier| frontier.as_option().copied());
        if new_input_time != self.input_time {
            self.input_time = new_input_time;
            self.input_time_changed = Some(*now);
            changed = true;
        }

        let new_output_time = output_probe.with_frontier(|frontier| frontier.as_option().copied());
        if new_output_time != self.output_time {
            self.output_time = new_output_time;
            self.output_time_changed = Some(*now);
            changed = true;
        }

        if self.intermediate_probes_required {
            for (id, probe) in intermediate_probes {
                let new_time = probe
                    .frontier
                    .with_frontier(|frontier| frontier.as_option().copied());
                let stat = self.stats.get(id);
                if let Some(stat) = stat {
                    if new_time != stat.time {
                        changed = true;
                    }
                } else {
                    changed = true;
                }
            }
        }

        let connector_stats: Vec<(String, ConnectorStats)> = connector_monitors
            .iter()
            .map(|connector_monitor| {
                let monitor = (**connector_monitor).borrow();
                (monitor.get_name(), monitor.get_stats())
            })
            .collect();

        if changed || self.run_callback_every_time {
            let mut row_counts: HashMap<usize, CountStats> = HashMap::new();
            if self.intermediate_probes_required {
                for (id, probe) in intermediate_probes {
                    self.stats
                        .insert(*id, Self::create_stats(&probe.frontier, self.input_time));
                    row_counts.insert(*id, *probe.counter.borrow());
                }
            }

            let prober_stats = ProberStats {
                input_stats: Self::create_stats(input_probe, self.input_time),
                output_stats: Self::create_stats(output_probe, self.input_time),
                operators_stats: self.stats.clone(),
                connector_stats,
                row_counts,
            };

            (self.callback)(prober_stats);
        }
    }
}
