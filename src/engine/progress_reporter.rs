use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

use arc_swap::ArcSwapOption;
use pyo3::{PyObject, Python};

use crate::python_api::threads::PythonThreadState;

use super::{Graph, ProberStats};

const PROGRESS_REPORTING_PERIOD: Duration = Duration::from_millis(200);

pub struct Runner {
    should_finish: Arc<AtomicBool>,
    reporting_thread_handle: Option<JoinHandle<()>>,
}

impl Runner {
    fn run(
        printing_period: Duration,
        stats: &Arc<ArcSwapOption<ProberStats>>,
        stats_monitor: PyObject,
    ) -> Runner {
        let should_finish = Arc::new(AtomicBool::new(false));
        let thread_handle = {
            let should_finish = Arc::clone(&should_finish);
            let stats = Arc::clone(stats);
            thread::Builder::new()
                .name("pathway:progress_reporting".to_owned())
                .spawn(move || {
                    let thread_state = PythonThreadState::new();

                    while !should_finish.load(Ordering::Relaxed) {
                        if let Some(ref stats) = *stats.load() {
                            let now = SystemTime::now();
                            let duration = u64::try_from(
                                now.duration_since(SystemTime::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis(),
                            )
                            .unwrap();
                            Python::with_gil(|py| {
                                stats_monitor
                                    .call_method1(
                                        py,
                                        "update_monitoring",
                                        ((**stats).clone(), duration),
                                    )
                                    .unwrap();
                            });
                        }

                        thread::park_timeout(printing_period);
                    }

                    drop(thread_state);
                })
                .expect("progress reporting thread creation failed")
        };
        Runner {
            should_finish,
            reporting_thread_handle: Some(thread_handle),
        }
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.should_finish.store(true, Ordering::Relaxed);
        let reporting_thread_handle = self.reporting_thread_handle.take().unwrap();
        reporting_thread_handle.thread().unpark();
        reporting_thread_handle
            .join()
            .expect("progress reporting thread failed");
    }
}

pub fn maybe_run_reporter(
    monitoring_level: &MonitoringLevel,
    graph: &dyn Graph,
    stats_monitor: Option<PyObject>,
) -> Option<Runner> {
    if *monitoring_level != MonitoringLevel::None && graph.worker_index() == 0 {
        let stats_shared = Arc::new(ArcSwapOption::from(None));
        let progress_reporter_runner = Runner::run(
            PROGRESS_REPORTING_PERIOD,
            &stats_shared,
            stats_monitor.expect("when monitoring is turned on, stats_monitor has to be defined"),
        );

        graph
            .attach_prober(
                Box::new(move |prober_stats| stats_shared.store(Some(Arc::new(prober_stats)))),
                *monitoring_level == MonitoringLevel::All,
                true,
            )
            .expect("Failed to start progress reporter");

        Some(progress_reporter_runner)
    } else {
        None
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum MonitoringLevel {
    None,
    InOut,
    All,
}
