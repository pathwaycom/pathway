// Copyright © 2026 Pathway

use log::{info, warn};

use timely::{CommunicationConfig, Config as TimelyConfig, WorkerConfig};

use crate::engine::dataflow::License;
use crate::engine::license::Error as LicenseError;
use crate::env::{parse_env_var, parse_env_var_required, Error as EnvError};

const MAX_WORKERS: usize = if cfg!(feature = "unlimited-workers") {
    usize::MAX
} else {
    8
};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("can't run with no threads")]
    NeedsThreads,

    #[error("can't run with no processes")]
    NeedsProcesses,

    #[error("invalid process ID {0}")]
    InvalidId(usize),

    #[error(transparent)]
    EnvError(#[from] EnvError),
}

#[derive(Clone, Debug)]
enum Processes {
    Single,
    Multi(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct Config {
    workers: usize,
    threads: usize,
    processes: Processes,
    process_id: usize,
    fixed_pool: bool,
}

impl Config {
    /// Force additional compaction work for arrangements created in the pipeline.
    /// By default, differential-dataflow only compacts when explicitly scheduled.
    /// Some pipelines, however, produce arrangements that are never scheduled for
    /// compaction on their own. This can lead to >50% retained memory in backfill
    /// runs at the end of the cold start phase.
    ///
    /// The "effort" is measured in differential-dataflow–specific work units,
    /// where one unit represents a single comparison or element copy in the
    /// merge-sort procedure. Each unit has O(1) cost. We use a logarithmic
    /// number of units (relative to the maximum collection size), as a reasonable
    /// trade-off between limiting overhead during normal processing and ensuring
    /// fast compaction after backfilling.
    ///
    /// Refer to external/differential-dataflow/src/operators/arrange/arrangement.rs:567
    /// to see how the regular compaction is applied.
    const IDLE_MERGE_EFFORT_SETTING: &str = "differential/idle_merge_effort";
    const BASE_IDLE_MERGE_EFFORT: isize = 64;

    pub fn check_scopes(&self, license: &License) -> Result<(), LicenseError> {
        if self.fixed_pool {
            license.check_entitlements(["multiple-machines"])?;
        }
        Ok(())
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn processes(&self) -> usize {
        match &self.processes {
            Processes::Single => 1,
            Processes::Multi(addresses) => addresses.len(),
        }
    }

    pub fn is_downscaling_possible(&self) -> bool {
        if self.fixed_pool {
            warn!("Downscaling is not supported with a fixed worker address pool; use --processes in CLI runner to enable scaling");
            return false;
        }
        self.processes() > 1
    }

    pub fn is_upscaling_possible(&self) -> bool {
        if self.fixed_pool {
            warn!("Upscaling is not supported with a fixed worker address pool; use --processes in CLI runner to enable scaling");
            return false;
        }
        (self.processes() + 1) * self.threads <= MAX_WORKERS
    }

    pub fn process_id(&self) -> usize {
        self.process_id
    }

    pub fn to_timely_config(&self) -> TimelyConfig {
        let mut result = match &self.processes {
            Processes::Single => {
                if self.threads > 1 {
                    TimelyConfig::process(self.threads)
                } else {
                    TimelyConfig::thread()
                }
            }
            Processes::Multi(addresses) => {
                assert!(self.process_id < addresses.len());
                TimelyConfig {
                    communication: CommunicationConfig::Cluster {
                        threads: self.threads,
                        process: self.process_id,
                        addresses: addresses.clone(),
                        report: false,
                        log_fn: Box::new(|_| None),
                    },
                    worker: WorkerConfig::default(),
                }
            }
        };
        result.worker.set(
            Self::IDLE_MERGE_EFFORT_SETTING.to_string(),
            Self::BASE_IDLE_MERGE_EFFORT,
        );
        result
    }

    pub fn from_env() -> Result<Self, Error> {
        let mut threads: usize = parse_env_var("PATHWAY_THREADS")?.unwrap_or(1);
        if threads == 0 {
            return Err(Error::NeedsThreads);
        }
        let mut processes: usize = parse_env_var("PATHWAY_PROCESSES")?.unwrap_or(1);
        if processes == 0 {
            return Err(Error::NeedsProcesses);
        }
        let workers = threads * processes;
        if workers > MAX_WORKERS {
            warn!("{workers} is greater than the the maximum allowed number of workers ({MAX_WORKERS}), reducing");
            threads = MAX_WORKERS / processes;
            if threads == 0 {
                threads = 1;
                processes = MAX_WORKERS;
            }
        }
        let workers = threads * processes;
        assert!(workers <= MAX_WORKERS);
        let (process_id, processes, fixed_pool) = if processes > 1 {
            let process_id: usize = parse_env_var_required("PATHWAY_PROCESS_ID")?;
            if process_id >= processes {
                return Err(Error::InvalidId(process_id));
            }
            let (addresses, fixed_pool) =
                if let Some(addresses) = parse_env_var::<String>("PATHWAY_ADDRESSES")? {
                    let addrs = addresses.split(',').map(str::to_string).collect();
                    info!("Spawning a worker over the set of addresses: ({process_id}, {addrs:?})");
                    (addrs, true)
                } else {
                    let first_port: usize = parse_env_var_required("PATHWAY_FIRST_PORT")?;
                    let addrs = (0..processes)
                        .map(|id| format!("127.0.0.1:{}", first_port + id))
                        .collect();
                    (addrs, false)
                };
            (process_id, Processes::Multi(addresses), fixed_pool)
        } else {
            (0, Processes::Single, false)
        };
        Ok(Self {
            workers,
            threads,
            processes,
            process_id,
            fixed_pool,
        })
    }
}
