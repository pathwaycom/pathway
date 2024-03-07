// Copyright © 2024 Pathway

use crate::env::{parse_env_var, parse_env_var_required, Error as EnvError};
use log::warn;
use timely::{CommunicationConfig, Config as TimelyConfig, WorkerConfig};

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
}

impl Config {
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

    pub fn process_id(&self) -> usize {
        self.process_id
    }

    pub fn to_timely_config(&self) -> TimelyConfig {
        match &self.processes {
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
        }
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
        let (process_id, processes) = if processes > 1 {
            let process_id: usize = parse_env_var_required("PATHWAY_PROCESS_ID")?;
            if process_id >= processes {
                return Err(Error::InvalidId(process_id));
            }
            let first_port: usize = parse_env_var_required("PATHWAY_FIRST_PORT")?;
            let addresses = (0..processes)
                .map(|id| format!("127.0.0.1:{}", first_port + id))
                .collect();
            (process_id, Processes::Multi(addresses))
        } else {
            (0, Processes::Single)
        };
        Ok(Self {
            workers,
            threads,
            processes,
            process_id,
        })
    }
}
