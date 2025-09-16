use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    metrics::{
        data::{AggregatedMetrics, Gauge, MetricData, ResourceMetrics},
        exporter::PushMetricExporter,
        Temporality,
    },
};
use rusqlite::{params, types::ToSqlOutput, Connection, ToSql};
use std::{error, path::Path, time::SystemTime};
use std::{sync::Arc, sync::Mutex, time::Duration, time::UNIX_EPOCH};
use thiserror::Error;

use crate::fs_helpers::ensure_directory;
pub const DDL: &str = r"
BEGIN;
CREATE TABLE IF NOT EXISTS Metrics (
  worker_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  operator_id INTEGER,
  name TEXT NOT NULL,
  value NUMERIC NOT NULL,
  PRIMARY KEY (worker_id, timestamp, operator_id, name)
);

CREATE TABLE IF NOT EXISTS Resources (
  run_id    TEXT NOT NULL,
  graph     TEXT NULL,
  PRIMARY KEY (run_id)
);

CREATE VIEW IF NOT EXISTS MetricsAgg AS
SELECT
  m.worker_id,
  m.operator_id,
  m.timestamp,
  MAX(CASE WHEN m.name = 'operator.latency'    THEN m.value END) AS latency_ms,
  MAX(CASE WHEN m.name = 'operator.insertions' THEN m.value END) AS rows_positive,
  MAX(CASE WHEN m.name = 'operator.deletions' THEN m.value END) AS rows_negative
FROM Metrics AS m
GROUP BY m.worker_id, m.operator_id, m.timestamp;

CREATE INDEX IF NOT EXISTS ix_metrics_operator_ts_name
ON Metrics (worker_id, operator_id, timestamp, name);
COMMIT;
";

#[derive(Error, Debug)]
pub enum SqliteExporterError {
    #[error(transparent)]
    DbError(#[from] rusqlite::Error),

    #[error("Failed to create metrics directory: {0}")]
    MetricsDirCreationFailed(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] Box<dyn error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, SqliteExporterError>;

#[derive(Debug, Clone, Copy)]
enum Number {
    I64(i64),
    U64(u64),
    F64(f64),
}

impl From<i64> for Number {
    fn from(x: i64) -> Self {
        Number::I64(x)
    }
}

impl From<u64> for Number {
    fn from(x: u64) -> Self {
        Number::U64(x)
    }
}

impl From<f64> for Number {
    fn from(x: f64) -> Self {
        Number::F64(x)
    }
}

impl ToSql for Number {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match *self {
            Number::I64(i) => ToSqlOutput::from(i),
            Number::U64(u) => ToSqlOutput::from(i64::try_from(u).unwrap_or(i64::MAX)),
            Number::F64(f) => ToSqlOutput::from(f),
        })
    }
}

#[derive(Debug, Clone)]
struct Sample {
    worker_id: usize,
    timestamp: i64,
    operator_id: Option<i64>,
    name: String,
    value: Number,
}

pub struct SqliteExporter {
    worker_id: usize,
    conn: Arc<Mutex<Connection>>,
    buf: Arc<Mutex<Vec<Sample>>>,
}

impl SqliteExporter {
    pub fn new(run_id: &str, path: &str, graph: Option<&String>, worker_id: usize) -> Result<Self> {
        let dir = Path::new(path);
        ensure_directory(dir)?;

        let file_path = dir.join(format!("metrics_{run_id}.db"));
        log::info!(
            "Detailed metrics will be stored in: {}",
            file_path.display()
        );

        let conn = Connection::open(file_path).and_then(|c| {
            if worker_id == 0 {
                c.pragma_update(None, "journal_mode", "WAL")?;
                c.pragma_update(None, "synchronous", "NORMAL")?;
                c.pragma_update(None, "busy_timeout", "5000")?;
                c.pragma_update(None, "temp_store", "MEMORY")?;
                c.pragma_update(None, "journal_size_limit", "6144000")?;
                c.execute_batch(DDL)?;
                c.execute(
                    "INSERT INTO Resources (run_id, graph) VALUES (?, ?)",
                    params![run_id, graph],
                )?;
            }
            Ok(c)
        })?;

        let this = SqliteExporter {
            worker_id,
            conn: Arc::new(Mutex::new(conn)),
            buf: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(this)
    }

    fn push_gauge<T>(&self, buf: &mut Vec<Sample>, metric_name: &str, g: &Gauge<T>)
    where
        T: Into<Number> + Copy,
    {
        let timestamp = system_time_to_epoch_ms(g.time());

        for dp in g.data_points() {
            let operator_id = dp
                .attributes()
                .find(|kv| kv.key == "operator.id".into())
                .and_then(|kv| as_i64(&kv.value));
            let sample = Sample {
                worker_id: self.worker_id,
                timestamp,
                operator_id,
                name: metric_name.to_string(),
                value: dp.value().into(),
            };
            buf.push(sample);
        }
    }

    fn flush_buffer(&self) -> Result<()> {
        let samples: Vec<_> = {
            let mut buf = self.buf.lock().unwrap();
            buf.drain(..).collect()
        };

        if samples.is_empty() {
            return Ok(());
        }

        let conn = self.conn.lock().unwrap();
        let tx = conn.unchecked_transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT INTO Metrics (worker_id, timestamp, operator_id, name, value)
                VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;

            for sample in samples {
                stmt.execute(params![
                    sample.worker_id,
                    sample.timestamp,
                    sample.operator_id,
                    sample.name,
                    sample.value,
                ])?;
            }
        }

        tx.commit().map_err(|err| {
            log::error!("Failed to commit transaction: {err}");
            err
        })?;

        Ok(())
    }
}

impl PushMetricExporter for SqliteExporter {
    fn export(
        &self,
        resource_metrics: &ResourceMetrics,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        {
            let mut buf = self.buf.lock().unwrap();
            for scope_metrics in resource_metrics.scope_metrics() {
                for metric in scope_metrics.metrics() {
                    match metric.data() {
                        AggregatedMetrics::I64(MetricData::Gauge(g)) => {
                            self.push_gauge(&mut buf, metric.name(), g);
                        }
                        AggregatedMetrics::U64(MetricData::Gauge(g)) => {
                            self.push_gauge(&mut buf, metric.name(), g);
                        }
                        AggregatedMetrics::F64(MetricData::Gauge(g)) => {
                            self.push_gauge(&mut buf, metric.name(), g);
                        }
                        _ => {}
                    }
                }
            }
        }

        async {
            self.flush_buffer()
                .map_err(|_| OTelSdkError::InternalFailure("Failed to flush buffer".into()))?;
            Ok(())
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.flush_buffer()
            .map_err(|_| OTelSdkError::InternalFailure("Failed to flush buffer".into()))?;
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        // Return the temporality of the exporter
        Temporality::Cumulative
    }
}

fn as_i64(value: &opentelemetry::Value) -> Option<i64> {
    if let opentelemetry::Value::I64(v) = value {
        Some(*v)
    } else {
        None
    }
}

fn system_time_to_epoch_ms(t: SystemTime) -> i64 {
    let dur = t
        .duration_since(UNIX_EPOCH)
        .expect("timestamp before UNIX_EPOCH is not supported");

    i64::try_from(dur.as_millis()).expect("timestamp does not fit into i64 milliseconds")
}
