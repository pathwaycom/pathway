use std::{
    sync::Arc,
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use super::{error::DynError, license::License, Graph, ProberStats, Result};
use crate::env::parse_env_var;
use arc_swap::ArcSwapOption;
use log::{debug, info};
use nix::sys::{
    resource::{getrusage, UsageWho},
    time::TimeValLike,
};
use opentelemetry::metrics::Unit;
use opentelemetry::{global, Key};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
        MeterProvider, PeriodicReader,
    },
    propagation::TraceContextPropagator,
    runtime,
    trace::{self, TracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::resource::{
    OTEL_SCOPE_NAME, PROCESS_PID, SERVICE_NAME, SERVICE_VERSION,
};
use scopeguard::defer;
use sysinfo::{get_current_pid, System};
use tokio::sync::mpsc;
use uuid::Uuid;

const DEFAULT_TELEMETRY_SERVER: &str = "";

const PERIODIC_READER_INTERVAL: Duration = Duration::from_secs(10);
const OPENTELEMETRY_EXPORT_TIMEOUT: Duration = Duration::from_secs(3);

const PROCESS_MEMORY_USAGE: &str = "process.memory.usage";
const PROCESS_CPU_USAGAE: &str = "process.cpu.usage";
const PROCESS_CPU_USER_TIME: &str = "process.cpu.utime";
const PROCESS_CPU_SYSTEM_TIME: &str = "process.cpu.stime";
const INPUT_LATENCY: &str = "latency.input";
const OUTPUT_LATENCY: &str = "latency.output";

const ROOT_TRACE_ID: Key = Key::from_static_str("root.trace.id");
const RUN_ID: Key = Key::from_static_str("run.id");

struct Telemetry {
    pub config: TelemetryEnabled,
}

impl Telemetry {
    fn new(config: TelemetryEnabled) -> Self {
        Telemetry { config }
    }

    fn resource(&self) -> Resource {
        let pid: i64 = get_current_pid()
            .expect("Failed to get current PID")
            .as_u32()
            .into();

        let root_trace_id = root_trace_id(self.config.trace_parent.as_deref()).unwrap_or_default();

        Resource::new([
            SERVICE_NAME.string(self.config.service_name.clone()),
            SERVICE_VERSION.string(self.config.service_version.clone()),
            OTEL_SCOPE_NAME.string("rust"),
            PROCESS_PID.i64(pid),
            ROOT_TRACE_ID.string(root_trace_id.to_string()),
            RUN_ID.string(self.config.run_id.clone()),
        ])
    }

    fn base_otel_exporter_builder(&self) -> opentelemetry_otlp::TonicExporterBuilder {
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_protocol(Protocol::Grpc)
            .with_endpoint(self.config.telemetry_server_endpoint.clone())
            .with_timeout(OPENTELEMETRY_EXPORT_TIMEOUT)
    }

    fn init_tracer_provider(&self) {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let exporter = self
            .base_otel_exporter_builder()
            .build_span_exporter()
            .expect("exporter initialization should not fail");

        let provider = TracerProvider::builder()
            .with_config(trace::Config::default().with_resource(self.resource()))
            .with_batch_exporter(exporter, runtime::Tokio)
            .build();

        global::set_tracer_provider(provider.clone());
    }

    fn init_meter_provider(&self) {
        let exporter = self
            .base_otel_exporter_builder()
            .build_metrics_exporter(
                Box::new(DefaultAggregationSelector::new()),
                Box::new(DefaultTemporalitySelector::new()),
            )
            .unwrap();

        let reader = PeriodicReader::builder(exporter, runtime::Tokio)
            .with_interval(PERIODIC_READER_INTERVAL)
            .build();

        let meter_provider = MeterProvider::builder()
            .with_resource(self.resource())
            .with_reader(reader)
            .build();

        global::set_meter_provider(meter_provider.clone());
    }

    fn init(&self) {
        self.init_meter_provider();
        self.init_tracer_provider();
    }

    fn teardown() {
        global::shutdown_meter_provider();
        global::shutdown_tracer_provider();
    }
}

fn root_trace_id(trace_parent: Option<&str>) -> Option<&str> {
    if let Some(trace_parent) = trace_parent {
        Some(
            trace_parent
                .split('-')
                .nth(1)
                .expect("trace parent should contain the root trace ID"),
        )
    } else {
        None
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct TelemetryEnabled {
    pub telemetry_server_endpoint: String,
    pub service_name: String,
    pub service_version: String,
    pub run_id: String,
    pub trace_parent: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Config {
    Enabled(TelemetryEnabled),
    Disabled,
}

impl Config {
    pub fn create(
        license: License,
        telemetry_server: Option<String>,
        trace_parent: Option<String>,
    ) -> Result<Self> {
        let telemetry_enabled: bool = parse_env_var("PATHWAY_TELEMETRY_ENABLED")
            .map_err(DynError::from)?
            .unwrap_or(false);
        let run_id: String = parse_env_var("PATHWAY_RUN_ID")
            .map_err(DynError::from)?
            .unwrap_or(Uuid::new_v4().to_string());
        let config = if telemetry_enabled {
            let telemetry_server =
                telemetry_server.unwrap_or(String::from(DEFAULT_TELEMETRY_SERVER));
            match license {
                License::Evaluation | License::DebugNoLimit => Config::Enabled(TelemetryEnabled {
                    telemetry_server_endpoint: telemetry_server,
                    service_name: env!("CARGO_PKG_NAME").to_string(),
                    service_version: env!("CARGO_PKG_VERSION").to_string(),
                    run_id,
                    trace_parent,
                }),
                License::NoLicenseKey => Config::Disabled,
            }
        } else {
            Config::Disabled
        };
        Ok(config)
    }
}

pub struct Runner {
    close_sender: mpsc::Sender<()>,
    telemetry_thread_handle: Option<JoinHandle<()>>,
}

impl Runner {
    fn run(telemetry: Telemetry, stats: Arc<ArcSwapOption<ProberStats>>) -> Runner {
        let (tx, mut rx) = mpsc::channel::<mpsc::Sender<()>>(1);
        let telemetry_thread_handle = start_telemetry_thread(telemetry, tx, stats);
        let close_sender = rx.blocking_recv().expect("expecting return sender");
        Runner {
            close_sender,
            telemetry_thread_handle: Some(telemetry_thread_handle),
        }
    }
}

fn start_telemetry_thread(
    telemetry: Telemetry,
    start_sender: mpsc::Sender<mpsc::Sender<()>>,
    stats: Arc<ArcSwapOption<ProberStats>>,
) -> JoinHandle<()> {
    let handle: JoinHandle<()> = Builder::new()
        .name("pathway:telemetry_thread".to_string())
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_time()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async {
                    let (tx, mut rx) = mpsc::channel::<()>(1);
                    telemetry.init();
                    defer! {
                        Telemetry::teardown();
                    }
                    register_stats_metrics(&stats);
                    register_sys_metrics();
                    start_sender.send(tx).await.expect("should not fail");
                    rx.recv().await;
                });
        })
        .expect("telemetry thread creation failed");
    handle
}

fn register_stats_metrics(stats: &Arc<ArcSwapOption<ProberStats>>) {
    let stats = stats.clone();

    let meter = global::meter("pathway-stats");

    let input_latency_gauge = meter
        .u64_observable_gauge(INPUT_LATENCY)
        .with_unit(Unit::new("ms"))
        .init();

    let output_latency_gauge = meter
        .u64_observable_gauge(OUTPUT_LATENCY)
        .with_unit(Unit::new("ms"))
        .init();

    meter
        .register_callback(
            &[input_latency_gauge.as_any(), output_latency_gauge.as_any()],
            move |observer| {
                let now = SystemTime::now();

                if let Some(ref stats) = *stats.load() {
                    if let Some(latency) = stats.input_stats.latency(now) {
                        observer.observe_u64(&input_latency_gauge, latency, &[]);
                    }
                    if let Some(latency) = stats.output_stats.latency(now) {
                        observer.observe_u64(&output_latency_gauge, latency, &[]);
                    }
                }
            },
        )
        .expect("Initializing meter callback should not fail");
}

fn register_sys_metrics() {
    let meter = global::meter("pathway-sys");

    let pid = get_current_pid().expect("Failed to get current PID");

    let memory_usage_gauge = meter
        .u64_observable_gauge(PROCESS_MEMORY_USAGE)
        .with_unit(Unit::new("byte"))
        .init();

    let cpu_usage_gauge = meter
        .f64_observable_gauge(PROCESS_CPU_USAGAE)
        .with_unit(Unit::new("%"))
        .init();

    let cpu_user_time_gauge = meter
        .i64_observable_gauge(PROCESS_CPU_USER_TIME)
        .with_unit(Unit::new("s"))
        .init();

    let cpu_system_time_gauge = meter
        .i64_observable_gauge(PROCESS_CPU_SYSTEM_TIME)
        .with_unit(Unit::new("s"))
        .init();

    meter
        .register_callback(
            &[
                memory_usage_gauge.as_any(),
                cpu_usage_gauge.as_any(),
                cpu_user_time_gauge.as_any(),
                cpu_system_time_gauge.as_any(),
            ],
            move |observer| {
                let mut sys: System = System::new();
                let usage = getrusage(UsageWho::RUSAGE_SELF).expect("Failed to call getrusage");
                sys.refresh_process(pid);

                if let Some(process) = sys.process(pid) {
                    observer.observe_u64(&memory_usage_gauge, process.memory(), &[]);
                    observer.observe_f64(&cpu_usage_gauge, process.cpu_usage().into(), &[]);
                }
                observer.observe_i64(&cpu_user_time_gauge, usage.user_time().num_seconds(), &[]);
                observer.observe_i64(
                    &cpu_system_time_gauge,
                    usage.system_time().num_seconds(),
                    &[],
                );
            },
        )
        .expect("Initializing meter callback should not fail");
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.close_sender.blocking_send(()).unwrap();
        self.telemetry_thread_handle
            .take()
            .unwrap()
            .join()
            .expect("telemetry thread drop failed");
    }
}

pub fn maybe_run_telemetry_thread(graph: &dyn Graph, config: Config) -> Option<Runner> {
    match config {
        Config::Enabled(config) => {
            info!("Telemetry enabled");
            debug!("Telemetry config: {config:?}");
            let telemetry = Telemetry::new(config);
            let stats_shared = Arc::new(ArcSwapOption::from(None));
            let runner = Runner::run(telemetry, stats_shared.clone());

            graph
                .attach_prober(
                    Box::new(move |prober_stats| stats_shared.store(Some(Arc::new(prober_stats)))),
                    false,
                    false,
                )
                .expect("failed to start telemetry thread");

            Some(runner)
        }
        Config::Disabled => {
            debug!("Telemetry disabled");
            None
        }
    }
}
