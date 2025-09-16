use opentelemetry::{trace::noop::NoopTracerProvider, InstrumentationScope};
use std::{
    sync::Arc,
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use super::{error::DynError, license::License, Graph, Result};
use crate::{engine::dataflow::monitoring::ProberStats, env::parse_env_var};
use arc_swap::ArcSwapOption;
use itertools::Itertools;
use log::{debug, error, info};
use nix::sys::{
    resource::{getrusage, UsageWho},
    time::TimeValLike,
};
use opentelemetry::{
    global,
    metrics::Gauge,
    metrics::{Meter, MeterProvider},
    KeyValue,
};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::SdkTracerProvider,
    Resource,
};
use opentelemetry_semantic_conventions::resource::{
    SERVICE_INSTANCE_ID, SERVICE_NAME, SERVICE_NAMESPACE, SERVICE_VERSION,
};
use sysinfo::{get_current_pid, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::mpsc;
use tonic::transport::ClientTlsConfig;
use uuid::Uuid;

const PATHWAY_TELEMETRY_SERVER: &str = "https://usage.pathway.com";
const PERIODIC_READER_INTERVAL: Duration = Duration::from_secs(60);
const OPENTELEMETRY_EXPORT_TIMEOUT: Duration = Duration::from_secs(3);

const PROCESS_MEMORY_USAGE: &str = "process.memory.usage";
const PROCESS_CPU_USER_TIME: &str = "process.cpu.utime";
const PROCESS_CPU_SYSTEM_TIME: &str = "process.cpu.stime";
const INPUT_LATENCY: &str = "latency.input";
const OUTPUT_LATENCY: &str = "latency.output";
const OPERATOR_LATENCY: &str = "operator.latency";
const OPERATOR_INSERTIONS: &str = "operator.insertions";
const OPERATOR_DELETIONS: &str = "operator.deletions";

const ROOT_TRACE_ID: &str = "root.trace.id";
const RUN_ID: &str = "run.id";
const LICENSE_KEY: &str = "license.key";
const WORKER_ID: &str = "worker.id";

const LOCAL_DEV_NAMESPACE: &str = "local-dev";

mod exporter;
use self::exporter::SqliteExporter;

struct Telemetry {
    pub config: Box<TelemetryEnabled>,
    pub worker_id: usize,
}

impl Telemetry {
    fn new(config: Box<TelemetryEnabled>, worker_id: usize) -> Self {
        Telemetry { config, worker_id }
    }

    fn resource(&self) -> Resource {
        let root_trace_id = root_trace_id(self.config.trace_parent.as_deref()).unwrap_or_default();

        Resource::builder()
            .with_attributes([
                KeyValue::new(SERVICE_NAME, self.config.service_name.clone()),
                KeyValue::new(SERVICE_VERSION, self.config.service_version.clone()),
                KeyValue::new(SERVICE_INSTANCE_ID, self.config.service_instance_id.clone()),
                KeyValue::new(SERVICE_NAMESPACE, self.config.service_namespace.clone()),
                KeyValue::new(ROOT_TRACE_ID, root_trace_id.to_string()),
                KeyValue::new(RUN_ID, self.config.run_id.clone()),
                KeyValue::new(LICENSE_KEY, self.config.license_key.clone()),
                KeyValue::new(WORKER_ID, self.worker_id.to_string()),
            ])
            .build()
    }

    fn init_tracer_provider(&self) -> Option<SdkTracerProvider> {
        if self.config.tracing_servers.is_empty() {
            return None;
        }
        global::set_text_map_propagator(TraceContextPropagator::new());

        let mut provider_builder = SdkTracerProvider::builder().with_resource(self.resource());

        for endpoint in &self.config.tracing_servers {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_protocol(Protocol::Grpc)
                .with_endpoint(endpoint)
                .with_timeout(OPENTELEMETRY_EXPORT_TIMEOUT)
                .with_tls_config(ClientTlsConfig::new().with_enabled_roots())
                .build()
                .expect("exporter initialization should not fail");

            provider_builder = provider_builder.with_batch_exporter(exporter);
        }

        let tracer_provider = provider_builder.build();
        global::set_tracer_provider(tracer_provider.clone());
        Some(tracer_provider)
    }

    fn init_meter_provider(&self) -> Option<SdkMeterProvider> {
        if self.config.metrics_servers.is_empty() {
            return None;
        }

        let mut provider_builder = SdkMeterProvider::builder().with_resource(self.resource());

        for endpoint in &self.config.metrics_servers {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_protocol(Protocol::Grpc)
                .with_endpoint(endpoint)
                .with_timeout(OPENTELEMETRY_EXPORT_TIMEOUT)
                .with_tls_config(ClientTlsConfig::new().with_enabled_roots())
                .build()
                .expect("exporter initialization should not fail");

            let reader = PeriodicReader::builder(exporter)
                .with_interval(self.config.periodic_reader_interval)
                .build();

            provider_builder = provider_builder.with_reader(reader);
        }

        let meter_provider = provider_builder.build();
        global::set_meter_provider(meter_provider.clone());
        Some(meter_provider)
    }

    fn init_detailed_meter_provider(&self) -> Option<SdkMeterProvider> {
        let output_directory = self.config.detailed_metrics_dir.as_ref()?;

        let mut provider_builder = SdkMeterProvider::builder().with_resource(self.resource());

        let exporter = SqliteExporter::new(
            &self.config.run_id,
            output_directory,
            self.config.graph.as_ref(),
            self.worker_id,
        )
        .unwrap();
        let reader = PeriodicReader::builder(exporter)
            .with_interval(self.config.periodic_reader_interval)
            .build();

        provider_builder = provider_builder.with_reader(reader);

        let meter_provider = provider_builder.build();
        Some(meter_provider)
    }

    fn init(&self) -> TelemetryObserver {
        // Since NoopMeterProvider is no longer public, we store the initial instance to restore it on drop.
        let noop_meter_provider = MeterProviderWrapper(global::meter_provider());

        let meter_provider = self.init_meter_provider();
        let detailed_meter_provider = self.init_detailed_meter_provider();
        let tracer_provider = self.init_tracer_provider();

        TelemetryObserver {
            meter_provider,
            detailed_meter_provider,
            tracer_provider,
            noop_meter_provider,
        }
    }
}

#[derive(Clone)]
struct MeterProviderWrapper(Arc<dyn MeterProvider + Send + Sync>);

impl MeterProvider for MeterProviderWrapper {
    fn meter_with_scope(&self, scope: InstrumentationScope) -> Meter {
        self.0.meter_with_scope(scope)
    }
}

#[must_use]
#[allow(clippy::struct_field_names)]
struct TelemetryObserver {
    meter_provider: Option<SdkMeterProvider>,
    detailed_meter_provider: Option<SdkMeterProvider>,
    tracer_provider: Option<SdkTracerProvider>,
    noop_meter_provider: MeterProviderWrapper,
}

impl TelemetryObserver {
    async fn observe_stats(
        self,
        stats: Arc<ArcSwapOption<ProberStats>>,
        interval: Duration,
        mut shutdown: mpsc::Receiver<()>,
    ) {
        let mut interval = tokio::time::interval(interval);
        let mut sys: System = System::new();
        let pid = get_current_pid().expect("Failed to get current PID");

        let global_gauges = self
            .meter_provider
            .as_ref()
            .map(|provider| provider.meter("pathway-stats"))
            .map(|m| Gauges::new(&m));
        let mut detailed_gauges = self
            .detailed_meter_provider
            .as_ref()
            .map(|provider| provider.meter("pathway-detailed-stats"))
            .map(|m| Gauges::new(&m));

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    break;
                }
                _ = interval.tick() => {
                    if global_gauges.is_some() || detailed_gauges.is_some() {
                        match SystemMetrics::new(&mut sys, pid) {
                            Ok(system_metrics) => {
                                if let Some(g) = global_gauges.as_ref() {
                                    g.record_system_metrics(&system_metrics);
                                }
                                if let Some(g) = detailed_gauges.as_ref() {
                                    g.record_system_metrics(&system_metrics);
                                }
                            }
                            Err(e) => {
                                error!("Failed to collect system metrics for pid {pid}: {e}");
                            }
                        }
                    }

                    if let Some(current_stats) = stats.load().as_ref() {
                        if let Some(g) = global_gauges.as_ref() { g.record_global_metrics(current_stats); }
                        if let Some(g) = detailed_gauges.as_mut() {
                            g.record_global_metrics(current_stats);
                            g.record_detailed_metrics(current_stats);
                        }
                    }
                }

            }
        }
    }
}

impl Drop for TelemetryObserver {
    fn drop(&mut self) {
        if let Some(provider) = self.meter_provider.take() {
            provider.force_flush().unwrap_or(());
            provider.shutdown().unwrap_or(());
        }
        if let Some(provider) = self.detailed_meter_provider.take() {
            provider.force_flush().unwrap_or(());
            provider.shutdown().unwrap_or(());
        }
        if let Some(provider) = self.tracer_provider.take() {
            provider.force_flush().unwrap_or(());
            provider.shutdown().unwrap_or(());
        }
        global::set_meter_provider(self.noop_meter_provider.clone());
        global::set_tracer_provider(NoopTracerProvider::new());
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

fn deduplicate(input: Vec<Option<String>>) -> Vec<String> {
    input.into_iter().flatten().sorted().dedup().collect()
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct TelemetryEnabled {
    pub telemetry_server: Option<String>,
    pub monitoring_server: Option<String>,
    pub detailed_metrics_dir: Option<String>,
    pub logging_servers: Vec<String>,
    pub tracing_servers: Vec<String>,
    pub metrics_servers: Vec<String>,
    pub service_name: String,
    pub service_version: String,
    pub service_namespace: String,
    pub service_instance_id: String,
    pub run_id: String,
    pub trace_parent: Option<String>,
    pub license_key: String,
    pub periodic_reader_interval: Duration,
    pub graph: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Config {
    Enabled(Box<TelemetryEnabled>),
    Disabled,
}

impl Config {
    pub fn create(
        license: &License,
        run_id: Option<String>,
        monitoring_server: Option<String>,
        detailed_metrics_dir: Option<String>,
        trace_parent: Option<String>,
        periodic_reader_interval: Option<u64>,
        graph: Option<String>,
    ) -> Result<Self> {
        let run_id = run_id.unwrap_or_else(|| Uuid::new_v4().to_string());

        if monitoring_server.is_some() {
            license
                .check_entitlements(["monitoring"])
                .map_err(DynError::from)?;
        }

        if detailed_metrics_dir.is_some() {
            license
                .check_entitlements(["monitoring"])
                .map_err(DynError::from)?;
        }

        let telemetry_server = if license.telemetry_required() {
            Some(PATHWAY_TELEMETRY_SERVER.to_string())
        } else {
            None
        };

        if monitoring_server.is_none()
            && telemetry_server.is_none()
            && detailed_metrics_dir.is_none()
        {
            return Ok(Config::Disabled);
        }

        let periodic_reader_interval = match periodic_reader_interval {
            Some(interval) if interval != PERIODIC_READER_INTERVAL.as_secs() => {
                license
                    .check_entitlements(["monitoring-internal"])
                    .map_err(DynError::from)?;
                Duration::from_secs(interval)
            }
            _ => PERIODIC_READER_INTERVAL,
        };

        match license {
            License::NoLicenseKey => Ok(Config::Disabled),
            _ => Config::create_enabled(
                run_id,
                telemetry_server,
                monitoring_server,
                detailed_metrics_dir,
                trace_parent,
                license,
                periodic_reader_interval,
                graph,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_enabled(
        run_id: String,
        telemetry_server: Option<String>,
        monitoring_server: Option<String>,
        detailed_metrics_dir: Option<String>,
        trace_parent: Option<String>,
        license: &License,
        periodic_reader_interval: Duration,
        graph: Option<String>,
    ) -> Result<Self> {
        let service_instance_id: String = parse_env_var("PATHWAY_SERVICE_INSTANCE_ID")
            .map_err(DynError::from)?
            .unwrap_or(Uuid::new_v4().to_string());
        let service_namespace: String = parse_env_var("PATHWAY_SERVICE_NAMESPACE")
            .map_err(DynError::from)?
            .unwrap_or_else(|| {
                if service_instance_id.ends_with(LOCAL_DEV_NAMESPACE) {
                    LOCAL_DEV_NAMESPACE.to_string()
                } else {
                    format!("external-{}", Uuid::new_v4())
                }
            });
        Ok(Config::Enabled(Box::new(TelemetryEnabled {
            telemetry_server: telemetry_server.clone(),
            monitoring_server: monitoring_server.clone(),
            detailed_metrics_dir,
            logging_servers: deduplicate(vec![monitoring_server.clone()]),
            tracing_servers: deduplicate(vec![telemetry_server.clone(), monitoring_server.clone()]),
            metrics_servers: deduplicate(vec![telemetry_server, monitoring_server]),
            service_name: env!("CARGO_PKG_NAME").to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            service_namespace,
            service_instance_id,
            run_id,
            trace_parent,
            license_key: license.shortcut(),
            periodic_reader_interval,
            graph,
        })))
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

struct SystemMetrics {
    memory_usage: Option<u64>,
    cpu_user_time: i64,
    cpu_system_time: i64,
}

impl SystemMetrics {
    fn new(sys: &mut System, pid: Pid) -> Result<SystemMetrics> {
        Self::refresh_process_specifics(pid, sys);

        let usage = getrusage(UsageWho::RUSAGE_SELF)
            .map_err(|e| DynError::from(format!("Failed to call getrusage: {e}")))?;

        Ok(Self {
            memory_usage: sys.process(pid).map(sysinfo::Process::memory),
            cpu_user_time: usage.user_time().num_seconds() as i64,
            cpu_system_time: usage.system_time().num_seconds() as i64,
        })
    }

    fn refresh_process_specifics(pid: Pid, sys: &mut System) {
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_cpu(),
        );
    }
}

struct Gauges {
    process_memory_usage: Gauge<u64>,
    process_cpu_user_time: Gauge<i64>,
    process_cpu_system_time: Gauge<i64>,
    input_latency: Gauge<u64>,
    output_latency: Gauge<u64>,
    operator_latencies: Gauge<u64>,
    operator_insertions: Gauge<i64>,
    operator_deletions: Gauge<i64>,
}

impl Gauges {
    fn new(meter: &Meter) -> Self {
        Self {
            process_memory_usage: meter
                .u64_gauge(PROCESS_MEMORY_USAGE)
                .with_unit("byte")
                .build(),
            process_cpu_user_time: meter
                .i64_gauge(PROCESS_CPU_USER_TIME)
                .with_unit("s")
                .build(),
            process_cpu_system_time: meter
                .i64_gauge(PROCESS_CPU_SYSTEM_TIME)
                .with_unit("s")
                .build(),
            input_latency: meter.u64_gauge(INPUT_LATENCY).with_unit("ms").build(),
            output_latency: meter.u64_gauge(OUTPUT_LATENCY).with_unit("ms").build(),
            operator_latencies: meter.u64_gauge(OPERATOR_LATENCY).with_unit("ms").build(),
            operator_insertions: meter
                .i64_gauge(OPERATOR_INSERTIONS)
                .with_unit("count")
                .build(),
            operator_deletions: meter
                .i64_gauge(OPERATOR_DELETIONS)
                .with_unit("count")
                .build(),
        }
    }

    fn record_system_metrics(&self, metrics: &SystemMetrics) {
        if let Some(process_memory_usage) = metrics.memory_usage {
            self.process_memory_usage.record(process_memory_usage, &[]);
        }
        self.process_cpu_user_time
            .record(metrics.cpu_user_time, &[]);
        self.process_cpu_system_time
            .record(metrics.cpu_system_time, &[]);
    }

    fn record_global_metrics(&self, stats: &Arc<ProberStats>) {
        let system_time_now = SystemTime::now();
        if let Some(latency) = stats.input_stats.latency(system_time_now) {
            self.input_latency.record(latency, &[]);
        }
        if let Some(latency) = stats.output_stats.latency(system_time_now) {
            self.output_latency.record(latency, &[]);
        }
    }

    fn record_detailed_metrics(&mut self, stats: &Arc<ProberStats>) {
        let system_time_now = SystemTime::now();
        for (operator_id, operator_stats) in &stats.operators_stats {
            if let Some(latency) = operator_stats.latency(system_time_now) {
                let attributes = [KeyValue::new(
                    "operator.id",
                    i64::try_from(*operator_id).expect("operator_id does not fit in i64"),
                )];
                self.operator_latencies.record(latency, &attributes);
            }
        }
        for (operator_id, row_counts) in &stats.row_counts {
            let attributes = [KeyValue::new(
                "operator.id",
                i64::try_from(*operator_id).expect("operator_id does not fit in i64"),
            )];
            self.operator_insertions
                .record(row_counts.get_insertions() as i64, &attributes);
            self.operator_deletions
                .record(row_counts.get_deletions() as i64, &attributes);
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
                    let (tx, rx) = mpsc::channel::<()>(1);
                    let telemetry_observer = telemetry.init();

                    let interval = telemetry.config.periodic_reader_interval;
                    let observer_handle =
                        tokio::spawn(telemetry_observer.observe_stats(stats, interval, rx));

                    start_sender.send(tx).await.expect("should not fail");
                    let _ = observer_handle.await;
                });
        })
        .expect("telemetry thread creation failed");
    handle
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

pub fn maybe_run_telemetry_thread(
    graph: &dyn Graph,
    config: Config,
    worker_id: usize,
) -> Option<Runner> {
    match config {
        Config::Enabled(config) => {
            if config.telemetry_server.is_some() {
                info!("Telemetry enabled");
            }
            if let Some(ref monitoring_server) = config.monitoring_server {
                info!("Monitoring server: {monitoring_server}");
            }
            if let Some(ref dir) = config.detailed_metrics_dir {
                info!("Detailed metrics directory: {dir}");
            }

            let telemetry = Telemetry::new(config.clone(), worker_id);
            let stats_shared = Arc::new(ArcSwapOption::from(None));
            let runner = Runner::run(telemetry, stats_shared.clone());

            graph
                .attach_prober(
                    Box::new(move |prober_stats| stats_shared.store(Some(Arc::new(prober_stats)))),
                    true,
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
