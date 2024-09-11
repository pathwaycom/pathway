use std::{
    sync::Arc,
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use super::{error::DynError, license::License, Graph, ProberStats, Result};
use crate::env::parse_env_var;
use arc_swap::ArcSwapOption;
use itertools::Itertools;
use log::{debug, info};
use nix::sys::{
    resource::{getrusage, UsageWho},
    time::TimeValLike,
};
use opentelemetry::metrics::noop::NoopMeterProvider;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
        PeriodicReader, SdkMeterProvider,
    },
    propagation::TraceContextPropagator,
    runtime,
    trace::{self, TracerProvider},
    Resource,
};
use opentelemetry_semantic_conventions::resource::{
    SERVICE_INSTANCE_ID, SERVICE_NAME, SERVICE_NAMESPACE, SERVICE_VERSION,
};
use sysinfo::{get_current_pid, ProcessesToUpdate, System};
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

const ROOT_TRACE_ID: &str = "root.trace.id";
const RUN_ID: &str = "run.id";
const LICENSE_KEY: &str = "license.key";

const LOCAL_DEV_NAMESPACE: &str = "local-dev";

struct Telemetry {
    pub config: Box<TelemetryEnabled>,
}

impl Telemetry {
    fn new(config: Box<TelemetryEnabled>) -> Self {
        Telemetry { config }
    }

    fn resource(&self) -> Resource {
        let root_trace_id = root_trace_id(self.config.trace_parent.as_deref()).unwrap_or_default();

        Resource::new([
            KeyValue::new(SERVICE_NAME, self.config.service_name.clone()),
            KeyValue::new(SERVICE_VERSION, self.config.service_version.clone()),
            KeyValue::new(SERVICE_INSTANCE_ID, self.config.service_instance_id.clone()),
            KeyValue::new(SERVICE_NAMESPACE, self.config.service_namespace.clone()),
            KeyValue::new(ROOT_TRACE_ID, root_trace_id.to_string()),
            KeyValue::new(RUN_ID, self.config.run_id.clone()),
            KeyValue::new(LICENSE_KEY, self.config.license_key.clone()),
        ])
    }

    fn base_otel_exporter_builder(
        server_endpoint: &str,
    ) -> opentelemetry_otlp::TonicExporterBuilder {
        let tls_config = ClientTlsConfig::new().with_enabled_roots();
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_protocol(Protocol::Grpc)
            .with_endpoint(server_endpoint)
            .with_timeout(OPENTELEMETRY_EXPORT_TIMEOUT)
            .with_tls_config(tls_config)
    }

    fn init_tracer_provider(&self) {
        if self.config.tracing_servers.is_empty() {
            return;
        }
        global::set_text_map_propagator(TraceContextPropagator::new());

        let mut provider_builder = TracerProvider::builder()
            .with_config(trace::Config::default().with_resource(self.resource()));

        for endpoint in &self.config.tracing_servers {
            let exporter = Telemetry::base_otel_exporter_builder(endpoint)
                .build_span_exporter()
                .expect("exporter initialization should not fail");
            provider_builder = provider_builder.with_batch_exporter(exporter, runtime::Tokio);
        }

        global::set_tracer_provider(provider_builder.build().clone());
    }

    fn init_meter_provider(&self) -> Option<SdkMeterProvider> {
        if self.config.metrics_servers.is_empty() {
            return None;
        }

        let mut provider_builder = SdkMeterProvider::builder().with_resource(self.resource());

        for endpoint in &self.config.metrics_servers {
            let exporter = Telemetry::base_otel_exporter_builder(endpoint)
                .build_metrics_exporter(
                    Box::new(DefaultAggregationSelector::new()),
                    Box::new(DefaultTemporalitySelector::new()),
                )
                .unwrap();

            let reader = PeriodicReader::builder(exporter, runtime::Tokio)
                .with_interval(PERIODIC_READER_INTERVAL)
                .build();

            provider_builder = provider_builder.with_reader(reader);
        }

        let meter_provider = provider_builder.build();

        global::set_meter_provider(meter_provider.clone());

        Some(meter_provider)
    }

    fn init(&self) -> TelemetryGuard {
        let meter_provider = self.init_meter_provider();
        self.init_tracer_provider();

        TelemetryGuard { meter_provider }
    }
}

#[must_use]
struct TelemetryGuard {
    meter_provider: Option<SdkMeterProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.meter_provider.take() {
            provider.force_flush().unwrap_or(());
        }
        global::set_meter_provider(NoopMeterProvider::new());

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

fn deduplicate(input: Vec<Option<String>>) -> Vec<String> {
    input.into_iter().flatten().sorted().dedup().collect()
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct TelemetryEnabled {
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
        trace_parent: Option<String>,
    ) -> Result<Self> {
        let run_id = run_id.unwrap_or_else(|| Uuid::new_v4().to_string());

        if monitoring_server.is_some() {
            license
                .check_entitlements(vec!["monitoring".to_string()])
                .map_err(DynError::from)?;
        }

        let telemetry_server = if license.telemetry_required() {
            Some(PATHWAY_TELEMETRY_SERVER.to_string())
        } else {
            None
        };

        if monitoring_server.is_none() && telemetry_server.is_none() {
            return Ok(Config::Disabled);
        }

        match license {
            License::LicenseKey(_) => Config::create_enabled(
                run_id,
                telemetry_server,
                monitoring_server,
                trace_parent,
                license,
            ),
            License::NoLicenseKey => Ok(Config::Disabled),
        }
    }

    fn create_enabled(
        run_id: String,
        telemetry_server: Option<String>,
        monitoring_server: Option<String>,
        trace_parent: Option<String>,
        license: &License,
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
                    let _telemetry_guard = telemetry.init();
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
        .with_unit("ms")
        .init();

    let output_latency_gauge = meter
        .u64_observable_gauge(OUTPUT_LATENCY)
        .with_unit("ms")
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
        .with_unit("byte")
        .init();

    let cpu_user_time_gauge = meter
        .i64_observable_gauge(PROCESS_CPU_USER_TIME)
        .with_unit("s")
        .init();

    let cpu_system_time_gauge = meter
        .i64_observable_gauge(PROCESS_CPU_SYSTEM_TIME)
        .with_unit("s")
        .init();

    meter
        .register_callback(
            &[
                memory_usage_gauge.as_any(),
                cpu_user_time_gauge.as_any(),
                cpu_system_time_gauge.as_any(),
            ],
            move |observer| {
                let mut sys: System = System::new();
                let usage = getrusage(UsageWho::RUSAGE_SELF).expect("Failed to call getrusage");
                sys.refresh_processes(ProcessesToUpdate::Some(&[pid]));

                if let Some(process) = sys.process(pid) {
                    observer.observe_u64(&memory_usage_gauge, process.memory(), &[]);
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
            if config.tracing_servers.is_empty() {
                debug!("Telemetry disabled");
            } else {
                info!("Telemetry enabled");
            }
            debug!("OTEL config: {config:?}");
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
