use std::env;
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::SystemTime;

use arc_swap::ArcSwapOption;
use hyper::service::{make_service_fn, service_fn};
use hyper::{header, Body, Method, Response, Server, StatusCode};
use log::{error, info};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use tokio::sync::oneshot::Sender;

use super::Error;
use super::Graph;
use super::ProberStats;

const DEFAULT_MONITORING_HTTP_PORT: u16 = 20000;

/// Retrieves metrics from prober stats in the `OpenMetrics` format
/// See <https://github.com/OpenObservability/OpenMetrics>
fn metrics_from_stats(stats: &Arc<ArcSwapOption<ProberStats>>) -> String {
    let stats_owned = stats.load().clone();
    let now = SystemTime::now();
    let mut metrics_text = String::new();
    if let Some(stats_owned) = stats_owned {
        let mut registry = <Registry>::default();

        let input_latency_ms: Gauge = Gauge::default();
        input_latency_ms.set(
            if let Some(latency) = stats_owned.input_stats.latency(now) {
                if let Ok(latency_converted) = i64::try_from(latency) {
                    latency_converted
                } else {
                    std::i64::MAX
                }
            } else {
                -1
            },
        );
        registry.register(
            "input_latency_ms",
            "A latency of input in milliseconds (-1 when finished)",
            input_latency_ms,
        );

        let output_latency_ms: Gauge = Gauge::default();
        output_latency_ms.set(
            if let Some(latency) = stats_owned.output_stats.latency(now) {
                if let Ok(latency_converted) = i64::try_from(latency) {
                    latency_converted
                } else {
                    std::i64::MAX
                }
            } else {
                -1
            },
        );
        registry.register(
            "output_latency_ms",
            "A latency of output in milliseconds (-1 when finished)",
            output_latency_ms,
        );

        encode(&mut metrics_text, &registry).unwrap();
    }
    metrics_text
}

/// Starts a lightweight http server allowing monitoring.
/// Available at: http://localhost:PORT/status
/// where PORT is `PATHWAY_MONITORING_HTTP_PORT + process_id`
/// It uses tokio and hyper. The status is passed using arcswap to avoid mutexes.
pub fn start_http_server_thread(
    process_id: u16,
    // monitoring_status: Arc<ArcSwap<String>>,
    stats: Arc<ArcSwapOption<ProberStats>>,
    http_terminate_receiver: tokio::sync::oneshot::Receiver<()>,
) -> JoinHandle<()> {
    let monitoring_http_port: u16 = env::var("PATHWAY_MONITORING_HTTP_PORT")
        .ok()
        .unwrap_or(String::new())
        .parse::<u16>()
        .unwrap_or(DEFAULT_MONITORING_HTTP_PORT);

    Builder::new()
        .name("pathway:http_monitoring".to_string())
        .spawn(move || {
            let stats = stats.clone();
            tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap()
                .block_on(async {
                    let addr = ([127, 0, 0, 1], monitoring_http_port + process_id).into();
                    let make_service = make_service_fn(move |_| {
                        let stats = stats.clone();
                        async move {
                            Ok::<_, Error>(service_fn(move |req| {
                                let stats = stats.clone();

                                async move {
                                    let mut response = Response::new(Body::empty());
                                    let stats = stats.clone();

                                    let metrics_text = metrics_from_stats(&stats);
                                    match (req.method(), req.uri().path()) {
                                        (&Method::GET, "/status") => {
                                            *response.body_mut() = Body::from(metrics_text);
                                            response.headers_mut().insert(
                                                header::CONTENT_TYPE,
                                                header::HeaderValue::from_static(
                                                    "application/json",
                                                ),
                                            );
                                        }
                                        (&Method::GET, "/metrics") => {                              
                                            *response.body_mut() = Body::from(metrics_text);
                                            response.headers_mut().insert(
                                                header::CONTENT_TYPE,
                                                header::HeaderValue::from_static(
                                                    "application/openmetrics-text; version=1.0.0; charset=utf-8",
                                                ),
                                            );
                                        }

                                        _ => {
                                            *response.status_mut() = StatusCode::NOT_FOUND;
                                        }
                                    };
                                    Ok::<_, Error>(response)
                                }
                            }))
                        }
                    });
                    let server = Server::bind(&addr).serve(make_service);
                    let shutdown_signal = async move {
                        http_terminate_receiver.await.unwrap();
                    };
                    let graceful = server.with_graceful_shutdown(shutdown_signal);
                    info!("Metrics available at http://{addr}");
                    if let Err(e) = graceful.await {
                        error!(
                            "http monitoring server error for process {process_id}: {e}"
                        );
                    }
                });
        })
        .expect("http monitoring thread creation failed")
}

pub struct Runner {
    http_server_thread_handle: Option<JoinHandle<()>>,
    http_terminate_transmitter: Option<Sender<()>>,
}

impl Runner {
    fn run(stats: &Arc<ArcSwapOption<ProberStats>>, process_id: usize) -> Runner {
        let (http_terminate_transmitter, http_terminate_receiver) =
            tokio::sync::oneshot::channel::<()>();
        let http_server_thread_handle = {
            let stats = Arc::clone(stats);
            start_http_server_thread(
                u16::try_from(process_id).unwrap(),
                stats,
                http_terminate_receiver,
            )
        };
        Runner {
            http_server_thread_handle: Some(http_server_thread_handle),
            http_terminate_transmitter: Some(http_terminate_transmitter),
        }
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.http_terminate_transmitter
            .take()
            .unwrap()
            .send(())
            .expect("couldn't send terminate message to http monitoring server");
        self.http_server_thread_handle
            .take()
            .unwrap()
            .join()
            .expect("http monitoring thread failed");
    }
}

pub fn maybe_run_http_server_thread(
    with_http_server: bool,
    graph: &dyn Graph,
    process_id: usize,
) -> Option<Runner> {
    if with_http_server && graph.worker_index() == 0 {
        let stats_shared = Arc::new(ArcSwapOption::from(None));
        let http_server_runner = Runner::run(&stats_shared, process_id);

        graph
            .attach_prober(
                Box::new(move |prober_stats| stats_shared.store(Some(Arc::new(prober_stats)))),
                false,
                false,
            )
            .expect("Failed to start http monitoring server");

        Some(http_server_runner)
    } else {
        None
    }
}
