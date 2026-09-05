//! OpenTelemetry metrics and traces for the MusicBrainz extractor.
//!
//! Self-contained on purpose: everything the extractor needs to push OTLP metrics lives in
//! this one file. It is a port of `catalog-ingestion`'s module at commit
//! `2135048d22001176fd012cd3f8fa336953b3cd76`, so the two stay diffable; only this crate's
//! own identity (instrumentation scope, default service name, `source` value) differs.
//!
//! # Contract
//!
//! * Transport is **OTLP over HTTP/protobuf** to `OTEL_EXPORTER_OTLP_ENDPOINT`, for metrics
//!   and traces alike (`http://otel-collector:4318`). No gRPC, no Prometheus scrape
//!   endpoint. The existing JSON `/health`, `/metrics`, `/ready`, `/trigger` endpoints are
//!   part of the ADR-0005 HTTP contract and are untouched by this module.
//! * Only **standard OTEL environment variables** are read, and all of them are read by the
//!   SDK itself: `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`,
//!   `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_METRICS_EXPORTER`,
//!   `OTEL_METRIC_EXPORT_INTERVAL`, and on the trace side `OTEL_TRACES_EXPORTER`,
//!   `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`,
//!   `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`. There are no GrooveMap-specific telemetry env
//!   vars.
//! * When no endpoint is configured, or `OTEL_METRICS_EXPORTER=none`, [`init_metrics`]
//!   returns `None` and every recording helper below degrades to the global no-op
//!   `MeterProvider`. [`init_traces`] behaves the same way: no provider, no `tracing` layer,
//!   and a no-op propagator that writes no `traceparent` onto published messages. Telemetry
//!   never fails startup and never panics.
//! * Export runs on the periodic reader's — and, for spans, the batch span processor's —
//!   own dedicated thread, never on the extraction path and never on a tokio worker.
//!   Recording an instrument is an in-memory aggregation update and closing a span is an
//!   enqueue; nothing on the extraction path touches the network.
//!
//! # Ordering
//!
//! [`init_metrics`] installs the global `MeterProvider` and MUST be called before the first
//! `record_*` call. The instruments are created once, lazily, from whatever provider is
//! global at that moment — so an early recording would permanently bind them to the no-op
//! provider. `main` calls `init_metrics` immediately after tracing is set up. It also
//! registers the process and tokio runtime observable instruments, which is why it has to
//! run on a tokio runtime thread.

use std::io::Read;
use std::sync::OnceLock;
use std::time::Duration;

use lapin::types::{AMQPValue, FieldTable};
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, PeriodicReader, SdkMeterProvider, Stream, Temporality};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing::{info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Instrumentation scope name reported on every metric this crate emits.
pub const INSTRUMENTATION_SCOPE: &str = "groovemap.musicbrainz-ingestion";

/// Code default for `service.name`; `OTEL_SERVICE_NAME` (set to `extractor-musicbrainz` by
/// compose) overrides it.
pub const DEFAULT_SERVICE_NAME: &str = "musicbrainz-ingestion";

// ---------------------------------------------------------------------------------------
// Metric names — GrooveMap OTEL conventions. Dot-names; Prometheus renders dots as
// underscores and appends `_total` / unit suffixes.
// ---------------------------------------------------------------------------------------

pub const METRIC_EXTRACTION_RECORDS: &str = "groovemap.extraction.records";
pub const METRIC_EXTRACTION_FILES: &str = "groovemap.extraction.files";
pub const METRIC_EXTRACTION_FILE_PROGRESS: &str = "groovemap.extraction.file.progress";
pub const METRIC_EXTRACTION_DOWNLOAD_BYTES: &str = "groovemap.extraction.download.bytes";
pub const METRIC_EXTRACTION_PUBLISH_CONFIRM_DURATION: &str = "groovemap.extraction.publish.confirm.duration";
pub const METRIC_EXTRACTION_ERRORS: &str = "groovemap.extraction.errors";
pub const METRIC_MESSAGING_SENT_MESSAGES: &str = "messaging.client.sent.messages";
pub const METRIC_PIPELINE_RECONNECTS: &str = "groovemap.pipeline.reconnects";

// ---------------------------------------------------------------------------------------
// Attribute keys — closed sets, low cardinality only. Never an id, file name, or free text.
// ---------------------------------------------------------------------------------------

pub const ATTR_SOURCE: &str = "source";
pub const ATTR_ENTITY: &str = "entity";
pub const ATTR_OUTCOME: &str = "outcome";
pub const ATTR_STAGE: &str = "stage";
pub const ATTR_SYSTEM: &str = "system";
pub const ATTR_MESSAGING_SYSTEM: &str = "messaging.system";
pub const ATTR_MESSAGING_DESTINATION_NAME: &str = "messaging.destination.name";

/// The only messaging system this extractor publishes to.
pub const MESSAGING_SYSTEM_RABBITMQ: &str = "rabbitmq";

/// The only provider this repository extracts; the `source` attribute value.
pub const SOURCE_MUSICBRAINZ: &str = "musicbrainz";

/// Terminal state of one input file, as reported on [`METRIC_EXTRACTION_FILES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileOutcome {
    Completed,
    Skipped,
    Failed,
}

impl FileOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            FileOutcome::Completed => "completed",
            FileOutcome::Skipped => "skipped",
            FileOutcome::Failed => "failed",
        }
    }
}

/// Pipeline stage an error is attributed to, as reported on [`METRIC_EXTRACTION_ERRORS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Download,
    Parse,
    Publish,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Download => "download",
            Stage::Parse => "parse",
            Stage::Publish => "publish",
        }
    }
}

// ---------------------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------------------

/// Explicit second-scale bucket boundaries for the publish-confirm histogram.
///
/// The SDK's default boundaries are millisecond-scale (5, 10, 25, … 10000), which would put
/// every broker confirm in the first bucket and make the histogram unreadable. Publisher
/// confirms range from sub-millisecond to the 30 s `PUBLISH_CONFIRM_TIMEOUT`.
const CONFIRM_DURATION_BOUNDARIES: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

/// Build and install the global metrics pipeline.
///
/// Returns the provider so the caller can shut it down (flushing the last export) on exit.
/// Returns `None` — after logging once — when metrics are disabled, when no OTLP endpoint is
/// configured, or when the exporter cannot be built. Never panics, never fails startup.
///
/// `service_name` is the code default for `service.name`; it is applied only when
/// `OTEL_SERVICE_NAME` is unset, so compose keeps the final say.
///
/// Safe to call from inside a tokio runtime. The blocking HTTP client is constructed on its
/// own thread by `opentelemetry-otlp`, and every export afterwards runs on the reader's
/// dedicated thread, so no runtime worker is ever blocked.
pub fn init_metrics(service_name: &str) -> Option<SdkMeterProvider> {
    if metrics_disabled() {
        info!("📉 OpenTelemetry metrics disabled (OTEL_METRICS_EXPORTER=none)");
        return None;
    }

    if !endpoint_configured() {
        info!("📉 OpenTelemetry metrics disabled (OTEL_EXPORTER_OTLP_ENDPOINT is unset)");
        return None;
    }

    let exporter = match opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_temporality(Temporality::Cumulative)
        .build()
    {
        Ok(exporter) => exporter,
        Err(e) => {
            // The endpoint is misconfigured, not the application. Log once and run without
            // metrics rather than taking the extractor down.
            warn!("⚠️ OpenTelemetry metrics disabled — failed to build OTLP exporter: {}", e);
            return None;
        }
    };

    // The stable periodic reader: it owns a dedicated OS thread and drives collection and
    // export from there, so the extraction loop and every tokio worker stay untouched. The
    // interval comes from `OTEL_METRIC_EXPORT_INTERVAL`, read by the builder.
    let reader = PeriodicReader::builder(exporter).build();

    let provider = SdkMeterProvider::builder()
        .with_resource(build_resource(service_name))
        .with_reader(reader)
        .with_view(confirm_duration_view)
        .build();

    global::set_meter_provider(provider.clone());

    // Only once a real provider is global: the observable callbacks are registered with
    // whatever provider is installed at this moment, so registering them on the disabled
    // path would bind them permanently to the no-op provider for nothing.
    register_runtime_metrics();

    info!("📈 OpenTelemetry metrics exporting over OTLP/HTTP-protobuf every {:?}", export_interval());

    Some(provider)
}

/// How long shutdown may spend flushing the last export before the process moves on.
///
/// Docker's default stop grace period is 10 s and other cleanup has already run by the time
/// this is called; an unreachable collector must not be what turns a clean SIGTERM into a
/// SIGKILL.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Flush and stop the metrics pipeline so the final export lands before the process exits.
///
/// Best-effort and bounded: a failure or a hung collector is logged, never propagated.
///
/// The reader's `shutdown` blocks the calling thread until its export thread replies, so it
/// runs on a blocking thread rather than on a runtime worker. The SDK documents that calling
/// it from the main thread of a current-thread runtime can deadlock; on a multi-thread
/// runtime it would merely stall a worker. `spawn_blocking` avoids both.
pub async fn shutdown_metrics(provider: Option<SdkMeterProvider>) {
    let Some(provider) = provider else {
        return;
    };

    let flush_and_stop = tokio::task::spawn_blocking(move || {
        if let Err(e) = provider.force_flush() {
            warn!("⚠️ Failed to flush OpenTelemetry metrics on shutdown: {}", e);
        }
        if let Err(e) = provider.shutdown() {
            warn!("⚠️ Failed to shut down the OpenTelemetry meter provider: {}", e);
        }
    });

    match tokio::time::timeout(SHUTDOWN_TIMEOUT, flush_and_stop).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("⚠️ OpenTelemetry shutdown task failed: {}", e),
        Err(_) => warn!("⚠️ OpenTelemetry shutdown did not finish within {:?} — exiting anyway", SHUTDOWN_TIMEOUT),
    }
}

/// `true` when `OTEL_METRICS_EXPORTER` explicitly selects `none`.
fn metrics_disabled() -> bool {
    std::env::var("OTEL_METRICS_EXPORTER").map(|v| v.trim().eq_ignore_ascii_case("none")).unwrap_or(false)
}

/// `true` when either the generic or the metrics-specific OTLP endpoint is set to a
/// non-empty value. The SDK reads the actual value; this only decides no-op vs. live.
fn endpoint_configured() -> bool {
    ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"]
        .iter()
        .any(|key| std::env::var(key).map(|v| !v.trim().is_empty()).unwrap_or(false))
}

/// The interval the SDK will use, resolved the same way `PeriodicReader` resolves it.
/// Logged at startup only; the reader reads the env var itself.
fn export_interval() -> Duration {
    std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(60))
}

/// Resource identifying this process.
///
/// `Resource::builder()` already runs the SDK-provided detector (`OTEL_SERVICE_NAME`) and the
/// environment detector (`OTEL_RESOURCE_ATTRIBUTES`, which carries `service.namespace` and
/// `deployment.environment.name` in compose). `service.version` comes from the package
/// version, and the code default for `service.name` is applied only when the env var is
/// absent so compose keeps precedence.
fn build_resource(service_name: &str) -> Resource {
    let builder = Resource::builder().with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")));

    match std::env::var("OTEL_SERVICE_NAME") {
        Ok(name) if !name.trim().is_empty() => builder.build(),
        _ => builder.with_service_name(service_name.to_owned()).build(),
    }
}

/// View that gives the publish-confirm histogram second-scale buckets.
fn confirm_duration_view(instrument: &Instrument) -> Option<Stream> {
    if instrument.name() != METRIC_EXTRACTION_PUBLISH_CONFIRM_DURATION {
        return None;
    }
    Stream::builder()
        .with_aggregation(Aggregation::ExplicitBucketHistogram { boundaries: CONFIRM_DURATION_BOUNDARIES.to_vec(), record_min_max: true })
        .build()
        .ok()
}

// ---------------------------------------------------------------------------------------
// Source label
// ---------------------------------------------------------------------------------------

static SOURCE: OnceLock<&'static str> = OnceLock::new();

/// Record which provider this process extracts, for the `source` attribute.
///
/// This container extracts exactly one provider, so the label is process-wide. Setting it
/// here keeps every call site free of source plumbing. Idempotent; the first call wins.
pub fn set_source(source: &str) {
    let interned: &'static str = match source {
        SOURCE_MUSICBRAINZ => SOURCE_MUSICBRAINZ,
        other => Box::leak(other.to_owned().into_boxed_str()),
    };
    let _ = SOURCE.set(interned);
}

/// The configured `source` attribute value, or `"unknown"` before [`set_source`] runs.
pub fn source() -> &'static str {
    SOURCE.get().copied().unwrap_or("unknown")
}

// ---------------------------------------------------------------------------------------
// Instruments
// ---------------------------------------------------------------------------------------

struct Instruments {
    records: Counter<u64>,
    files: Counter<u64>,
    file_progress: Gauge<f64>,
    download_bytes: Counter<u64>,
    publish_confirm_duration: Histogram<f64>,
    errors: Counter<u64>,
    messages_sent: Counter<u64>,
    reconnects: Counter<u64>,
}

impl Instruments {
    fn new(meter: &Meter) -> Self {
        Self {
            records: meter
                .u64_counter(METRIC_EXTRACTION_RECORDS)
                .with_description("Records extracted from provider dumps and handed to the publish pipeline")
                .build(),
            files: meter.u64_counter(METRIC_EXTRACTION_FILES).with_description("Provider dump files reaching a terminal state").build(),
            file_progress: meter
                .f64_gauge(METRIC_EXTRACTION_FILE_PROGRESS)
                .with_unit("1")
                .with_description("Fraction of the current dump file consumed, 0..1")
                .build(),
            download_bytes: meter
                .u64_counter(METRIC_EXTRACTION_DOWNLOAD_BYTES)
                .with_unit("By")
                .with_description("Bytes downloaded from the provider")
                .build(),
            publish_confirm_duration: meter
                .f64_histogram(METRIC_EXTRACTION_PUBLISH_CONFIRM_DURATION)
                .with_unit("s")
                .with_description("Time awaiting a RabbitMQ publisher confirm")
                .build(),
            errors: meter.u64_counter(METRIC_EXTRACTION_ERRORS).with_description("Extraction errors by pipeline stage").build(),
            messages_sent: meter
                .u64_counter(METRIC_MESSAGING_SENT_MESSAGES)
                .with_description("Messages successfully published and confirmed")
                .build(),
            reconnects: meter.u64_counter(METRIC_PIPELINE_RECONNECTS).with_description("Reconnections to a backing system").build(),
        }
    }
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

fn instruments() -> &'static Instruments {
    // Under `cfg(test)` the in-memory harness is installed before the instruments bind, so
    // it does not matter which test in the binary touches an instrument first — every test
    // observes the same real (non-no-op) provider.
    #[cfg(test)]
    let _ = &*test_harness::HARNESS;

    INSTRUMENTS.get_or_init(|| Instruments::new(&global::meter(INSTRUMENTATION_SCOPE)))
}

/// In-memory metrics pipeline used by the unit tests.
///
/// Deliberately lives in the module (not the test file) so [`instruments`] can force it into
/// existence before the one-shot instrument binding happens.
#[cfg(test)]
pub(crate) mod test_harness {
    use super::*;
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    use std::sync::LazyLock;

    pub(crate) struct Harness {
        pub(crate) exporter: InMemoryMetricExporter,
        pub(crate) provider: SdkMeterProvider,
    }

    pub(crate) static HARNESS: LazyLock<Harness> = LazyLock::new(|| {
        let exporter = InMemoryMetricExporter::default();
        // A long interval keeps the reader from exporting on its own; tests drive exports
        // explicitly with `force_flush`.
        let reader = PeriodicReader::builder(exporter.clone()).with_interval(Duration::from_secs(24 * 60 * 60)).build();
        let provider = SdkMeterProvider::builder()
            .with_resource(Resource::builder_empty().with_service_name("musicbrainz-ingestion-test").build())
            .with_reader(reader)
            .with_view(confirm_duration_view)
            .build();
        global::set_meter_provider(provider.clone());
        Harness { exporter, provider }
    });
}

// ---------------------------------------------------------------------------------------
// Recording helpers — the only surface the call sites touch.
// ---------------------------------------------------------------------------------------

/// `groovemap.extraction.records` — records extracted for one entity.
///
/// Called once per batch rather than once per record: a monthly dump is O(100M) records, and
/// a per-record attribute-set hash on that path is pure overhead for an identical total.
pub fn record_records(entity: &str, count: u64) {
    if count == 0 {
        return;
    }
    instruments().records.add(count, &[KeyValue::new(ATTR_SOURCE, source()), KeyValue::new(ATTR_ENTITY, entity.to_owned())]);
}

/// `groovemap.extraction.files` — one dump file reached a terminal state.
pub fn record_file_outcome(outcome: FileOutcome) {
    instruments().files.add(1, &[KeyValue::new(ATTR_SOURCE, source()), KeyValue::new(ATTR_OUTCOME, outcome.as_str())]);
}

/// `groovemap.extraction.file.progress` — fraction of the current file consumed, clamped to 0..1.
pub fn record_file_progress(entity: &str, ratio: f64) {
    let ratio = if ratio.is_finite() { ratio.clamp(0.0, 1.0) } else { 0.0 };
    instruments()
        .file_progress
        .record(ratio, &[KeyValue::new(ATTR_SOURCE, source()), KeyValue::new(ATTR_ENTITY, entity.to_owned())]);
}

/// `groovemap.extraction.download.bytes` — bytes pulled from the provider.
pub fn record_download_bytes(bytes: u64) {
    if bytes == 0 {
        return;
    }
    instruments().download_bytes.add(bytes, &[KeyValue::new(ATTR_SOURCE, source())]);
}

/// `groovemap.extraction.publish.confirm.duration` — seconds awaiting a publisher confirm.
pub fn record_publish_confirm(elapsed: Duration) {
    instruments().publish_confirm_duration.record(elapsed.as_secs_f64(), &[KeyValue::new(ATTR_SOURCE, source())]);
}

/// `groovemap.extraction.errors` — one error attributed to a pipeline stage.
pub fn record_error(stage: Stage) {
    instruments().errors.add(1, &[KeyValue::new(ATTR_SOURCE, source()), KeyValue::new(ATTR_STAGE, stage.as_str())]);
}

/// `messaging.client.sent.messages` — messages confirmed by the broker.
///
/// `destination` is the fanout exchange name, which is derived from the exchange prefix and
/// the entity type — a closed, low-cardinality set.
pub fn record_messages_sent(destination: &str, count: u64) {
    if count == 0 {
        return;
    }
    instruments().messages_sent.add(
        count,
        &[
            KeyValue::new(ATTR_MESSAGING_SYSTEM, MESSAGING_SYSTEM_RABBITMQ),
            KeyValue::new(ATTR_MESSAGING_DESTINATION_NAME, destination.to_owned()),
        ],
    );
}

/// `groovemap.pipeline.reconnects` — a reconnection to a backing system.
pub fn record_reconnect(system: &str) {
    instruments().reconnects.add(1, &[KeyValue::new(ATTR_SYSTEM, system.to_owned())]);
}

// ---------------------------------------------------------------------------------------
// Traces
//
// The trace pipeline mirrors the metric one exactly: OTLP over HTTP/protobuf, env-var
// driven, a no-op when disabled, and never able to fail startup. `BatchSpanProcessor` owns
// a dedicated OS thread just as `PeriodicReader` does, so the blocking reqwest client never
// touches a tokio worker and building the exporter from inside `#[tokio::main]` is safe.
//
// Spans reach this pipeline through `tracing`: `tracing-opentelemetry` layers the bridge
// onto the existing `tracing_subscriber` registry, so the call sites stay ordinary
// `tracing` spans and nothing in the extraction path depends on the OTEL API directly.
// ---------------------------------------------------------------------------------------

/// `messaging.operation.name` on a producer span; the only operation this service performs.
pub const ATTR_MESSAGING_OPERATION_NAME: &str = "messaging.operation.name";
pub const MESSAGING_OPERATION_PUBLISH: &str = "publish";

/// W3C header this service writes onto every published message.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// Build and install the global traces pipeline.
///
/// Returns the provider so the caller can flush and stop it on exit, and `None` — after
/// logging once — when traces are disabled, when no OTLP endpoint is configured, or when
/// the exporter cannot be built. Never panics, never fails startup.
///
/// # Ordering
///
/// The layer this provider feeds has to exist *before* the subscriber is installed, so
/// `main` necessarily calls this before there is anywhere for these log lines to go and
/// reports the resolved state itself once the subscriber is up. The logging is kept here
/// anyway so the function reads — and behaves — exactly like [`init_metrics`] for any
/// caller that initialises in the other order.
pub fn init_traces(service_name: &str) -> Option<SdkTracerProvider> {
    if traces_disabled() {
        info!("📉 OpenTelemetry traces disabled (OTEL_TRACES_EXPORTER=none)");
        return None;
    }

    if !trace_endpoint_configured() {
        info!("📉 OpenTelemetry traces disabled (OTEL_EXPORTER_OTLP_ENDPOINT is unset)");
        return None;
    }

    let exporter = match opentelemetry_otlp::SpanExporter::builder().with_http().with_protocol(opentelemetry_otlp::Protocol::HttpBinary).build() {
        Ok(exporter) => exporter,
        Err(e) => {
            warn!("⚠️ OpenTelemetry traces disabled — failed to build OTLP exporter: {}", e);
            return None;
        }
    };

    let mut builder = SdkTracerProvider::builder().with_resource(build_resource(service_name)).with_batch_exporter(exporter);

    // The SDK reads `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` itself, so it is only
    // the *default* that has to be corrected: the SDK falls back to `parentbased_always_on`,
    // where the GrooveMap contract is `parentbased_traceidratio`. Leaving the SDK's own
    // resolution alone whenever the variable IS set keeps every documented sampler value
    // working without this module having to reimplement the parser.
    if !env_is_set("OTEL_TRACES_SAMPLER") {
        builder = builder.with_sampler(default_sampler());
    }

    let provider = builder.build();
    global::set_tracer_provider(provider.clone());

    // W3C TraceContext, so `traceparent` on a published message is the same wire format the
    // Python consumers extract. Without this the global propagator stays a no-op and
    // [`inject_trace_context`] writes no headers at all — which is exactly what should
    // happen when traces are disabled.
    global::set_text_map_propagator(TraceContextPropagator::new());

    info!("🧵 OpenTelemetry traces exporting over OTLP/HTTP-protobuf");

    Some(provider)
}

/// Flush and stop the traces pipeline so the last spans land before the process exits.
///
/// Best-effort and bounded on the same reasoning as [`shutdown_metrics`]: the batch
/// processor's `shutdown` blocks the calling thread until its export thread replies, so it
/// runs on a blocking thread rather than on a runtime worker.
pub async fn shutdown_traces(provider: Option<SdkTracerProvider>) {
    let Some(provider) = provider else {
        return;
    };

    let flush_and_stop = tokio::task::spawn_blocking(move || {
        if let Err(e) = provider.force_flush() {
            warn!("⚠️ Failed to flush OpenTelemetry spans on shutdown: {}", e);
        }
        if let Err(e) = provider.shutdown() {
            warn!("⚠️ Failed to shut down the OpenTelemetry tracer provider: {}", e);
        }
    });

    match tokio::time::timeout(SHUTDOWN_TIMEOUT, flush_and_stop).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("⚠️ OpenTelemetry trace shutdown task failed: {}", e),
        Err(_) => warn!("⚠️ OpenTelemetry trace shutdown did not finish within {:?} — exiting anyway", SHUTDOWN_TIMEOUT),
    }
}

/// The `tracing` layer that turns this crate's spans into OTEL spans on `provider`.
///
/// Composed into the registry alongside the `fmt` layer; when [`init_traces`] returned
/// `None` there is no layer at all, so a disabled pipeline costs nothing per span.
pub fn trace_layer<S>(provider: &SdkTracerProvider) -> tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    tracing_opentelemetry::layer().with_tracer(provider.tracer(INSTRUMENTATION_SCOPE))
}

/// `true` when `OTEL_TRACES_EXPORTER` explicitly selects `none`.
fn traces_disabled() -> bool {
    std::env::var("OTEL_TRACES_EXPORTER").map(|v| v.trim().eq_ignore_ascii_case("none")).unwrap_or(false)
}

/// `true` when either the generic or the traces-specific OTLP endpoint is set.
fn trace_endpoint_configured() -> bool {
    ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"].iter().any(|key| env_is_set(key))
}

/// `true` when `key` is present and not blank. An empty value is treated as unset rather
/// than as a malformed setting.
fn env_is_set(key: &str) -> bool {
    std::env::var(key).map(|v| !v.trim().is_empty()).unwrap_or(false)
}

/// The GrooveMap default sampler, used only when `OTEL_TRACES_SAMPLER` is unset.
///
/// `OTEL_TRACES_SAMPLER_ARG` still applies — compose's dev stack sets 1.0 and the prod
/// overlay 0.1 — and an out-of-range or unparseable value falls back to sampling
/// everything rather than silently dropping traces.
fn default_sampler() -> Sampler {
    let ratio = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|ratio| (0.0..=1.0).contains(ratio))
        .unwrap_or(1.0);
    Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
}

// ---------------------------------------------------------------------------------------
// Span constructors — the only place span names, kinds, and attributes are decided.
//
// Names are low-cardinality by construction: every interpolated fragment comes from the
// same closed sets the metric attributes use. No file name, id, or byte count ever reaches
// a span, and no span carries a payload event.
// ---------------------------------------------------------------------------------------

/// `extract {source} {entity}` — the INTERNAL root span covering one dump file.
pub fn extract_span(entity: &str) -> tracing::Span {
    let name = format!("extract {} {}", source(), entity);
    tracing::info_span!("extract", otel.name = name.as_str(), otel.kind = "internal", source = source(), entity = entity,)
}

/// `download` — the INTERNAL child span covering one file's transfer from the provider.
pub fn download_span() -> tracing::Span {
    tracing::info_span!("download", otel.kind = "internal")
}

/// `parse` — the INTERNAL child span covering one file's JSONL parse.
pub fn parse_span() -> tracing::Span {
    tracing::info_span!("parse", otel.kind = "internal")
}

/// `publish {destination}` — the PRODUCER span covering one published batch.
///
/// `destination` is the fanout exchange name, derived from the exchange prefix and the
/// entity type, so it is the same closed set the messaging metrics use.
pub fn publish_span(destination: &str) -> tracing::Span {
    let name = format!("publish {}", destination);
    tracing::info_span!(
        "publish",
        otel.name = name.as_str(),
        otel.kind = "producer",
        messaging.system = MESSAGING_SYSTEM_RABBITMQ,
        messaging.destination.name = destination,
        messaging.operation.name = MESSAGING_OPERATION_PUBLISH,
    )
}

// ---------------------------------------------------------------------------------------
// AMQP context propagation
// ---------------------------------------------------------------------------------------

/// Writes W3C header entries into an AMQP header table as long strings.
struct AmqpHeaderInjector<'a>(&'a mut FieldTable);

impl Injector for AmqpHeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.into(), AMQPValue::LongString(value.into()));
    }
}

/// Reads W3C header entries back out of an AMQP header table.
struct AmqpHeaderExtractor<'a>(&'a FieldTable);

impl Extractor for AmqpHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        match self.0.inner().get(key) {
            Some(AMQPValue::LongString(value)) => std::str::from_utf8(value.as_bytes()).ok(),
            Some(AMQPValue::ShortString(value)) => Some(value.as_str()),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        self.0.inner().keys().map(|key| key.as_str()).collect()
    }
}

/// Inject the active span's trace context into `headers` as `traceparent` (and `tracestate`
/// when one is in play).
///
/// Reads the OTEL context off the current `tracing` span, so a call site only has to be
/// inside the producer span — it never has to hold an OTEL `Context` itself. With traces
/// disabled the global propagator is the SDK's no-op and this writes nothing, so a message
/// published by a non-traced extractor is byte-identical to one published before this
/// existed.
pub fn inject_trace_context(headers: &mut FieldTable) {
    let context = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| propagator.inject_context(&context, &mut AmqpHeaderInjector(headers)));
}

/// Extract a trace context from AMQP headers — the consumer side of
/// [`inject_trace_context`], and what the propagation tests assert round-trips.
///
/// Returns an empty context when the headers carry no valid `traceparent`, which is what a
/// consumer needs in order to start a fresh trace rather than join a malformed one.
pub fn extract_trace_context(headers: &FieldTable) -> opentelemetry::Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&AmqpHeaderExtractor(headers)))
}

// ---------------------------------------------------------------------------------------
// Runtime metrics — process resource usage and tokio scheduler state.
//
// Both families are *observable*: the SDK invokes their callbacks on the periodic reader's
// own thread at collection time, so nothing on the extraction path pays for them and there
// is no sampling task to schedule. The process family is Linux-only by construction — it
// reads `/proc/self`, which is the whole point of needing no extra crate — and registers
// nothing at all off Linux, so a missing series is an honest "not measurable here" rather
// than a zero that looks like a measurement.
// ---------------------------------------------------------------------------------------

pub const METRIC_PROCESS_CPU_TIME: &str = "process.cpu.time";
pub const METRIC_PROCESS_MEMORY_USAGE: &str = "process.memory.usage";
pub const METRIC_PROCESS_THREAD_COUNT: &str = "process.thread.count";
pub const METRIC_PROCESS_OPEN_FILE_DESCRIPTOR_COUNT: &str = "process.open_file_descriptor.count";
pub const METRIC_RUNTIME_TOKIO_WORKERS: &str = "groovemap.runtime.tokio.workers";
pub const METRIC_RUNTIME_TOKIO_ALIVE_TASKS: &str = "groovemap.runtime.tokio.alive_tasks";
pub const METRIC_RUNTIME_TOKIO_GLOBAL_QUEUE_DEPTH: &str = "groovemap.runtime.tokio.global_queue_depth";

/// Split of `process.cpu.time`; the only two values the kernel accounts for a process.
pub const ATTR_CPU_MODE: &str = "cpu.mode";
pub const CPU_MODE_USER: &str = "user";
pub const CPU_MODE_SYSTEM: &str = "system";

/// One-shot guard: the observable callbacks are registered with the meter, not owned by the
/// returned handles, so registering twice would double every reported value.
static RUNTIME_METRICS_REGISTERED: OnceLock<()> = OnceLock::new();

/// Register the process and tokio runtime observable instruments on the global meter.
///
/// Call from *inside* the tokio runtime: the tokio gauges capture `Handle::current()` once,
/// so the exporter thread can read the scheduler's counters without being on it. Idempotent
/// — the first call wins and later calls return without touching the meter.
///
/// Registration is deliberately not part of instrument binding: [`init_metrics`] calls this
/// only after it has a live provider, so a disabled bootstrap installs no callbacks at all.
///
/// Capturing the handle once is right for a service that has a single runtime for its whole
/// life, but it does mean the gauges report on whichever runtime registered FIRST. Under
/// libtest — where every `#[tokio::test]` gets its own runtime and drops it when the test
/// ends — only the test that registers can meaningfully assert on these gauges; a second
/// registering test would silently be reading a dead runtime.
pub fn register_runtime_metrics() {
    if RUNTIME_METRICS_REGISTERED.set(()).is_err() {
        return;
    }

    // Same reasoning as `instruments()`: force the in-memory harness into existence first so
    // the callbacks bind to the test provider rather than to the global no-op one.
    #[cfg(test)]
    let _ = &*test_harness::HARNESS;

    let meter = global::meter(INSTRUMENTATION_SCOPE);
    register_process_metrics(&meter);
    register_tokio_metrics(&meter);
}

/// `true` once [`register_runtime_metrics`] has installed the callbacks.
#[cfg(test)]
pub(crate) fn runtime_metrics_registered() -> bool {
    RUNTIME_METRICS_REGISTERED.get().is_some()
}

/// Observable gauges over `tokio::runtime::Handle::metrics()`.
///
/// Only the three stable `RuntimeMetrics` accessors are read. Everything richer in that API
/// (per-worker poll counts, steal counts, queue histograms) is gated behind
/// `--cfg tokio_unstable`, which would make the whole crate's build flags load-bearing, so
/// it is deliberately out of scope.
fn register_tokio_metrics(meter: &Meter) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        // Registering off-runtime would capture nothing to read; the caller is `init_metrics`
        // inside `#[tokio::main]`, so this is a misuse rather than an expected state.
        warn!("⚠️ tokio runtime gauges not registered — register_runtime_metrics ran outside a tokio runtime");
        return;
    };

    let workers = handle.clone();
    let _ = meter
        .u64_observable_gauge(METRIC_RUNTIME_TOKIO_WORKERS)
        .with_description("Worker threads in the tokio runtime")
        .with_callback(move |observer| observer.observe(workers.metrics().num_workers() as u64, &[]))
        .build();

    let alive_tasks = handle.clone();
    let _ = meter
        .u64_observable_gauge(METRIC_RUNTIME_TOKIO_ALIVE_TASKS)
        .with_description("Tasks spawned on the tokio runtime that have not yet completed")
        .with_callback(move |observer| observer.observe(alive_tasks.metrics().num_alive_tasks() as u64, &[]))
        .build();

    let queue_depth = handle;
    let _ = meter
        .u64_observable_gauge(METRIC_RUNTIME_TOKIO_GLOBAL_QUEUE_DEPTH)
        .with_description("Tasks queued on the tokio runtime's global injection queue")
        .with_callback(move |observer| observer.observe(queue_depth.metrics().global_queue_depth() as u64, &[]))
        .build();
}

/// Observable instruments over `/proc/self`.
///
/// Every reader returns `Option` and a `None` simply skips that observation for the cycle:
/// a transient `/proc` read failure leaves a gap in the series rather than reporting a zero
/// or taking the extractor down.
#[cfg(target_os = "linux")]
fn register_process_metrics(meter: &Meter) {
    let _ = meter
        .f64_observable_counter(METRIC_PROCESS_CPU_TIME)
        .with_unit("s")
        .with_description("CPU time consumed by this process, split by kernel accounting mode")
        .with_callback(|observer| {
            if let Some((user, system)) = read_process_cpu_seconds() {
                observer.observe(user, &[KeyValue::new(ATTR_CPU_MODE, CPU_MODE_USER)]);
                observer.observe(system, &[KeyValue::new(ATTR_CPU_MODE, CPU_MODE_SYSTEM)]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge(METRIC_PROCESS_MEMORY_USAGE)
        .with_unit("By")
        .with_description("Resident set size of this process")
        .with_callback(|observer| {
            if let Some(bytes) = read_process_rss_bytes() {
                observer.observe(bytes, &[]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge(METRIC_PROCESS_THREAD_COUNT)
        .with_description("OS threads in this process")
        .with_callback(|observer| {
            if let Some(threads) = read_process_thread_count() {
                observer.observe(threads, &[]);
            }
        })
        .build();

    let _ = meter
        .u64_observable_gauge(METRIC_PROCESS_OPEN_FILE_DESCRIPTOR_COUNT)
        .with_description("File descriptors currently open by this process")
        .with_callback(|observer| {
            if let Some(fds) = read_open_file_descriptor_count() {
                observer.observe(fds, &[]);
            }
        })
        .build();
}

/// Off Linux there is no `/proc/self`, so the process instruments are not created — an
/// absent series, never a fabricated zero. The tokio gauges are portable and still register.
#[cfg(not(target_os = "linux"))]
fn register_process_metrics(_meter: &Meter) {
    tracing::debug!("🔍 process.* runtime instruments not registered — /proc/self is Linux-only");
}

/// The kernel reports `utime`/`stime` in clock ticks. `USER_HZ` — what `sysconf(_SC_CLK_TCK)`
/// returns — is a frozen part of the Linux userspace ABI at 100, independent of the kernel's
/// internal `CONFIG_HZ`, so the conversion is a constant and costs no `libc` dependency.
#[cfg(target_os = "linux")]
const USER_HZ: f64 = 100.0;

/// `(user, system)` CPU seconds for this process, from `/proc/self/stat`.
#[cfg(target_os = "linux")]
fn read_process_cpu_seconds() -> Option<(f64, f64)> {
    parse_process_cpu_seconds(&std::fs::read_to_string("/proc/self/stat").ok()?)
}

/// Split out from the reader so the field arithmetic is testable without a real `/proc`.
#[cfg(target_os = "linux")]
fn parse_process_cpu_seconds(stat: &str) -> Option<(f64, f64)> {
    // Field 2 (`comm`) is parenthesised and may itself contain spaces and parentheses, so
    // whitespace splitting is only unambiguous after the LAST ')'.
    let rest = stat.rsplit_once(')')?.1;
    let mut fields = rest.split_whitespace();
    // The first field after `comm` is `state` (field 3), so `utime` (field 14) is 11 hops on
    // and `stime` (field 15) is the one after it.
    let utime: u64 = fields.nth(11)?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some((utime as f64 / USER_HZ, stime as f64 / USER_HZ))
}

/// Resident set size in bytes, from the `VmRSS` line of `/proc/self/status`.
///
/// `status` rather than `statm` on purpose: it reports kB directly, so the page size never
/// has to be looked up through `sysconf`.
#[cfg(target_os = "linux")]
fn read_process_rss_bytes() -> Option<u64> {
    read_status_field(&std::fs::read_to_string("/proc/self/status").ok()?, "VmRSS:").map(|kb| kb.saturating_mul(1024))
}

/// Live OS thread count, from the `Threads` line of `/proc/self/status`.
#[cfg(target_os = "linux")]
fn read_process_thread_count() -> Option<u64> {
    read_status_field(&std::fs::read_to_string("/proc/self/status").ok()?, "Threads:")
}

/// First numeric token of the `key` line in a `/proc/self/status` body.
#[cfg(target_os = "linux")]
fn read_status_field(status: &str, key: &str) -> Option<u64> {
    status.lines().find_map(|line| line.strip_prefix(key)?.split_whitespace().next()?.parse().ok())
}

/// Open descriptors, counted as entries in `/proc/self/fd`.
#[cfg(target_os = "linux")]
fn read_open_file_descriptor_count() -> Option<u64> {
    // `read_dir` itself holds an open descriptor on the directory it is enumerating, and
    // that descriptor appears in the listing — so it is discounted from the total it is
    // being used to take.
    let entries = std::fs::read_dir("/proc/self/fd").ok()?.count() as u64;
    Some(entries.saturating_sub(1))
}

// ---------------------------------------------------------------------------------------
// Parse-progress reader
// ---------------------------------------------------------------------------------------

/// Report progress at most this often, in bytes consumed, so the gauge does not churn once
/// per 8 KiB read on a multi-GB dump.
const PROGRESS_REPORT_STEP: u64 = 8 * 1024 * 1024;

/// A `Read` adapter that publishes [`METRIC_EXTRACTION_FILE_PROGRESS`] as it is consumed.
///
/// Wraps the *compressed* input file, so the ratio is bytes consumed over the on-disk file
/// size — a genuinely monotone 0..1 for the xz-compressed JSONL dumps, where the
/// uncompressed record count is unknown until the file has been fully read.
pub struct ProgressReader<R> {
    inner: R,
    entity: &'static str,
    total: u64,
    read: u64,
    reported_at: u64,
}

impl<R: Read> ProgressReader<R> {
    fn ratio(&self) -> f64 {
        if self.total == 0 { 0.0 } else { self.read as f64 / self.total as f64 }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += n as u64;
        if self.read.saturating_sub(self.reported_at) >= PROGRESS_REPORT_STEP {
            self.reported_at = self.read;
            record_file_progress(self.entity, self.ratio());
        }
        Ok(n)
    }
}

impl<R> Drop for ProgressReader<R> {
    fn drop(&mut self) {
        // Publish the final ratio, so a file that finished lands on 1.0 rather than on the
        // last 8 MiB checkpoint, and an aborted one lands on where it actually stopped.
        let ratio = if self.total == 0 { 0.0 } else { self.read as f64 / self.total as f64 };
        record_file_progress(self.entity, ratio);
    }
}

/// Wrap a reader so consuming it reports file progress for `entity`.
///
/// `total` is the on-disk size of the file being read; `0` disables the ratio (it is
/// reported as 0.0) rather than dividing by zero.
pub fn progress_reader<R: Read>(inner: R, total: u64, entity: &'static str) -> ProgressReader<R> {
    ProgressReader { inner, entity, total, read: 0, reported_at: 0 }
}

#[cfg(test)]
#[path = "tests/telemetry_tests.rs"]
mod tests;
