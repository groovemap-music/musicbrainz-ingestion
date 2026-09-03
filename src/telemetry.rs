//! OpenTelemetry metrics for the MusicBrainz extractor.
//!
//! Self-contained on purpose: everything the extractor needs to push OTLP metrics lives in
//! this one file. It is a verbatim port of the `catalog-ingestion` module so the two stay
//! diffable, with only the crate's own identity (scope, default service name) changed.
//!
//! # Contract
//!
//! * Transport is **OTLP over HTTP/protobuf** to `OTEL_EXPORTER_OTLP_ENDPOINT`
//!   (`http://otel-collector:4318`). No gRPC, no Prometheus scrape endpoint. The existing
//!   JSON `/health`, `/metrics`, `/ready`, `/trigger` endpoints are part of the ADR-0005 HTTP
//!   contract and are untouched by this module.
//! * Only **standard OTEL environment variables** are read, and all of them are read by the
//!   SDK itself: `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`,
//!   `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_METRICS_EXPORTER`,
//!   `OTEL_METRIC_EXPORT_INTERVAL`. There are no GrooveMap-specific telemetry env vars.
//! * When no endpoint is configured, or `OTEL_METRICS_EXPORTER=none`, [`init_metrics`]
//!   returns `None` and every recording helper below degrades to the global no-op
//!   `MeterProvider`. Telemetry never fails startup and never panics.
//! * Export runs on the periodic reader's own dedicated thread, never on the extraction path
//!   and never on a tokio worker. Recording an instrument is an in-memory aggregation
//!   update; nothing on the extraction path touches the network.
//!
//! # Ordering
//!
//! [`init_metrics`] installs the global `MeterProvider` and MUST be called before the first
//! `record_*` call. The instruments are created once, lazily, from whatever provider is
//! global at that moment — so an early recording would permanently bind them to the no-op
//! provider. `main` calls `init_metrics` immediately after tracing is set up.

use std::io::Read;
use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, PeriodicReader, SdkMeterProvider, Stream, Temporality};
use tracing::{info, warn};

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
