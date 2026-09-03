//! MusicBrainz-owned acquisition, JSONL enrichment, and orchestration.
//!
//! The provider capability depends only on shared runtime mechanics within this
//! repository. It has no cross-container ordering, health polling, or lock.

pub mod downloader;
pub mod jsonl_parser;

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

use crate::config::ExtractorConfig;
use crate::runtime::{
    BatcherConfig, ExtractionStatus, ExtractorState, MessageQueueFactory, initial_run_outcome, message_batcher, message_publisher,
    reset_status_after_failed_check, spawn_shutdown_flag_monitor, wait_for_trigger,
};
use crate::state_marker::{PhaseStatus, ProcessingDecision, StateMarker};
use crate::telemetry;
use crate::types::{DataMessage, DataType, ExtractionProgress};

pub async fn run_musicbrainz_loop(
    config: Arc<ExtractorConfig>,
    state: Arc<RwLock<ExtractorState>>,
    shutdown: Arc<tokio::sync::Notify>,
    force_reprocess: bool,
    mq_factory: Arc<dyn MessageQueueFactory>,
    trigger: Arc<tokio::sync::Mutex<Option<bool>>>,
) -> Result<()> {
    // AtomicBool for non-consuming shutdown checks — see spawn_shutdown_flag_monitor. The monitor
    // parks on the Notify and sets the flag; process_musicbrainz_data polls the flag without side
    // effects, so the outer select! keeps its own shutdown arm.
    let shutdown_flag = spawn_shutdown_flag_monitor(shutdown.clone());

    info!("🎵 Starting MusicBrainz extraction...");

    let success = process_musicbrainz_data(config.clone(), state.clone(), shutdown_flag.clone(), force_reprocess, mq_factory.clone()).await?;

    // `process_musicbrainz_data` returns Ok(false) for BOTH a real failure and a between-files
    // shutdown; only this initial path promoted Ok(false) to Err. Short-circuit shutdown to Ok(())
    // so an operator-requested SIGTERM does not trigger the 600s failure cooldown + exit(1). (cu2.45)
    initial_run_outcome(success, shutdown_flag.load(Ordering::SeqCst), "MusicBrainz")?;

    info!("✅ Initial MusicBrainz processing completed successfully");

    // Periodic check loop — uses the AtomicBool flag for shutdown detection instead of
    // Notify::notified() in the select!, since the monitor task already consumes it.
    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            info!("🛑 Shutdown detected, exiting MusicBrainz periodic check loop");
            break;
        }

        // Transition Completed → Waiting before sleeping — see the equivalent block
        // in run_discogs_loop for rationale.
        {
            let mut s = state.write().await;
            if s.extraction_status == ExtractionStatus::Completed {
                s.extraction_status = ExtractionStatus::Waiting;
            }
        }

        let check_interval = Duration::from_secs(config.periodic_check_days * 24 * 60 * 60);
        info!("⏰ Waiting {} days before next MusicBrainz check...", config.periodic_check_days);

        tokio::select! {
            _ = sleep(check_interval) => {
                info!("🔄 Starting periodic check for new MusicBrainz dumps...");
                let start = Instant::now();
                match process_musicbrainz_data(config.clone(), state.clone(), shutdown_flag.clone(), false, mq_factory.clone()).await {
                    Ok(true) => {
                        info!("✅ Periodic MusicBrainz check completed successfully in {:?}", start.elapsed());
                    }
                    Ok(false) => {
                        error!("❌ Periodic MusicBrainz check completed with errors");
                    }
                    Err(e) => {
                        error!("❌ Periodic MusicBrainz check failed: {}", e);
                        reset_status_after_failed_check(&state).await; // (cu2.41)
                    }
                }
            }
            trigger_force_reprocess = wait_for_trigger(&trigger) => {
                info!("🔄 MusicBrainz extraction triggered via API (force_reprocess={})...", trigger_force_reprocess);
                let start = Instant::now();
                match process_musicbrainz_data(config.clone(), state.clone(), shutdown_flag.clone(), trigger_force_reprocess, mq_factory.clone()).await {
                    Ok(true) => info!("✅ Triggered MusicBrainz extraction completed in {:?}", start.elapsed()),
                    Ok(false) => error!("❌ Triggered MusicBrainz extraction completed with errors"),
                    Err(e) => {
                        error!("❌ Triggered MusicBrainz extraction failed: {}", e);
                        reset_status_after_failed_check(&state).await; // (cu2.41)
                    }
                }
            }
            _ = shutdown.notified() => {
                info!("🛑 Shutdown requested, stopping MusicBrainz periodic checks");
                break;
            }
        }
    }

    Ok(())
}

/// Process MusicBrainz JSONL dump files and publish records to AMQP.
///
/// Pipeline per file: blocking JSONL parser -> async batcher -> async publisher
pub async fn process_musicbrainz_data(
    config: Arc<ExtractorConfig>,
    state: Arc<RwLock<ExtractorState>>,
    shutdown_flag: Arc<AtomicBool>,
    force_reprocess: bool,
    mq_factory: Arc<dyn MessageQueueFactory>,
) -> Result<bool> {
    use self::downloader::{MbDownloader, discover_mb_dump_files};
    use self::jsonl_parser::{build_mbid_discogs_map_from_file, parse_mb_jsonl_file};

    let extraction_started_at = chrono::Utc::now();

    // Never start a new run under shutdown: the prelude below (multi-GB download, dump
    // discovery, AMQP setup, full artist.jsonl.xz scan) has no natural exit point, and
    // the first per-file check comes far too late for any stop grace period.
    if shutdown_flag.load(Ordering::SeqCst) {
        info!("🛑 Shutdown requested, not starting MusicBrainz extraction");
        return Ok(false);
    }

    // Reset progress for new run
    {
        let mut s = state.write().await;
        s.extraction_progress = ExtractionProgress::default();
        s.last_extraction_time.clear();
        s.completed_files.clear();
        s.active_connections.clear();
        s.error_count = 0;
        s.extraction_status = ExtractionStatus::Running;
    }

    // Download latest MusicBrainz dump if needed
    let downloader = MbDownloader::new(config.musicbrainz_root.clone(), config.musicbrainz_dump_url.clone());
    let download_result = downloader.download_latest().await?;

    // The download can take hours; do not follow it with the equally long map-building
    // and per-file passes if shutdown arrived in the meantime.
    if shutdown_flag.load(Ordering::SeqCst) {
        info!("🛑 Shutdown requested after MusicBrainz download, stopping before processing");
        return Ok(false);
    }
    let version = download_result.version().to_string();
    let versioned_root = config.musicbrainz_root.join(&version);
    info!("📋 Using MusicBrainz dump version: {} from {:?}", version, versioned_root);

    // Discover dump files in the versioned directory
    let dump_files = discover_mb_dump_files(&versioned_root)?;

    if dump_files.is_empty() {
        warn!("⚠️ No MusicBrainz dump files found after download");
        let mut s = state.write().await;
        s.extraction_status = ExtractionStatus::Completed;
        return Ok(true);
    }

    // Check state marker — skip if already completed and not force_reprocess
    let marker_path = StateMarker::musicbrainz_file_path(&config.musicbrainz_root, &version);
    let mut state_marker = if force_reprocess {
        info!("🔄 Force reprocess requested, creating new state marker");
        StateMarker::new(version.clone())
    } else {
        StateMarker::load(&marker_path).await?.unwrap_or_else(|| StateMarker::new(version.clone()))
    };

    let decision = state_marker.should_process();
    match decision {
        ProcessingDecision::Skip => {
            info!("✅ MusicBrainz version {} already processed, skipping", version);
            let mut s = state.write().await;
            s.extraction_status = ExtractionStatus::Completed;
            return Ok(true);
        }
        ProcessingDecision::Reprocess => {
            warn!("⚠️ Will re-process MusicBrainz version {}", version);
            state_marker = StateMarker::new(version.clone());
        }
        ProcessingDecision::Continue => {
            info!("🔄 Will continue processing MusicBrainz version {}", version);
        }
    }

    // Create message queue connection with MusicBrainz exchange prefix
    let mq = mq_factory
        .create(&config.amqp_connection, &config.musicbrainz_exchange_prefix)
        .await
        .context("Failed to connect to message queue for MusicBrainz")?;

    // Declare exchanges for MusicBrainz data types
    for data_type in DataType::musicbrainz() {
        mq.setup_exchange(data_type).await?;
    }

    // Start processing phase — wrap in Arc<Mutex> for shared access across loop iterations
    let file_count = dump_files.len();
    if state_marker.processing_phase.status == PhaseStatus::Pending {
        state_marker.start_processing(file_count);
        state_marker.save(&marker_path).await?;
        info!("🚀 Starting MusicBrainz processing phase: {} dump file(s)", file_count);
    } else if state_marker.processing_phase.status == PhaseStatus::InProgress {
        // Resume: update total count but do not reset progress counters
        state_marker.processing_phase.files_total = file_count;
        state_marker.save(&marker_path).await?;
        info!(
            "🔄 Resuming MusicBrainz processing phase: {} dump file(s), {} already completed",
            file_count, state_marker.processing_phase.files_processed
        );

        // Rehydrate the per-run progress counters from the persisted per-file progress so the
        // /health endpoint reports true totals for types already completed before the crash.
        // MB dump filenames don't parse via extract_data_type, so map through dump_files. (cu2.92)
        {
            let mut s = state.write().await;
            for (dt, path) in &dump_files {
                if let Some(fname) = path.file_name().and_then(|n| n.to_str())
                    && let Some(file_state) = state_marker.processing_phase.progress_by_file.get(fname)
                    && file_state.status == PhaseStatus::Completed
                {
                    s.extraction_progress.add(*dt, file_state.records_extracted);
                }
            }
        }
    }

    // Use the ORIGINAL processing start time persisted in the state marker (never this
    // possibly-resumed process's now()). On resume, already-completed files are skipped
    // and not re-sent, so their rows keep updated_at from the earlier session; sending
    // the resumed now() would make brainztableinator's stale-row purge wipe those rows.
    let processing_started_at = state_marker.processing_phase.started_at.unwrap_or(extraction_started_at);

    let state_marker = Arc::new(tokio::sync::Mutex::new(state_marker));

    // First pass: build MBID→Discogs ID map for artist relationship target resolution
    let artist_discogs_map = if let Some(artist_path) = dump_files.get(&DataType::Artists) {
        info!("🔍 First pass: building MBID→Discogs ID map for artists...");
        let path = artist_path.clone();
        tokio::task::spawn_blocking(move || build_mbid_discogs_map_from_file(&path, "artist")).await??
    } else {
        HashMap::new()
    };

    let mut record_counts: HashMap<String, u64> = HashMap::new();
    let mut success = true;

    for (data_type, file_path) in &dump_files {
        // Check for shutdown between files — allows graceful exit without waiting
        // for the next (potentially multi-GB) file to finish processing.
        // Uses AtomicBool instead of Notify::notified() to avoid consuming the
        // notification permit that the outer select! in run_musicbrainz_loop needs.
        if shutdown_flag.load(Ordering::SeqCst) {
            warn!("🛑 Shutdown requested, stopping MusicBrainz processing between files");
            success = false;
            break;
        }

        let file_name = file_path.file_name().and_then(|n| n.to_str()).unwrap_or("unknown");

        // Skip files already completed in state marker
        {
            let sm = state_marker.lock().await;
            if let Some(status) = sm.processing_phase.progress_by_file.get(file_name)
                && status.status == PhaseStatus::Completed
            {
                info!("✅ Skipping already-completed file: {}", file_name);
                telemetry::record_file_outcome(telemetry::FileOutcome::Skipped);
                // Still report the persisted count so a resumed run's extraction_complete
                // carries true totals for types completed in an earlier session. Without this
                // the skipped type's key is omitted from the message entirely. (cu2.92)
                record_counts.insert(data_type.to_string(), status.records_extracted);
                continue;
            }
        }

        info!("🚀 Starting MusicBrainz extraction of {} from {:?}", data_type, file_path);
        {
            let mut sm = state_marker.lock().await;
            sm.start_file_processing(file_name);
            sm.save(&marker_path).await?;
        }

        // Track active connection
        {
            let mut s = state.write().await;
            s.active_connections.insert(*data_type, file_name.to_string());
        }

        // Create channel for parser -> batcher -> publisher pipeline
        let (parse_sender, parse_receiver) = mpsc::channel::<DataMessage>(config.queue_size);
        let (batch_sender, batch_receiver) = mpsc::channel::<Vec<DataMessage>>(100);

        // Spawn parser on blocking thread — pass MBID→Discogs map for artist relationship enrichment
        let parser_path = file_path.clone();
        let parser_dt = *data_type;
        let parser_map = if parser_dt == DataType::Artists {
            Some(artist_discogs_map.clone())
        } else {
            None
        };
        let parser_handle = tokio::task::spawn_blocking(move || parse_mb_jsonl_file(&parser_path, parser_dt, parse_sender, parser_map.as_ref()));

        // Spawn batcher — share the same state_marker Arc across all iterations
        let batcher_config = BatcherConfig {
            batch_size: config.batch_size,
            data_type: *data_type,
            state: state.clone(),
            state_marker: state_marker.clone(),
            marker_path: marker_path.clone(),
            file_name: file_name.to_string(),
            state_save_interval: config.state_save_interval,
        };
        let batcher_handle = tokio::spawn(async move { message_batcher(parse_receiver, batch_sender, batcher_config).await });

        // Spawn publisher
        let pub_mq = mq.clone();
        let pub_dt = *data_type;
        let pub_state = state.clone();
        let publisher_handle = tokio::spawn(async move { message_publisher(batch_receiver, pub_mq, pub_dt, pub_state).await });

        // Wait for all stages — use per-file success flag to avoid cross-file bleed
        let mut file_success = true;

        let total_count = match parser_handle.await {
            Ok(Ok(count)) => count,
            Ok(Err(e)) => {
                error!("❌ MusicBrainz parser failed for {}: {}", data_type, e);
                file_success = false;
                0
            }
            Err(e) => {
                error!("❌ MusicBrainz parser task panicked for {}: {}", data_type, e);
                file_success = false;
                0
            }
        };

        match batcher_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("❌ MusicBrainz batcher failed for {}: {}", data_type, e);
                file_success = false;
            }
            Err(e) => {
                error!("❌ MusicBrainz batcher task panicked for {}: {}", data_type, e);
                file_success = false;
            }
        }

        match publisher_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("❌ MusicBrainz publisher failed for {}: {}", data_type, e);
                file_success = false;
            }
            Err(e) => {
                error!("❌ MusicBrainz publisher task panicked for {}: {}", data_type, e);
                file_success = false;
            }
        }

        if !file_success {
            success = false;
        }

        // Send file_complete BEFORE marking the file Completed in the state marker
        // (only attempted on success, to avoid misleading consumers). A failed send
        // must also flip file_success back off so the marker below stays NOT
        // Completed and the file remains pending on the next run — instead of the
        // signal being silently and permanently dropped for a file the marker
        // already claims is done.
        if file_success && let Err(e) = mq.send_file_complete(*data_type, file_name, total_count).await {
            error!("❌ Failed to send file_complete for {}: {}", data_type, e);
            file_success = false;
            success = false;
        }

        // Mark file complete only on success; on failure, save current state without marking complete
        {
            let mut sm = state_marker.lock().await;
            if file_success {
                sm.complete_file_processing(file_name, total_count);
            }
            sm.save(&marker_path).await?;
        }

        // Update shared state
        {
            let mut s = state.write().await;
            if file_success {
                s.completed_files.insert(file_name.to_string());
            }
            s.active_connections.remove(data_type);
        }

        telemetry::record_file_outcome(if file_success {
            telemetry::FileOutcome::Completed
        } else {
            telemetry::FileOutcome::Failed
        });

        record_counts.insert(data_type.to_string(), total_count);
        info!("✅ Completed MusicBrainz {} extraction: {} records", data_type, total_count);
    }

    // Send extraction_complete to MusicBrainz exchanges only (no masters) — only if all succeeded
    if success {
        let mb_types = DataType::musicbrainz();
        if let Err(e) = mq.send_extraction_complete(&version, processing_started_at, record_counts, &mb_types).await {
            error!("❌ Failed to send extraction_complete: {}", e);
            success = false;
        }
    } else {
        error!("❌ Skipping extraction_complete broadcast — processing had failures");
    }
    let _ = mq.close().await;

    // Finalize state marker
    {
        let mut sm = state_marker.lock().await;
        if success {
            sm.complete_processing();
            sm.complete_extraction();
            sm.save(&marker_path).await?;
            info!("✅ MusicBrainz processing completed: version {}", version);
        } else {
            sm.save(&marker_path).await?;
            error!("❌ MusicBrainz processing finished with errors — not marking complete");
        }
    }

    // Update extraction status
    {
        let mut s = state.write().await;
        s.extraction_status = if success { ExtractionStatus::Completed } else { ExtractionStatus::Failed };
    }

    Ok(success)
}
