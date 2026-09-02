//! Compatibility exports for callers of the pre-partition `extractor` module.
//!
//! New composition code should depend on `musicbrainz` or `runtime` directly.
//! No provider policy or orchestration belongs here.

pub use crate::musicbrainz::{process_musicbrainz_data, run_musicbrainz_loop};
pub use crate::runtime::{
    BatcherConfig, DefaultMessageQueueFactory, ExtractionStatus, ExtractorState, MessageQueueFactory, message_batcher, message_publisher,
};
