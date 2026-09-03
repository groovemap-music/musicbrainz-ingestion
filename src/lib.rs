// Library exports for testing

pub mod config;
pub mod generated {
    pub mod catalog_contract;
}
pub mod extractor;
pub mod health;
pub mod message_queue;
pub mod musicbrainz;
pub mod polite_http;
pub mod runtime;
pub mod state_marker;
pub mod telemetry;
pub mod types;

// Stable MusicBrainz provider entry points.
pub use musicbrainz::downloader as musicbrainz_downloader;
pub use musicbrainz::jsonl_parser;

// Additional test modules
#[cfg(test)]
#[path = "tests/message_queue_unit_tests.rs"]
mod message_queue_unit_tests;
