// Generated from contracts/catalog-events/definitions/musicbrainz.json; do not edit.

pub const CONTRACT_NAME: &str = "groovemap.catalog-events";
pub const CONTRACT_VERSION: u32 = 1;
pub const SOURCE: &str = "musicbrainz";
pub const AMQP_EXCHANGE_TYPE: &str = "fanout";
pub const EXCHANGE_PREFIX_ENV: &str = "MUSICBRAINZ_EXCHANGE_PREFIX";
pub const DEFAULT_EXCHANGE_PREFIX: &str = "groovemap-musicbrainz";
pub const ENTITY_TYPES: &[&str] = &["artists", "labels", "release-groups", "releases"];
pub const CONSUMERS: &[&str] = &["brainzgraphinator", "brainztableinator"];
