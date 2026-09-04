//! Canonical media block (ADR 0007).
//!
//! MusicBrainz describes a release's media as a list of mediums naming a format, plus three
//! release-level enumerations that live beside them: the release `status`, the `packaging`,
//! and the release group's primary and secondary types. Medium, edition, packaging, release
//! kind, and traits therefore arrive in four unrelated places, and every consumer that
//! needed one of them re-derived it differently.
//!
//! This module maps those inputs onto the provider-neutral vocabulary vendored at
//! `contracts/catalog-events/vocab/media-taxonomy.json` and produces the canonical `media`
//! block the producer attaches to every `releases` event, once, at the normalization
//! boundary, so the content hash covers it and every downstream store reads one shape.
//!
//! The vocabulary — never this code — decides routing, so a new upstream format name is a
//! re-vendoring, not a code change. Values the vocabulary does not know are preserved under
//! `unmapped` and never dropped.
//!
//! Behaviour is fixed by the conformance fixtures in `tests/fixtures/media/`, which the
//! design repository's reference mapper (`scripts/media-mapper.mjs`) also has to satisfy.
//! The Discogs producer carries the byte-similar sibling of this file; the two
//! implementations and the shared Python mapper must agree exactly, so change this file
//! only alongside those fixtures.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

/// The vendored vocabulary, compiled in. `contracts/generate.py --check` fails the build
/// gate when these bytes drift from the digest recorded beside them, so parsing it is
/// infallible in practice.
const TAXONOMY_JSON: &str = include_str!("../../contracts/catalog-events/vocab/media-taxonomy.json");

// ── The vendored vocabulary ─────────────────────────────────────────

/// A media family: the closed top-level grouping (`vinyl`, `optical`, `digital`, …).
///
/// `resolve` lets a family that a source names without a medium (MusicBrainz `Vinyl`) pick a
/// medium from an item attribute. MusicBrainz never states a size, so its parent formats
/// always fall through to `<family>_unspecified`; the rule is carried because the vocabulary
/// is shared with Discogs, which does derive a size from its descriptions.
#[derive(Debug, Deserialize)]
struct FamilyDefinition {
    id: String,
    #[serde(default)]
    resolve: Option<ResolveRule>,
}

/// How a family resolves to a medium: look the stringified value of `attribute` up in `map`.
#[derive(Debug, Deserialize)]
struct ResolveRule {
    attribute: String,
    map: HashMap<String, String>,
}

/// A canonical medium, prefixed by its family (`vinyl_12`, `optical_cd`, `digital_file`).
///
/// `defaults` fill attributes the source never stated — 78 RPM for shellac, 12 inches for a
/// 12" medium.
#[derive(Debug, Deserialize)]
struct MediumDefinition {
    id: String,
    family: String,
    #[serde(default)]
    defaults: Map<String, Value>,
}

/// What a MusicBrainz format *name* means: a medium, a family, a container, or a release
/// flag.
///
/// The vocabulary guarantees these are mutually exclusive — a container or a flag entry
/// never carries a medium, and a variant never appears without one.
#[derive(Debug, Default, Deserialize)]
struct FormatEntry {
    #[serde(default)]
    family: Option<String>,
    #[serde(default)]
    medium: Option<String>,
    #[serde(default)]
    variant: Option<String>,
    #[serde(default)]
    container: Option<String>,
    #[serde(default)]
    flag: Option<String>,
}

/// The MusicBrainz half of the vocabulary: one table per upstream enumeration.
///
/// A `status` maps to an edition or to `null` — `Official` is the unmarked default and adds
/// nothing rather than an `official` edition nobody would filter on.
#[derive(Debug, Deserialize)]
struct MusicBrainzVocabulary {
    formats: HashMap<String, FormatEntry>,
    status: HashMap<String, Option<String>>,
    packaging: HashMap<String, String>,
    primary_types: HashMap<String, String>,
    secondary_types: HashMap<String, String>,
}

/// The document as vendored. Sections this producer does not consume (`values`, `discogs`,
/// `license`) are ignored rather than rejected.
#[derive(Debug, Deserialize)]
struct TaxonomyDocument {
    taxonomy_version: String,
    families: Vec<FamilyDefinition>,
    media: Vec<MediumDefinition>,
    musicbrainz: MusicBrainzVocabulary,
}

/// The vocabulary indexed for lookup, parsed once per process.
#[derive(Debug)]
struct Taxonomy {
    taxonomy_version: String,
    families: HashMap<String, FamilyDefinition>,
    media: HashMap<String, MediumDefinition>,
    musicbrainz: MusicBrainzVocabulary,
}

impl From<TaxonomyDocument> for Taxonomy {
    fn from(document: TaxonomyDocument) -> Self {
        Taxonomy {
            taxonomy_version: document.taxonomy_version,
            families: document.families.into_iter().map(|family| (family.id.clone(), family)).collect(),
            media: document.media.into_iter().map(|medium| (medium.id.clone(), medium)).collect(),
            musicbrainz: document.musicbrainz,
        }
    }
}

fn taxonomy() -> &'static Taxonomy {
    static TAXONOMY: OnceLock<Taxonomy> = OnceLock::new();
    TAXONOMY.get_or_init(|| {
        let document: TaxonomyDocument =
            serde_json::from_str(TAXONOMY_JSON).expect("the vendored media taxonomy is valid JSON in the expected shape");
        Taxonomy::from(document)
    })
}

// ── The canonical block ─────────────────────────────────────────────

/// The provider fields exactly as received, kept as the provenance record.
///
/// `descriptions` and `text` are Discogs-only and are always empty and `null` here; they are
/// part of the shape so a block means the same thing whichever producer wrote it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ItemSource {
    pub provider: String,
    pub name: Option<String>,
    pub descriptions: Vec<String>,
    pub text: Option<String>,
}

/// One entry per source medium entry, in source order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MediaItem {
    pub family: Option<String>,
    pub medium: Option<String>,
    pub qty: u64,
    pub size_inches: Option<Number>,
    pub speed_rpm: Option<Number>,
    pub channels: Option<String>,
    pub codec: Option<String>,
    pub variants: Vec<String>,
    pub appearance: Vec<String>,
    pub position: Option<i64>,
    pub track_count: Option<i64>,
    pub source: ItemSource,
}

/// Raw values the vocabulary did not recognise. Sorted and de-duplicated, never dropped, so
/// coverage is measurable from the published events.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Unmapped {
    pub formats: Vec<String>,
    pub descriptions: Vec<String>,
}

/// The canonical `media` block attached to every `releases` event.
///
/// Every field is always present: `null` or an empty list when unknown. Lists other than
/// `items` and `source.descriptions` are sorted and de-duplicated, so two implementations
/// serialise byte-identical output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MediaBlock {
    pub taxonomy_version: String,
    pub items: Vec<MediaItem>,
    pub families: Vec<String>,
    pub release_kind: Option<String>,
    pub traits: Vec<String>,
    pub edition: Vec<String>,
    pub packaging: Option<String>,
    pub container: Option<String>,
    pub flags: Vec<String>,
    pub unmapped: Unmapped,
}

impl MediaItem {
    fn new(name: Option<String>, descriptions: Vec<String>, text: Option<String>) -> Self {
        MediaItem {
            family: None,
            medium: None,
            qty: 1,
            size_inches: None,
            speed_rpm: None,
            channels: None,
            codec: None,
            variants: Vec::new(),
            appearance: Vec::new(),
            position: None,
            track_count: None,
            source: ItemSource { provider: "musicbrainz".to_string(), name, descriptions, text },
        }
    }

    /// Set a medium attribute only when the source has not already stated it. The medium
    /// defaults are the only thing that fills one on this side.
    fn fill_attribute(&mut self, attribute: &str, value: &Value) {
        match attribute {
            "size_inches" => fill(&mut self.size_inches, value.as_number().cloned()),
            "speed_rpm" => fill(&mut self.speed_rpm, value.as_number().cloned()),
            "channels" => fill(&mut self.channels, value.as_str().map(str::to_string)),
            "codec" => fill(&mut self.codec, value.as_str().map(str::to_string)),
            _ => {}
        }
    }

    /// The stringified attribute a family resolves a medium by, or `None` when unset.
    fn attribute_key(&self, attribute: &str) -> Option<String> {
        match attribute {
            "size_inches" => self.size_inches.as_ref().map(Number::to_string),
            "speed_rpm" => self.speed_rpm.as_ref().map(Number::to_string),
            "channels" => self.channels.clone(),
            "codec" => self.codec.clone(),
            _ => None,
        }
    }
}

impl MediaBlock {
    fn empty(taxonomy: &Taxonomy) -> Self {
        MediaBlock {
            taxonomy_version: taxonomy.taxonomy_version.clone(),
            items: Vec::new(),
            families: Vec::new(),
            release_kind: None,
            traits: Vec::new(),
            edition: Vec::new(),
            packaging: None,
            container: None,
            flags: Vec::new(),
            unmapped: Unmapped::default(),
        }
    }
}

/// Fill a slot only when it is still unset — the first value the source states wins, and a
/// medium default never overwrites it.
fn fill<T>(slot: &mut Option<T>, value: Option<T>) {
    if slot.is_none()
        && let Some(value) = value
    {
        *slot = Some(value);
    }
}

fn sorted_unique(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

// ── The mapper ──────────────────────────────────────────────────────

/// Apply a format-name entry, returning the item when the entry names a medium.
///
/// A container or flag entry is a release-level fact and produces no item at all. The
/// MusicBrainz vocabulary has no such entry today; the branch is carried because the
/// vocabulary is shared and re-vendoring may add one.
fn apply_format_entry(block: &mut MediaBlock, entry: &FormatEntry, mut item: MediaItem) -> Option<MediaItem> {
    if let Some(container) = &entry.container {
        block.container = Some(container.clone());
    }
    if let Some(flag) = &entry.flag {
        block.flags.push(flag.clone());
    }
    if entry.family.is_none() && entry.medium.is_none() {
        return None;
    }
    if let Some(medium) = &entry.medium {
        item.medium = Some(medium.clone());
    }
    if let Some(family) = &entry.family {
        item.family = Some(family.clone());
    }
    if let Some(variant) = &entry.variant {
        item.variants.push(variant.clone());
    }
    Some(item)
}

/// Resolve the item's medium and family, fill the medium's defaults, and order its lists.
fn finish_item(mut item: MediaItem, taxonomy: &Taxonomy) -> MediaItem {
    if let Some(medium_id) = item.medium.clone()
        && item.family.is_none()
        && let Some(medium) = taxonomy.media.get(&medium_id)
    {
        item.family = Some(medium.family.clone());
    }
    if item.medium.is_none()
        && let Some(family_id) = item.family.clone()
    {
        let resolved = taxonomy.families.get(&family_id).and_then(|family| family.resolve.as_ref()).and_then(|resolve| {
            let key = item.attribute_key(&resolve.attribute)?;
            resolve.map.get(&key).cloned()
        });
        item.medium = Some(resolved.unwrap_or_else(|| format!("{family_id}_unspecified")));
    }
    if let Some(medium_id) = item.medium.clone()
        && let Some(medium) = taxonomy.media.get(&medium_id)
    {
        for (attribute, value) in medium.defaults.clone() {
            item.fill_attribute(&attribute, &value);
        }
    }
    item.variants = sorted_unique(item.variants);
    item.appearance = sorted_unique(item.appearance);
    item
}

fn finish_block(mut block: MediaBlock) -> MediaBlock {
    block.families = sorted_unique(block.items.iter().filter_map(|item| item.family.clone()).collect());
    block.traits = sorted_unique(block.traits);
    block.edition = sorted_unique(block.edition);
    block.flags = sorted_unique(block.flags);
    block.unmapped.formats = sorted_unique(block.unmapped.formats);
    block.unmapped.descriptions = sorted_unique(block.unmapped.descriptions);
    block
}

/// Map one medium onto an item, or onto nothing.
///
/// A medium that states no format at all is a real but unidentified medium — the release
/// genuinely has a disc there — so it becomes an `other_unspecified` item rather than an
/// unmapped value. A format name the vocabulary does not know is the opposite case: it is
/// recorded under `unmapped.formats` and produces no item, because guessing a family from an
/// unknown name is exactly the per-consumer divergence this block exists to end.
fn map_medium(block: &mut MediaBlock, medium: &Map<String, Value>, taxonomy: &Taxonomy) -> Option<MediaItem> {
    let name = medium.get("format").and_then(Value::as_str).filter(|name| !name.is_empty()).map(str::to_string);
    let mut item = MediaItem::new(name.clone(), Vec::new(), None);
    item.position = medium.get("position").and_then(Value::as_i64);
    item.track_count = medium.get("track_count").and_then(Value::as_i64);

    let Some(name) = name else {
        item.medium = Some("other_unspecified".to_string());
        return Some(item);
    };
    match taxonomy.musicbrainz.formats.get(&name) {
        None => {
            block.unmapped.formats.push(name);
            None
        }
        Some(entry) => apply_format_entry(block, entry, item),
    }
}

/// Route the release-level enumerations that sit beside the medium list.
///
/// Each is a single table lookup: a value the vocabulary knows lands on its one target, and
/// one it does not is preserved under `unmapped.descriptions`. A `status` the vocabulary maps
/// to `null` (`Official`) is known and deliberately contributes nothing.
fn map_release_facts(block: &mut MediaBlock, release: &Value, taxonomy: &Taxonomy) {
    if let Some(status) = release.get("status").and_then(Value::as_str) {
        match taxonomy.musicbrainz.status.get(status) {
            None => block.unmapped.descriptions.push(status.to_string()),
            Some(None) => {}
            Some(Some(edition)) => block.edition.push(edition.clone()),
        }
    }
    if let Some(packaging) = release.get("packaging").and_then(Value::as_str) {
        match taxonomy.musicbrainz.packaging.get(packaging) {
            None => block.unmapped.descriptions.push(packaging.to_string()),
            Some(mapped) => block.packaging = Some(mapped.clone()),
        }
    }

    let group = release.get("release_group");
    if let Some(primary) = group.and_then(|group| group.get("primary_type")).and_then(Value::as_str) {
        match taxonomy.musicbrainz.primary_types.get(primary) {
            None => block.unmapped.descriptions.push(primary.to_string()),
            Some(kind) => block.release_kind = Some(kind.clone()),
        }
    }
    let secondary_types: &[Value] = group.and_then(|group| group.get("secondary_types")).and_then(Value::as_array).map_or(&[], Vec::as_slice);
    for secondary in secondary_types {
        let Some(secondary) = secondary.as_str() else {
            continue;
        };
        match taxonomy.musicbrainz.secondary_types.get(secondary) {
            None => block.unmapped.descriptions.push(secondary.to_string()),
            Some(release_trait) => block.traits.push(release_trait.clone()),
        }
    }
}

/// Map a MusicBrainz release view onto the canonical media block.
///
/// The view is the shape [`release_view`] builds and the conformance fixtures carry:
/// `media` (the snake_case medium list the parser already emits as `media_raw`), `status`,
/// `packaging`, and `release_group.primary_type` / `release_group.secondary_types`.
///
/// A release with no media at all yields the empty block rather than no block, so every
/// release carries the same shape.
pub fn map_musicbrainz_release(release: &Value) -> MediaBlock {
    let taxonomy = taxonomy();
    let mut block = MediaBlock::empty(taxonomy);
    let media: &[Value] = release.get("media").and_then(Value::as_array).map_or(&[], Vec::as_slice);

    for medium in media {
        // The dump occasionally yields a bare string, number, array, or null where a medium
        // object belongs. Anything that is not an object states nothing this mapper could
        // read, so it is skipped outright rather than becoming an item that claims a medium
        // the release may not have. The JavaScript reference mapper admits an array here,
        // because its guard is `typeof medium === "object"`; that is a defect in the
        // reference rather than the rule, and the Discogs and Python mappers skip it too.
        let Some(medium) = medium.as_object() else {
            continue;
        };
        if let Some(item) = map_medium(&mut block, medium, taxonomy) {
            let finished = finish_item(item, taxonomy);
            block.items.push(finished);
        }
    }

    map_release_facts(&mut block, release, taxonomy);
    finish_block(block)
}

/// Build the mapper's view of a raw MusicBrainz release record.
///
/// The dump hyphenates its release-group keys and nests the medium fields the parser has
/// already normalized into `media_raw`, so this is the one place the two spellings meet.
/// Only the whitelisted values the vocabulary reads are copied; nothing else in the record is
/// touched or published by this module.
pub fn release_view(record: &Value, media_raw: &[Value]) -> Value {
    serde_json::json!({
        "media": media_raw,
        "status": record["status"],
        "packaging": record["packaging"],
        "release_group": {
            "primary_type": record["release-group"]["primary-type"],
            "secondary_types": record["release-group"]["secondary-types"]
        }
    })
}

/// Attach the canonical `media` block to a normalized `releases` record, in place.
///
/// `data` is the record the parser is assembling and `record` the raw dump line it came
/// from. The raw `media_raw` list is left untouched: it stays the provenance record. Callers
/// run this before the content hash, so the hash covers the block.
pub fn attach_media_block(data: &mut Value, record: &Value) {
    let Some(map) = data.as_object_mut() else {
        return;
    };
    let media_raw: Vec<Value> = map.get("media_raw").and_then(Value::as_array).cloned().unwrap_or_default();
    let block = map_musicbrainz_release(&release_view(record, &media_raw));
    let Ok(value) = serde_json::to_value(&block) else {
        return;
    };
    map.insert("media".to_string(), value);
}

#[cfg(test)]
#[path = "tests/media_tests.rs"]
mod tests;
