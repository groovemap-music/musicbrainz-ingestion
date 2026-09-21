//! Canonical MusicBrainz release identifiers (ADR 0011).

use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

use serde::Deserialize;
use serde_json::{Value, json};

const VOCABULARY_JSON: &str = include_str!("../../contracts/catalog-events/vocab/identifier-types.json");

#[derive(Deserialize)]
struct Vocabulary {
    vocabulary_version: String,
    unmapped_type: String,
    alias_namespaces: Vec<Namespace>,
    musicbrainz: MusicBrainz,
}

#[derive(Deserialize)]
struct Namespace {
    provider: String,
    #[serde(rename = "type")]
    type_id: String,
    normalization: String,
}

#[derive(Deserialize)]
struct MusicBrainz {
    barcode_field: String,
    catalog_number_field: String,
    types: HashMap<String, String>,
}

fn vocabulary() -> &'static Vocabulary {
    static VOCABULARY: OnceLock<Vocabulary> = OnceLock::new();
    VOCABULARY.get_or_init(|| serde_json::from_str(VOCABULARY_JSON).expect("vendored identifier vocabulary is valid"))
}

fn trimmed(value: Option<&Value>) -> Option<&str> {
    let value = value?.as_str()?.trim();
    (!value.is_empty()).then_some(value)
}

fn alias_value(rule: &str, value: &str) -> Option<String> {
    let value = match rule {
        "digits_only" => value.chars().filter(char::is_ascii_digit).collect(),
        "collapse_space" => value.split_whitespace().collect::<Vec<_>>().join(" "),
        "upper_collapse_space" => value.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase(),
        _ => return None,
    };
    (!value.is_empty()).then_some(value)
}

/// Map the original MusicBrainz fields, retaining source order and the received values.
pub fn map_musicbrainz_identifiers(release: &Value) -> Value {
    let vocab = vocabulary();
    let mut items = Vec::new();
    let mut types = BTreeSet::new();
    let mut aliases = BTreeSet::new();
    let mut unmapped = BTreeSet::new();

    let mut add = |raw_type: &str, value: &str, field: &str| {
        let type_id = vocab.musicbrainz.types.get(raw_type).map(String::as_str).unwrap_or_else(|| {
            unmapped.insert(raw_type.to_string());
            &vocab.unmapped_type
        });
        types.insert(type_id.to_string());
        items.push(json!({
            "type": type_id, "value": value, "description": null,
            "source": {"provider": "musicbrainz", "type": null, "field": field}
        }));
        for namespace in vocab.alias_namespaces.iter().filter(|namespace| namespace.type_id == type_id) {
            if let Some(external_id) = alias_value(&namespace.normalization, value) {
                aliases.insert((namespace.provider.clone(), external_id));
            }
        }
    };

    if let Some(value) = trimmed(release.get("barcode")) {
        add("barcode", value, &vocab.musicbrainz.barcode_field);
    }
    if let Some(entries) = release.get("label-info").and_then(Value::as_array) {
        for entry in entries {
            if let Some(value) = trimmed(entry.get("catalog-number")) {
                add("catalog-number", value, &vocab.musicbrainz.catalog_number_field);
            }
        }
    }

    json!({
        "identifiers_version": vocab.vocabulary_version,
        "items": items,
        "types": types.into_iter().collect::<Vec<_>>(),
        "aliases": aliases.into_iter().map(|(provider, external_id)| json!({"provider": provider, "external_id": external_id})).collect::<Vec<_>>(),
        "unmapped": {"types": unmapped.into_iter().collect::<Vec<_>>()}
    })
}

pub fn attach_identifiers_block(data: &mut Value, release: &Value) {
    if let Some(data) = data.as_object_mut() {
        data.insert("identifiers".to_string(), map_musicbrainz_identifiers(release));
    }
}

#[cfg(test)]
#[path = "tests/identifiers_tests.rs"]
mod tests;
