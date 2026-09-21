use serde_json::{Value, json};

use crate::musicbrainz::identifiers::map_musicbrainz_identifiers;
use crate::musicbrainz::jsonl_parser::parse_mb_release_line;
use crate::types::calculate_content_hash;

#[test]
fn design_conformance() {
    for fixture in ["musicbrainz-barcode-and-catalogue-number.json", "musicbrainz-absent-barcode-and-malformed-label-info.json"] {
        let path = format!("src/musicbrainz/tests/fixtures/identifiers/{fixture}");
        let fixture: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(map_musicbrainz_identifiers(&fixture["input"]), fixture["expected"]);
    }
}

#[test]
fn malformed_and_empty_are_schema_shaped() {
    for input in [
        json!({}),
        json!({"barcode": 42, "label-info": "bad"}),
        json!({"barcode": " ", "label-info": [null, {"catalog-number": 12}]}),
    ] {
        let block = map_musicbrainz_identifiers(&input);
        assert_eq!(block["items"], json!([]));
        assert_eq!(block["types"], json!([]));
        assert_eq!(block["aliases"], json!([]));
        assert_eq!(block["unmapped"]["types"], json!([]));
    }
}

#[test]
fn identifiers_are_hashed_and_raw_fields_survive() {
    let release = json!({"id":"release", "title":"Title", "barcode":"07599251581", "label-info":[{"catalog-number":"1-25158"}], "relations":[]});
    let original = parse_mb_release_line(&release.to_string()).unwrap();
    assert_eq!(original.data["barcode"], "07599251581");
    assert_eq!(original.data["catalog_numbers"][0]["catalog_number"], "1-25158");
    assert_eq!(original.sha256, calculate_content_hash(&original.data));
    let mut changed = release.clone();
    changed["barcode"] = json!("07599251582");
    let changed = parse_mb_release_line(&changed.to_string()).unwrap();
    assert_ne!(original.sha256, changed.sha256);
    assert_ne!(original.data["identifiers"], changed.data["identifiers"]);
    let mut block_changed = original.data.clone();
    block_changed["identifiers"]["identifiers_version"] = json!("2");
    assert_ne!(original.sha256, calculate_content_hash(&block_changed));
}
