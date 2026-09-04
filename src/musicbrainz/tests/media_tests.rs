//! Canonical media block (ADR 0007) mapper tests.
//!
//! The conformance suite in `fixtures/media/` is vendored verbatim from the design
//! repository's `taxonomy/media/v1/fixtures/`. Those input/expected pairs are the contract
//! between this producer, the Discogs producer, and the shared Python mapper: all three must
//! reproduce the design repository's reference mapper exactly. Never edit a fixture to make
//! this code pass — re-vendor the suite when the vocabulary changes.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::musicbrainz::jsonl_parser::parse_mb_release_line;
use crate::musicbrainz::media::{attach_media_block, map_musicbrainz_release, release_view};
use crate::types::calculate_content_hash;

/// The vendored conformance suite: 19 pairs at the pinned design commit, of which the seven
/// `musicbrainz-*` ones exercise this producer. The 12 `discogs-*` pairs are carried
/// unchanged so the vendored suite matches the pinned upstream set file for file; the Discogs
/// producer owns their mapper.
const FIXTURE_TOTAL: usize = 19;
const MUSICBRAINZ_FIXTURE_TOTAL: usize = 7;

fn fixture_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src/musicbrainz/tests/fixtures/media")
}

fn fixtures() -> Vec<(String, Value)> {
    let mut loaded: Vec<(String, Value)> = fs::read_dir(fixture_directory())
        .expect("the vendored media fixture directory is readable")
        .map(|entry| entry.expect("the fixture directory entry is readable").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "json"))
        .map(|path| {
            let name = path.file_name().expect("the fixture has a file name").to_string_lossy().to_string();
            let text = fs::read_to_string(&path).expect("the fixture is readable");
            let value: Value = serde_json::from_str(&text).unwrap_or_else(|error| panic!("{name} is valid JSON: {error}"));
            (name, value)
        })
        .collect();
    loaded.sort_by(|left, right| left.0.cmp(&right.0));
    loaded
}

/// Map a release view the way the fixtures express it.
fn map(release: &Value) -> Value {
    serde_json::to_value(map_musicbrainz_release(release)).expect("the media block serializes")
}

/// A release view carrying one medium and nothing else, for the medium-level cases.
fn one_medium(medium: Value) -> Value {
    json!({"media": [medium]})
}

// ── Conformance ─────────────────────────────────────────────────────

/// Guard the vendored suite itself: a fixture silently lost or added would otherwise make
/// the conformance test below vacuously weaker.
#[test]
fn test_vendored_fixture_suite_is_complete() {
    let all = fixtures();
    let musicbrainz = all.iter().filter(|(_, fixture)| fixture["provider"] == json!("musicbrainz")).count();
    assert_eq!(all.len(), FIXTURE_TOTAL, "the vendored suite must match the pinned design commit file for file");
    assert_eq!(musicbrainz, MUSICBRAINZ_FIXTURE_TOTAL, "every musicbrainz fixture must be present");
}

/// Every MusicBrainz conformance pair: run the fixture's raw input through the mapper and
/// demand the exact block the design repository's reference mapper produces, field for field.
#[test]
fn test_musicbrainz_conformance_fixtures() {
    let mut checked = 0;
    for (name, fixture) in fixtures() {
        if fixture["provider"] != json!("musicbrainz") {
            continue;
        }
        let produced = map(&fixture["input"]);
        let expected = &fixture["expected"];
        assert_eq!(
            &produced,
            expected,
            "fixture {name} does not match the reference mapper\n  produced: {}\n  expected: {}",
            serde_json::to_string_pretty(&produced).unwrap_or_default(),
            serde_json::to_string_pretty(expected).unwrap_or_default()
        );
        checked += 1;
    }
    assert_eq!(checked, MUSICBRAINZ_FIXTURE_TOTAL, "every musicbrainz fixture must be exercised");
}

// ── Shape guarantees ────────────────────────────────────────────────

/// A release the dump gave no media still carries the block, empty — never a missing key, so
/// no consumer has to branch on its absence.
#[test]
fn test_absent_media_produce_the_empty_block() {
    let produced = map(&json!({}));
    assert_eq!(
        produced,
        json!({
            "taxonomy_version": "1",
            "items": [],
            "families": [],
            "release_kind": null,
            "traits": [],
            "edition": [],
            "packaging": null,
            "container": null,
            "flags": [],
            "unmapped": {"formats": [], "descriptions": []}
        })
    );
}

/// A `media` value that is not a list is treated as no media rather than panicking.
#[test]
fn test_malformed_media_produce_no_items() {
    for media in [json!(null), json!("CD"), json!(7), json!({"medium": []})] {
        let produced = map(&json!({"media": media}));
        assert_eq!(produced["items"], json!([]), "{media} must yield no items");
        assert_eq!(produced["families"], json!([]), "{media} must yield no families");
    }
}

/// A medium entry that is not a JSON object states nothing this mapper could read, so it is
/// skipped outright: no item, and nothing recorded under `unmapped`. An array is skipped like
/// any other non-object, which is what separates it from a medium object that merely omits
/// its format — that one is a real medium and does become an item.
#[test]
fn test_a_non_object_medium_entry_is_skipped() {
    let produced = map(&json!({"media": [null, "CD", 7, true, [], ["CD"], {"format": "CD", "position": 1, "track_count": 3}]}));
    assert_eq!(produced["items"].as_array().expect("items is a list").len(), 1, "only the real medium survives");
    assert_eq!(produced["items"][0]["medium"], json!("optical_cd"));
    assert_eq!(produced["families"], json!(["optical"]));
    assert_eq!(produced["unmapped"], json!({"formats": [], "descriptions": []}), "a skipped entry is not an unmapped value");
}

/// Every field is present with an explicit null or empty list, so two implementations
/// serialize the same bytes.
#[test]
fn test_every_field_is_always_present() {
    let produced = map(&one_medium(json!({"format": "CD", "position": 1, "track_count": 12})));
    let block = produced.as_object().expect("the block is an object");
    for key in [
        "taxonomy_version",
        "items",
        "families",
        "release_kind",
        "traits",
        "edition",
        "packaging",
        "container",
        "flags",
        "unmapped",
    ] {
        assert!(block.contains_key(key), "the block must carry {key}");
    }
    let item = produced["items"][0].as_object().expect("the item is an object");
    for key in [
        "family",
        "medium",
        "qty",
        "size_inches",
        "speed_rpm",
        "channels",
        "codec",
        "variants",
        "appearance",
        "position",
        "track_count",
        "source",
    ] {
        assert!(item.contains_key(key), "the item must carry {key}");
    }
    let source = produced["items"][0]["source"].as_object().expect("the source is an object");
    for key in ["provider", "name", "descriptions", "text"] {
        assert!(source.contains_key(key), "the source must carry {key}");
    }
}

/// MusicBrainz never states a quantity, free text, or a description list, so every item is
/// one unit sourced from a bare format name.
#[test]
fn test_every_item_is_one_unit_from_a_bare_name() {
    let produced = map(&one_medium(json!({"format": "Cassette", "position": 1, "track_count": 10})));
    assert_eq!(produced["items"][0]["qty"], json!(1));
    assert!(produced["items"][0]["qty"].is_u64(), "qty is a JSON number");
    assert_eq!(produced["items"][0]["source"], json!({"provider": "musicbrainz", "name": "Cassette", "descriptions": [], "text": null}));
}

// ── Format-name routing ─────────────────────────────────────────────

/// The `Vinyl` parent names a family and no medium, and MusicBrainz never states a size, so
/// the family's `size_inches` resolve rule finds nothing and the item lands on the
/// unspecified vinyl medium rather than guessing a pressing size.
#[test]
fn test_parent_vinyl_resolves_to_the_unspecified_medium() {
    let produced = map(&one_medium(json!({"format": "Vinyl", "position": 1, "track_count": 4})));
    assert_eq!(produced["items"][0]["family"], json!("vinyl"));
    assert_eq!(produced["items"][0]["medium"], json!("vinyl_unspecified"));
    assert_eq!(produced["items"][0]["size_inches"], json!(null), "no size is invented for the parent");
    assert_eq!(produced["families"], json!(["vinyl"]));
}

/// The sized vinyl children name a medium outright and inherit its default size.
#[test]
fn test_sized_vinyl_children_carry_the_medium_default_size() {
    for (format, medium, size) in [("7\" Vinyl", "vinyl_7", 7), ("10\" Vinyl", "vinyl_10", 10), ("12\" Vinyl", "vinyl_12", 12)] {
        let produced = map(&one_medium(json!({"format": format, "position": 1, "track_count": 2})));
        assert_eq!(produced["items"][0]["medium"], json!(medium), "{format} maps to {medium}");
        assert_eq!(produced["items"][0]["size_inches"], json!(size), "{format} takes its size from the medium default");
    }
}

/// The `CD` and `Cassette` parents are not ambiguous: they name a medium directly, so unlike
/// `Vinyl` they never fall through to an unspecified medium.
#[test]
fn test_unambiguous_parents_map_straight_to_a_medium() {
    for (format, family, medium) in [("CD", "optical", "optical_cd"), ("Cassette", "tape", "tape_cassette")] {
        let produced = map(&one_medium(json!({"format": format, "position": 1, "track_count": 1})));
        assert_eq!(produced["items"][0]["family"], json!(family));
        assert_eq!(produced["items"][0]["medium"], json!(medium));
    }
}

/// `Other` is the `other` family, never digital — the ADR calls this out because the two are
/// easy to conflate and a store that got it wrong would file physical oddities as downloads.
#[test]
fn test_other_maps_to_the_other_family() {
    let produced = map(&one_medium(json!({"format": "Other", "position": 1, "track_count": 3})));
    assert_eq!(produced["items"][0]["family"], json!("other"));
    assert_eq!(produced["items"][0]["medium"], json!("other_unspecified"));
    assert_eq!(produced["families"], json!(["other"]));
}

/// `Digital Media` is the same digital medium as the Discogs `File` format.
#[test]
fn test_digital_media_maps_to_the_digital_file_medium() {
    let produced = map(&one_medium(json!({"format": "Digital Media", "position": 1, "track_count": 14})));
    assert_eq!(produced["items"][0]["family"], json!("digital"));
    assert_eq!(produced["items"][0]["medium"], json!("digital_file"));
    assert_eq!(produced["items"][0]["codec"], json!(null), "MusicBrainz never states a codec");
}

/// A format entry that carries a variant adds it to the item it belongs to.
#[test]
fn test_a_format_entry_variant_lands_on_the_item() {
    let produced = map(&one_medium(json!({"format": "Hybrid SACD", "position": 1, "track_count": 11})));
    assert_eq!(produced["items"][0]["medium"], json!("optical_sacd"));
    assert_eq!(produced["items"][0]["variants"], json!(["hybrid_layer"]));
}

/// A medium with no format is a real but unidentified medium — the release genuinely has a
/// disc there — so it becomes an item, not an unmapped value.
#[test]
fn test_a_missing_format_is_an_unspecified_medium_not_an_unmapped_value() {
    for format in [json!(null), json!(""), Value::Null] {
        let produced = map(&one_medium(json!({"format": format, "position": 2, "track_count": 8})));
        assert_eq!(produced["items"][0]["medium"], json!("other_unspecified"), "format {format} yields an item");
        assert_eq!(produced["items"][0]["family"], json!("other"));
        assert_eq!(produced["items"][0]["source"]["name"], json!(null), "the source name records that none was given");
        assert_eq!(produced["unmapped"]["formats"], json!([]), "a missing format is not an unmapped value");
    }
    let produced = map(&one_medium(json!({"position": 1, "track_count": 5})));
    assert_eq!(produced["items"][0]["medium"], json!("other_unspecified"), "an absent key behaves like an absent format");
}

/// A format name the vocabulary does not know is preserved and produces no item: guessing a
/// family from an unknown name is the per-consumer divergence this block exists to end.
#[test]
fn test_an_unknown_format_lands_in_unmapped_with_no_item() {
    let produced = map(&json!({
        "media": [
            {"format": "Quantum Crystal", "position": 1, "track_count": 1},
            {"format": "Wax Cylinder Mk II", "position": 2, "track_count": 1},
            {"format": "Quantum Crystal", "position": 3, "track_count": 1},
            {"format": "CD", "position": 4, "track_count": 9}
        ]
    }));
    assert_eq!(produced["items"].as_array().expect("items is a list").len(), 1, "only the known format yields an item");
    assert_eq!(produced["items"][0]["medium"], json!("optical_cd"));
    assert_eq!(produced["unmapped"]["formats"], json!(["Quantum Crystal", "Wax Cylinder Mk II"]), "sorted and de-duplicated");
    assert_eq!(produced["families"], json!(["optical"]), "an unmapped format contributes no family");
}

// ── Ordering, position, and track count ─────────────────────────────

/// Items keep source order while `families` is sorted and de-duplicated, and each medium
/// carries its own position and track count.
#[test]
fn test_items_keep_source_order_with_sorted_families() {
    let produced = map(&json!({
        "media": [
            {"format": "Hybrid SACD", "position": 1, "title": "Album", "track_count": 11},
            {"format": "DVD-Video", "position": 2, "title": "Bonus", "track_count": 6},
            {"format": "CD", "position": 3, "title": "", "track_count": 4}
        ]
    }));
    let media: Vec<&Value> = produced["items"].as_array().expect("items is a list").iter().map(|item| &item["medium"]).collect();
    assert_eq!(media, vec![&json!("optical_sacd"), &json!("video_dvd"), &json!("optical_cd")], "source order, not sorted");
    assert_eq!(produced["families"], json!(["optical", "video"]), "families are sorted and de-duplicated");
    assert_eq!(produced["items"][0]["position"], json!(1));
    assert_eq!(produced["items"][1]["position"], json!(2));
    assert_eq!(produced["items"][1]["track_count"], json!(6));
    assert_eq!(produced["items"][2]["track_count"], json!(4));
}

/// The same format twice is two items and one family entry.
#[test]
fn test_a_repeated_format_deduplicates_families_only() {
    let produced = map(&json!({
        "media": [
            {"format": "Vinyl", "position": 1, "track_count": 4},
            {"format": "Vinyl", "position": 2, "track_count": 5}
        ]
    }));
    assert_eq!(produced["items"].as_array().expect("items is a list").len(), 2);
    assert_eq!(produced["families"], json!(["vinyl"]));
}

/// A position or track count the dump omits, nulls, or states as a non-integer stays null
/// rather than being coerced.
#[test]
fn test_a_non_integer_position_or_track_count_is_null() {
    for value in [json!(null), json!("1"), json!(1.5), json!(true)] {
        let produced = map(&one_medium(json!({"format": "CD", "position": value, "track_count": value})));
        assert_eq!(produced["items"][0]["position"], json!(null), "position {value} is not an integer");
        assert_eq!(produced["items"][0]["track_count"], json!(null), "track_count {value} is not an integer");
    }
    let produced = map(&one_medium(json!({"format": "CD"})));
    assert_eq!(produced["items"][0]["position"], json!(null));
    assert_eq!(produced["items"][0]["track_count"], json!(null));
}

// ── Release-level routing ───────────────────────────────────────────

/// A status maps to an edition; `Official` is the unmarked default and deliberately
/// contributes nothing rather than an `official` edition nobody would filter on.
#[test]
fn test_status_routes_to_edition() {
    for (status, edition) in [
        ("Bootleg", "unofficial"),
        ("Promotion", "promo"),
        ("Withdrawn", "withdrawn"),
        ("Cancelled", "cancelled"),
        ("Expunged", "expunged"),
        ("Pseudo-Release", "pseudo_release"),
    ] {
        let produced = map(&json!({"status": status}));
        assert_eq!(produced["edition"], json!([edition]), "{status} maps to {edition}");
        assert_eq!(produced["unmapped"]["descriptions"], json!([]));
    }
    let official = map(&json!({"status": "Official"}));
    assert_eq!(official["edition"], json!([]), "Official is known and adds nothing");
    assert_eq!(official["unmapped"]["descriptions"], json!([]), "Official is not an unmapped value");
}

/// Packaging routes to the single packaging slot.
#[test]
fn test_packaging_routes_to_packaging() {
    for (packaging, mapped) in [
        ("Gatefold Cover", "gatefold"),
        ("Digipak", "digipak"),
        ("Jewel Case", "jewel_case"),
        ("Cardboard/Paper Sleeve", "cardboard_sleeve"),
        ("None", "none"),
        ("Other", "other"),
    ] {
        let produced = map(&json!({"packaging": packaging}));
        assert_eq!(produced["packaging"], json!(mapped), "{packaging} maps to {mapped}");
    }
    let absent = map(&json!({"packaging": null}));
    assert_eq!(absent["packaging"], json!(null), "an absent packaging stays null");
}

/// The release group's primary type is the release kind and its secondary types are traits;
/// they are different targets and never collide.
#[test]
fn test_release_group_types_route_to_kind_and_traits() {
    for (primary, kind) in [("Album", "album"), ("Single", "single"), ("EP", "ep"), ("Broadcast", "broadcast"), ("Other", "other")] {
        let produced = map(&json!({"release_group": {"primary_type": primary, "secondary_types": []}}));
        assert_eq!(produced["release_kind"], json!(kind), "{primary} maps to {kind}");
        assert_eq!(produced["traits"], json!([]));
    }
    let produced = map(&json!({
        "release_group": {"primary_type": "Album", "secondary_types": ["Remix", "Compilation", "Live", "Compilation"]}
    }));
    assert_eq!(produced["release_kind"], json!("album"));
    assert_eq!(produced["traits"], json!(["compilation", "live", "remix"]), "traits are sorted and de-duplicated");
}

/// An unrecognised status, packaging, or release-group type is preserved under
/// `unmapped.descriptions` — the format table is for format names alone.
#[test]
fn test_unknown_release_values_land_in_unmapped_descriptions() {
    let produced = map(&json!({
        "status": "Rumoured",
        "packaging": "Hessian Sack",
        "release_group": {"primary_type": "Novella", "secondary_types": ["Podcast", "Live", "Podcast"]}
    }));
    assert_eq!(produced["unmapped"]["descriptions"], json!(["Hessian Sack", "Novella", "Podcast", "Rumoured"]), "sorted and de-duplicated");
    assert_eq!(produced["edition"], json!([]), "an unknown status sets no edition");
    assert_eq!(produced["packaging"], json!(null), "an unknown packaging sets no packaging");
    assert_eq!(produced["release_kind"], json!(null), "an unknown primary type sets no release kind");
    assert_eq!(produced["traits"], json!(["live"]), "the known secondary type still routes");
    assert_eq!(produced["unmapped"]["formats"], json!([]), "release values never land in unmapped formats");
}

/// The release-level values are read independently of the media list, so a release whose
/// only medium is unmapped still carries its kind, edition, and packaging.
#[test]
fn test_release_facts_survive_an_unmapped_medium() {
    let produced = map(&json!({
        "media": [{"format": "Quantum Crystal", "position": 1, "track_count": 1}],
        "status": "Withdrawn",
        "packaging": "Jewel Case",
        "release_group": {"primary_type": "Single", "secondary_types": ["Demo"]}
    }));
    assert_eq!(produced["items"], json!([]));
    assert_eq!(produced["release_kind"], json!("single"));
    assert_eq!(produced["edition"], json!(["withdrawn"]));
    assert_eq!(produced["packaging"], json!("jewel_case"));
    assert_eq!(produced["traits"], json!(["demo"]));
    assert_eq!(produced["unmapped"]["formats"], json!(["Quantum Crystal"]));
}

/// Non-string release values and a null release group are ignored rather than coerced.
#[test]
fn test_non_string_release_values_are_ignored() {
    let produced = map(&json!({
        "status": 7,
        "packaging": ["Digipak"],
        "release_group": null
    }));
    assert_eq!(produced["edition"], json!([]));
    assert_eq!(produced["packaging"], json!(null));
    assert_eq!(produced["release_kind"], json!(null));
    assert_eq!(produced["unmapped"]["descriptions"], json!([]), "a non-string is not a value the vocabulary could have known");

    let produced = map(&json!({"release_group": {"primary_type": "Album", "secondary_types": ["Live", 7, null]}}));
    assert_eq!(produced["traits"], json!(["live"]), "non-string secondary types are skipped");
}

// ── The dump-record view ────────────────────────────────────────────

/// `release_view` is the one place the dump's hyphenated release-group keys meet the
/// snake_case shape the mapper and the fixtures use.
#[test]
fn test_release_view_translates_the_dump_spelling() {
    let record = json!({
        "id": "a-release",
        "title": "A Release",
        "status": "Official",
        "packaging": "Digipak",
        "release-group": {"id": "a-group", "primary-type": "Album", "secondary-types": ["Live"]},
        "media": [{"format": "CD", "track-count": 9}]
    });
    let media_raw = vec![json!({"format": "CD", "format_id": null, "position": 1, "title": "", "track_count": 9})];
    let view = release_view(&record, &media_raw);
    assert_eq!(
        view,
        json!({
            "media": [{"format": "CD", "format_id": null, "position": 1, "title": "", "track_count": 9}],
            "status": "Official",
            "packaging": "Digipak",
            "release_group": {"primary_type": "Album", "secondary_types": ["Live"]}
        }),
        "only the whitelisted values the vocabulary reads are copied"
    );
}

/// A record missing every release-level key still yields a usable view of nulls.
#[test]
fn test_release_view_of_a_bare_record() {
    let view = release_view(&json!({"id": "a-release"}), &[]);
    assert_eq!(
        view,
        json!({
            "media": [],
            "status": null,
            "packaging": null,
            "release_group": {"primary_type": null, "secondary_types": null}
        })
    );
    assert_eq!(map(&view)["items"], json!([]));
}

// ── Attachment at the producer boundary ─────────────────────────────

#[test]
fn test_attach_media_block_leaves_the_raw_medium_list_untouched() {
    let media_raw = json!([{"format": "12\" Vinyl", "format_id": "7a", "position": 1, "title": "", "track_count": 9}]);
    let record = json!({"status": "Promotion", "packaging": "Gatefold Cover", "release-group": {"primary-type": "Album"}});
    let mut data = json!({"name": "A Release", "status": "Promotion", "media_raw": media_raw.clone()});

    attach_media_block(&mut data, &record);

    assert_eq!(data["media_raw"], media_raw, "the raw provider field stays the provenance record");
    assert_eq!(data["media"]["items"][0]["medium"], json!("vinyl_12"));
    assert_eq!(data["media"]["items"][0]["size_inches"], json!(12));
    assert_eq!(data["media"]["edition"], json!(["promo"]));
    assert_eq!(data["media"]["packaging"], json!("gatefold"));
    assert_eq!(data["media"]["release_kind"], json!("album"));
    assert_eq!(data["name"], json!("A Release"), "no other field is disturbed");
}

#[test]
fn test_attach_media_block_on_a_record_without_media() {
    let mut data = json!({"name": "A Release"});
    attach_media_block(&mut data, &json!({}));
    assert_eq!(data["media"]["items"], json!([]));
    assert_eq!(data["media"]["families"], json!([]));
    assert_eq!(data["media"]["taxonomy_version"], json!("1"));
}

#[test]
fn test_attach_media_block_ignores_a_non_object_record() {
    let mut data = json!(["not", "a", "record"]);
    attach_media_block(&mut data, &json!({}));
    assert_eq!(data, json!(["not", "a", "record"]));
}

// ── The block travels through the parser, inside the hash ───────────

/// A dump line shaped the way MusicBrainz emits one, with the release-group types and the
/// hyphenated medium keys the parser normalizes.
fn release_line(format: &str, status: &str) -> String {
    json!({
        "id": "3f2a0d0e-0000-4000-8000-000000000001",
        "title": "A Release",
        "status": status,
        "packaging": "Digipak",
        "release-group": {
            "id": "3f2a0d0e-0000-4000-8000-000000000002",
            "primary-type": "Album",
            "secondary-types": ["Live", "Compilation"]
        },
        "media": [{"format": format, "format-id": "7a", "position": 1, "title": "Disc 1", "track-count": 9}]
    })
    .to_string()
}

/// The parser attaches the block to the release it assembles, mapping the raw medium list it
/// already emits plus the release-level values it reads straight from the dump line.
#[test]
fn test_parser_attaches_the_block_to_releases() {
    let message = parse_mb_release_line(&release_line("12\" Vinyl", "Promotion")).expect("the release line parses");

    assert_eq!(message.data["media"]["items"][0]["medium"], json!("vinyl_12"));
    assert_eq!(message.data["media"]["items"][0]["position"], json!(1));
    assert_eq!(message.data["media"]["items"][0]["track_count"], json!(9));
    assert_eq!(message.data["media"]["families"], json!(["vinyl"]));
    assert_eq!(message.data["media"]["release_kind"], json!("album"));
    assert_eq!(message.data["media"]["traits"], json!(["compilation", "live"]));
    assert_eq!(message.data["media"]["edition"], json!(["promo"]));
    assert_eq!(message.data["media"]["packaging"], json!("digipak"));
    assert_eq!(message.data["media_raw"][0]["format"], json!("12\" Vinyl"), "the raw medium list survives");
    assert_eq!(message.data["media_raw"][0]["format_id"], json!("7a"));
    assert!(!message.sha256.is_empty(), "the parser populates the content hash");
}

/// The block is attached before the hash, so a change the vocabulary sees still changes the
/// content hash consumers key change detection on.
#[test]
fn test_hash_covers_the_media_block() {
    let vinyl = parse_mb_release_line(&release_line("12\" Vinyl", "Official")).expect("the release line parses");
    let compact = parse_mb_release_line(&release_line("CD", "Official")).expect("the release line parses");
    let vinyl_again = parse_mb_release_line(&release_line("12\" Vinyl", "Official")).expect("the release line parses");

    assert_ne!(vinyl.sha256, compact.sha256, "a different resolved medium must change the hash");
    assert_eq!(vinyl.sha256, vinyl_again.sha256, "the same record must hash the same");

    let mut without_block = vinyl.data.clone();
    without_block.as_object_mut().expect("the record is an object").remove("media");
    assert_ne!(vinyl.sha256, calculate_content_hash(&without_block), "the hash must cover the media block");
}

/// A release-level value the raw record already carried still moves the hash through the
/// block: `status` travels twice, raw and mapped, and both are inside the hash.
#[test]
fn test_hash_covers_a_release_level_change() {
    let official = parse_mb_release_line(&release_line("CD", "Official")).expect("the release line parses");
    let bootleg = parse_mb_release_line(&release_line("CD", "Bootleg")).expect("the release line parses");

    assert_eq!(official.data["media"]["edition"], json!([]));
    assert_eq!(bootleg.data["media"]["edition"], json!(["unofficial"]));
    assert_ne!(official.sha256, bootleg.sha256);
}

// ── Contract fixture parity ─────────────────────────────────────────

/// The `releases` contract fixture (`contracts/catalog-events/definitions/musicbrainz.json`)
/// carries the raw `media_raw` and `status` a consumer would receive, plus the `media` block
/// this producer is expected to compute from them. The fixture is hand-authored JSON, not
/// generated from this mapper, so nothing else stops it drifting from `media.rs` as the
/// vocabulary or the mapper evolve. Recompute the block from the fixture's own inputs and
/// demand the fixture still matches, so a drift fails `cargo test` instead of surfacing only
/// downstream in a consumer.
#[test]
fn test_contract_fixture_media_matches_the_mapper() {
    let definitions_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("contracts/catalog-events/definitions/musicbrainz.json");
    let text = fs::read_to_string(&definitions_path).expect("the MusicBrainz contract definition is readable");
    let definitions: Value = serde_json::from_str(&text).expect("the contract definition is valid JSON");
    let releases = &definitions["fixture_payloads"]["releases"];

    let media_raw = releases["media_raw"].as_array().expect("the releases fixture carries media_raw").clone();
    let record = json!({"status": releases["status"]});
    let produced = map(&release_view(&record, &media_raw));

    assert_eq!(
        &produced,
        &releases["media"],
        "contracts/catalog-events/definitions/musicbrainz.json fixture_payloads.releases.media has drifted \
         from src/musicbrainz/media.rs -- regenerate it from the mapper, don't hand-edit it back into sync\n  \
         mapper produced: {}\n  fixture carries:  {}",
        serde_json::to_string_pretty(&produced).unwrap_or_default(),
        serde_json::to_string_pretty(&releases["media"]).unwrap_or_default()
    );
}
