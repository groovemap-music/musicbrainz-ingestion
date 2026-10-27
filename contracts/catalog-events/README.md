# MusicBrainz catalog event contract

This repository owns the MusicBrainz `groovemap.catalog-events/v1` producer contract.
The maintained source is `definitions/musicbrainz.json`; `v1/` contains the generated
manifest, Python binding, JSON fixtures, and shared event schema.

```bash
just contract
just contract-check
```

The contract preserves the `groovemap-musicbrainz` exchange prefix and the artists,
labels, release-groups, and releases entity vocabulary. Consumer repositories promote
artifacts from a reviewed immutable commit rather than editing generated output.

## Data message `dump_version`

Every `data` message carries `dump_version`, a non-empty string naming the MusicBrainz dump
version the record was extracted from. It is exactly the `version` the same extraction's
`extraction_complete` message carries, so a consumer can attribute each record to its dump
when it processes the record instead of waiting for the completion signal, which arrives
after all of an extraction's records.

It is additive within contract v1: `data` events already allow additional properties, a
consumer built before it existed ignores it, and `schemas/event.schema.json` declares it
without making it required. A message published before this field existed has none, so a
consumer that depends on it must handle its absence. The producer sets it on every entity
kind, and it stays outside the `sha256` content hash, so an unchanged record keeps its hash
from one dump to the next. `just contract` writes the field name into the generated
bindings (`DUMP_VERSION_FIELD`), and `just contract-check` fails if the schema stops
declaring it. `definitions/musicbrainz.json`'s `fixture_dump_version` is the version the
generated data fixtures and the `extraction-complete` fixture share.

## Release media fields

`releases` events carry two media fields, both additive within contract v1 -- a consumer
built against the v1 schema before either existed keeps working unchanged:

- `media_raw`: the release's mediums exactly as MusicBrainz sent them, one entry per
  medium with `format`, `format_id`, `position`, `title`, and `track_count`. This is the
  provenance record; nothing here is reshaped or dropped.
- `media`: the canonical media block ([ADR 0007, "Canonical media taxonomy and
  media-neutral product core"][adr-0007]), computed from `media_raw` plus the release
  `status`, `packaging`, and release-group primary and secondary types against the
  vendored media vocabulary (see "Vendored media taxonomy" below). Every consumer reads
  the same shape instead of re-deriving one from the raw fields itself.

`definitions/musicbrainz.json`'s `fixture_payloads.releases` carries a worked example of
both fields, and a test in `src/musicbrainz/tests/media_tests.rs` recomputes the `media`
block from that fixture's own `media_raw` and `status` and asserts it against the fixture
so the two cannot drift apart. See `docs/extraction.md` for the producer-side mapping and
`src/musicbrainz/media.rs` for the mapper itself.

[adr-0007]: https://github.com/groovemap-music/design/blob/main/docs/adr/0007-canonical-media-taxonomy.md

## Release identifier fields

`releases` events also carry four catalog-identifier fields, additive within contract
v1 (catalog-identifiers program, wave 1, [ADR 0011, "Catalog identifiers, manufacturing
credits, and release country"][adr-0011]):

- `country`: the release's ISO country code from the dump, or `null` when absent.
- `release_events`: `[{date, area_name, area_mbid}]` from the dump's raw
  `release-events`; an entry with neither a date nor an area is dropped.
- `catalog_numbers`: `[{catalog_number, label_mbid, label_name}]` from the dump's raw
  `label-info`; an entry without a catalogue number is dropped.
- `identifiers`: the canonical identifier block, mapped from the release's `barcode` and
  each `label-info` catalogue number against the vendored identifier vocabulary (see
  "Vendored identifier vocabulary" below). It carries the source-order `items` (canonical
  `type`, the value as received, and the `source.field` it came from), the sorted `types`
  and `aliases` the release mints, and `unmapped.types`. A release with neither a
  barcode nor a catalogue number still carries the block, with empty lists.

`country`, `release_events`, and `catalog_numbers` carry the whitelisted release-events
and label-info fields verbatim; none of the four surfaces manufacturing relations, which
stay in the generic `relations` array. See `definitions/musicbrainz.json`'s
`fixture_payloads.releases` for a worked example, `docs/extraction.md` for the
producer-side mapping, and `src/musicbrainz/identifiers.rs` for the identifier mapper.

[adr-0011]: https://github.com/groovemap-music/design/blob/main/docs/adr/0011-catalog-identifiers-and-manufacturing-credits.md

## Vendored media taxonomy

`vocab/media-taxonomy.json` is **not generated**. It is the provider-neutral media
vocabulary owned by the `design` repository at `taxonomy/media/v1/media-taxonomy.json`
(see that repository's `taxonomy/media/README.md` and [ADR 0007, "Canonical media
taxonomy and media-neutral product core"][adr-0007]), vendored into this repository
verbatim, byte for byte. `vocab/source.json` beside it records the design commit and
SHA-256 digest the copy was vendored from:

```json
{
  "source": "design",
  "path": "taxonomy/media/v1/media-taxonomy.json",
  "commit": "<40-char design repo commit sha>",
  "sha256": "<sha256 of the vendored file>"
}
```

`just contract-check` fails if `vocab/media-taxonomy.json` is missing or its digest no
longer matches `vocab/source.json`. `just contract` never writes or rewrites either file
-- both are edited only by hand, as part of a deliberate re-vendor.

### Updating the vendored copy

1. In the design repository, pick the reviewed commit that should become the new source
   and confirm the taxonomy file's digest:

   ```bash
   git -C <path-to-design-repo> rev-parse <commit>
   shasum -a 256 <path-to-design-repo>/taxonomy/media/v1/media-taxonomy.json
   ```

2. Copy the file byte for byte into this repository:

   ```bash
   cp <path-to-design-repo>/taxonomy/media/v1/media-taxonomy.json \
      contracts/catalog-events/vocab/media-taxonomy.json
   ```

3. Update `vocab/source.json` with the new commit sha (full 40 characters) and digest.
4. Run `just check` (or at least `just contract-check`) to confirm the vendored copy and
   the source record agree.

Never write an absolute host path into `source.json` or any tracked file -- only the
commit sha, the design-repo-relative path, and the digest.

## Vendored identifier vocabulary

`vocab/identifier-types.json`, `vocab/identifier-types.schema.json`, and
`vocab/identifier-block.schema.json` are likewise **not generated**: they are copied byte
for byte from the `design` repository's `taxonomy/identifiers/v1/` ([ADR 0011][adr-0011]).
`src/musicbrainz/identifiers.rs` compiles in `identifier-types.json`. Their source record
is `vocab/identifiers-source.json`, which names one design commit plus each file's
design-repo-relative path and SHA-256 digest.

`just contract-check` fails if a recorded file's digest no longer matches, or if the
recorded commit differs from the reviewed design commit pinned in `contracts/generate.py`.
A re-vendor therefore follows the steps above for each file, updates its entry and the
shared commit in `vocab/identifiers-source.json`, and moves that pin in the same change.
