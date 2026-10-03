# Changelog

All notable changes to this repository will be recorded here by Commitizen from
Conventional Commits.

## v0.3.0 (2026-10-03)

### Feat

- **catalog**: normalize MusicBrainz release identifiers before hashing
- **contracts**: carry country, release events, and catalogue numbers in release fixtures and document them
- **parser**: carry country, release events, and catalogue numbers in release events
- **telemetry**: port the wave-2 runtime metrics and tracing module

### Fix

- **logging**: include deployment environment in JSON events
- **deps**: update rustls to clear RUSTSEC-2026-0285
- **deps**: refresh rust:1.98-slim digest and pin Python 3.14.7 (#3)
- **ci**: accept commitizen's no-eligible-commits bump-preview state

### Refactor

- **runtime**: remove dormant Discogs seams
- **runtime**: consolidate ingestion lifecycle seams

### Perf

- **build**: cache rustc output with sccache

## v0.2.1 (2026-09-04)

### Fix

- **image**: copy the vendored media taxonomy into the builder stage

## v0.2.0 (2026-09-04)

### Feat

- **contracts**: carry raw mediums and the media block in release fixtures and document them
- **media**: add the Rust media mapper and attach the canonical media block
- **contracts**: vendor the media taxonomy with a digest check
- **parser**: keep raw MusicBrainz mediums in release events
- **telemetry**: export OTLP metrics from the extraction pipeline

### Fix

- **parser**: compute the content hash for every MusicBrainz event
- **musicbrainz**: invalidate Docker library cache
- **musicbrainz**: seed Docker library target
- **musicbrainz**: validate provider repository metadata
- **toolchain**: bind pinned tools through mise
- **split**: freeze provider compatibility baseline

### Refactor

- **musicbrainz**: make repository provider-exclusive
- **musicbrainz**: make repository provider-exclusive
- **contracts**: generate source-owned v1 exports
- **runtime**: partition provider-owned modules

## v0.1.1 (2026-08-31)

### Fix

- **release**: use supported files-only flag
- **ci**: accept release-boundary bump states

## v0.1.0 (2026-08-31)

The `v0.1.0` workflow failed before publishing artifacts or images. The tag is
retained as an immutable record of that release attempt.
