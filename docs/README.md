# MusicBrainz ingestion documentation

- [Extraction architecture](extraction.md) — download, archive, JSONL, enrichment,
  publication, and independent scheduling behavior.
- [State-marker system](state-marker-system.md) — restart, durability, and checksum provenance.
- [Periodic state-marker checkpoints](state-marker-periodic-updates.md) — recovery guarantees.
- [Runtime identity](runtime-identity.md) — repository, image, service, and RabbitMQ names.
- [Publication readiness](publication-readiness.md) — release-history and approval gates.
- [Catalog event contract](../contracts/catalog-events/README.md) — generated artifacts.

The Discogs producer is maintained independently in `groovemap-music/discogs-ingestion`.

## Automation recipes

The `Justfile` follows the GrooveMap capability contract. `setup`, `check`, `format`,
`format-check`, `lint`, `test`, `coverage`, `audit`, `license-check`, `secret-scan`,
`build`, `install-check`, `image`, `bump-preview`, `bump`, and `release-dry-run` retain
their shared meanings. `contract`, `contract-check`, `repository-check`,
`repository-tests`, `build-check`, `cyclonedx`, and `bootstrap` are focused helpers used
by those capabilities. `source-characterization` is the MusicBrainz-specific parser
characterization suite.

The old `publication-history-test` alias was removed because `repository-tests` already
runs the same test module. The old `history-rehearsal` alias was also removed: publication
history was a one-time repository sanitization boundary, not an ongoing build capability.
Its scripts, tests, and [historical readiness record](publication-readiness.md) remain for
auditability. Dependency auditing, local image construction, contract verification, and
release rehearsal remain explicit supported gates. `check` stays credential-free and
does not publish, contact a live service, build an image, or perform a history rewrite.
