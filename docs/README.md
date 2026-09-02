# MusicBrainz ingestion documentation

- [Extraction architecture](extraction.md) — download, archive, JSONL, enrichment,
  publication, and independent scheduling behavior.
- [State-marker system](state-marker-system.md) — restart, durability, and checksum provenance.
- [Periodic state-marker checkpoints](state-marker-periodic-updates.md) — recovery guarantees.
- [Runtime identity](runtime-identity.md) — repository, image, service, and RabbitMQ names.
- [Publication readiness](publication-readiness.md) — release-history and approval gates.
- [Catalog event contract](../contracts/catalog-events/README.md) — generated artifacts.

The Discogs producer is maintained independently in `groovemap-music/discogs-ingestion`.
