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
