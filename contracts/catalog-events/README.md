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

## Vendored media taxonomy

`vocab/media-taxonomy.json` is **not generated**. It is the provider-neutral media
vocabulary owned by the `design` repository at `taxonomy/media/v1/media-taxonomy.json`
(see that repository's `taxonomy/media/README.md` and ADR 0007, "Canonical media
taxonomy and media-neutral product core"), vendored into this repository verbatim, byte
for byte. `vocab/source.json` beside it records the design commit and SHA-256 digest the
copy was vendored from:

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
