# Repository instructions

- Preserve `musicbrainz-ingestion` as the owner of catalog event schemas, generated bindings,
  fixtures, extraction rules, and producer behavior.
- Run `just check` before proposing changes. Run the separate `just audit`, `just image`,
  and `just release-dry-run` gates when dependency, image, or release behavior changes.
- Regenerate contracts with `just contract`; never edit generated bindings directly or
  write generated files into consumer repositories.
- Do not commit credentials, data dumps, state markers, build output, private keys, or
  generated authentication files.
- Do not publish crates, images, packages, tags, or releases without explicit approval.

