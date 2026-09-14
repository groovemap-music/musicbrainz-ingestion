# Downloader reliability contract

The MusicBrainz downloader owns these observable acquisition guarantees. The focused Rust
contract suite in `src/musicbrainz/tests/downloader_reliability_contract_tests.rs` exercises
them without contacting MusicBrainz or requiring a live dump.

| Invariant | MusicBrainz contract |
| --- | --- |
| Timeout | Connection setup and every idle body-read interval are bounded to 120 seconds. There is deliberately no total deadline for a multi-gigabyte transfer that continues making progress. |
| Retry and backoff | A post-connect download failure receives at most three attempts, with exponential waits of 2 seconds then 4 seconds in production. HTTP 429/503 handling remains inside the polite HTTP client and does not consume these attempts. |
| Partial cleanup | Each attempt streams extraction into a sibling `.tmp`. Any network, extraction, or checksum failure removes it, and unverified bytes never appear at the final path. |
| Restart and resume | A failed tarball restarts from byte zero without a `Range` request. Across process restarts, each atomically published entity is skipped, while a leftover `.tmp` is discarded and reacquired. |
| Integrity | `SHA256SUMS` is required. The complete upstream `.tar.xz` byte stream is hashed, including bytes after the selected tar entry, and the extracted entity is published only after the digest matches. |
| Terminal error | Exhaustion returns an error that names the entity and retains the final timeout, HTTP status, extraction, or checksum cause. |

## Comparison with discogs-ingestion

The independently maintained Discogs downloader has the same 120-second connect/read bounds,
three post-connect attempts, 2-second exponential-backoff base, cleanup on failure,
restart-from-zero behavior, SHA-256 verification, and cause-preserving terminal errors. Those
are the invariants a paired-repository verification may compare.

The storage protocols intentionally differ:

- MusicBrainz requires `SHA256SUMS`, transforms each tarball while streaming into a sibling
  `.tmp`, and atomically publishes the extracted entity only after verification. It resumes a
  version by skipping each already-published entity.
- Discogs stores the upstream gzip at its final filename while streaming. A crash may leave that
  path partial, but the absence of trusted metadata forces the next run to replace it. Its
  monthly published checksum lookup is best-effort.

The comparison is behavioral. Repository-local constants and implementation structure are not
a shared source contract and should not be synchronized by copying source hashes.
