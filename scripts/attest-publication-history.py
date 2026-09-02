"""Attest a separately rewritten publication candidate without mutating a remote."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any


REPOSITORY = "groovemap-music/musicbrainz-ingestion"
ARCHIVE_COMMIT = "daf82a149aaa382b3cebbd4b43d3c82e53d4128e"
PRIVATE_PLANNING_ROOTS = (".planning", "docs/superpowers", "docs/specs")
PRESERVED_PUBLIC_PATH = "docs/extraction.md"
# This migration-only index was created in the split repository and therefore is not part
# of the original monorepo corpus captured by planning-archive. Pin all three identities so
# changed content at the same path cannot bypass archive coverage.
REVIEWED_NON_ARCHIVAL_BLOBS = frozenset(
    {
        (
            "docs/superpowers/README.md",
            "ca3b96ae188d756ef40549035cce987742e1ddcc",
            "fef85bb4804255946e49000752761e5480ded906d2109973d5e916e57e77925c",
        )
    }
)
CREDENTIAL_PATH = re.compile(r"(^|/)(?:\.env(?:\.|$)|secrets?(?:/|$))|\.(?:age|key|p12|pem)$", re.IGNORECASE)
GIT = shutil.which("git")
assert GIT is not None, "git is required"


def is_private_planning_path(path: str) -> bool:
    """Return whether a Git object path belongs to a private planning tree."""
    return any(path == root or path.startswith(f"{root}/") for root in PRIVATE_PLANNING_ROOTS)


def require_reviewed_non_archival_blob(path: str, object_id: str, digest: str) -> None:
    """Fail closed unless one non-archival blob has its exact reviewed identity."""
    identity = (path, object_id, digest)
    assert identity in REVIEWED_NON_ARCHIVAL_BLOBS, f"private blob is not mapped to the reviewed archive: {path} {object_id}"


def run_git(repository: Path, *arguments: str, input_text: str | None = None) -> str:
    """Run Git in one repository and return stripped standard output."""
    return subprocess.run(  # noqa: S603 -- Git is resolved once from PATH
        [GIT, "-C", str(repository), *arguments],
        input=input_text,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    ).stdout.strip()


def reachable_objects(repository: Path) -> list[tuple[str, str]]:
    """Return object IDs and paths reachable from every ref."""
    entries: list[tuple[str, str]] = []
    for line in run_git(repository, "rev-list", "--objects", "--all").splitlines():
        object_id, separator, path = line.partition(" ")
        entries.append((object_id, path if separator else ""))
    return entries


def object_types(repository: Path, object_ids: list[str]) -> dict[str, str]:
    """Resolve object types in a single batch."""
    if not object_ids:
        return {}
    lines = run_git(
        repository,
        "cat-file",
        "--batch-check=%(objectname) %(objecttype)",
        input_text="\n".join(object_ids) + "\n",
    ).splitlines()
    return dict(line.split(maxsplit=1) for line in lines)


def sha256_blob(repository: Path, object_id: str) -> str:
    """Hash one Git blob by its exact bytes."""
    completed = subprocess.run(  # noqa: S603 -- Git is resolved once from PATH
        [GIT, "-C", str(repository), "cat-file", "blob", object_id],
        check=True,
        capture_output=True,
        timeout=120,
    )
    return hashlib.sha256(completed.stdout).hexdigest()


def load_archive_manifest(archive_repository: Path) -> dict[str, Any]:
    """Load the immutable private-archive manifest from its reviewed commit."""
    commit = run_git(archive_repository, "rev-parse", f"{ARCHIVE_COMMIT}^{{commit}}")
    assert commit == ARCHIVE_COMMIT, "reviewed planning-archive commit is unavailable"
    return json.loads(run_git(archive_repository, "show", f"{ARCHIVE_COMMIT}:archive/manifest.json"))


def parse_commit_map(sanitized_repository: Path) -> list[dict[str, str]]:
    """Read git-filter-repo's complete old-to-new map."""
    path = sanitized_repository / "filter-repo" / "commit-map"
    assert path.is_file(), f"missing git-filter-repo commit map: {path}"
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        old, new = line.split()
        rows.append({"old": old, "new": new})
    assert rows, "git-filter-repo commit map is empty"
    return rows


def build_attestation(
    *,
    backup_repository: Path,
    candidate_source: Path,
    candidate_commit: str,
    sanitized_repository: Path,
    archive_repository: Path,
) -> dict[str, Any]:
    """Verify archive coverage, rewrite completeness, and candidate identity."""
    for repository in (backup_repository, candidate_source, sanitized_repository, archive_repository):
        assert repository.exists(), f"repository does not exist: {repository}"

    source_commit = run_git(candidate_source, "rev-parse", f"{candidate_commit}^{{commit}}")
    source_tree = run_git(candidate_source, "rev-parse", f"{source_commit}^{{tree}}")
    assert run_git(sanitized_repository, "show-ref", "--verify", "--hash", "refs/heads/main"), "sanitized main is missing"

    source_objects = reachable_objects(candidate_source)
    source_types = object_types(candidate_source, [object_id for object_id, _ in source_objects])
    private_blobs = sorted(
        (object_id, path)
        for object_id, path in source_objects
        if path and is_private_planning_path(path) and source_types.get(object_id) == "blob"
    )
    assert private_blobs, "source repository contains no private planning blobs to map"

    manifest = load_archive_manifest(archive_repository)
    archived = {
        (occurrence["path"], occurrence["git_blob_oid"], occurrence["sha256"])
        for occurrence in manifest["occurrences"]
        if occurrence["repository"] == REPOSITORY
    }
    mapped_blobs = []
    boundary_only_blobs = []
    for object_id, path in private_blobs:
        digest = sha256_blob(candidate_source, object_id)
        record = {"git_blob_oid": object_id, "path": path, "sha256": digest}
        if (path, object_id, digest) in archived:
            mapped_blobs.append(record)
            continue
        require_reviewed_non_archival_blob(path, object_id, digest)
        boundary_only_blobs.append(record)

    sanitized_objects = reachable_objects(sanitized_repository)
    forbidden_paths = sorted(
        path
        for _, path in sanitized_objects
        if path and (is_private_planning_path(path) or CREDENTIAL_PATH.search(path))
    )
    assert not forbidden_paths, f"sanitized object graph retains forbidden paths: {', '.join(forbidden_paths)}"
    run_git(sanitized_repository, "fsck", "--full", "--strict")

    commit_map = parse_commit_map(sanitized_repository)
    mapped_source = next((row["new"] for row in commit_map if row["old"] == source_commit), None)
    assert mapped_source and set(mapped_source) != {"0"}, "candidate commit is absent from the old-to-new map"
    sanitized_tree = run_git(sanitized_repository, "rev-parse", f"{mapped_source}^{{tree}}")
    assert run_git(sanitized_repository, "rev-parse", "refs/heads/main") == mapped_source, "sanitized main is not the mapped candidate"

    source_public_blob = run_git(candidate_source, "rev-parse", f"{source_commit}:{PRESERVED_PUBLIC_PATH}")
    sanitized_public_blob = run_git(sanitized_repository, "rev-parse", f"{mapped_source}:{PRESERVED_PUBLIC_PATH}")
    assert source_public_blob == sanitized_public_blob, f"public guide changed during rewrite: {PRESERVED_PUBLIC_PATH}"

    backup_refs = run_git(backup_repository, "for-each-ref", "--format=%(refname) %(objectname)").splitlines()
    sanitized_refs = run_git(sanitized_repository, "for-each-ref", "--format=%(refname) %(objectname)").splitlines()
    assert backup_refs, "private-remote backup has no refs"
    assert sanitized_refs, "sanitized candidate has no refs"

    return {
        "schema_version": 1,
        "repository": REPOSITORY,
        "archive": {
            "commit": ARCHIVE_COMMIT,
            "mapped_private_blob_count": len(mapped_blobs),
            "mapped_private_blobs": mapped_blobs,
            "removed_non_archival_blob_count": len(boundary_only_blobs),
            "removed_non_archival_blobs": boundary_only_blobs,
        },
        "candidate": {
            "source_commit": source_commit,
            "source_tree": source_tree,
            "sanitized_commit": mapped_source,
            "sanitized_tree": sanitized_tree,
        },
        "history": {
            "backup_refs": sorted(backup_refs),
            "commit_map": commit_map,
            "forbidden_path_matches": [],
            "sanitized_object_count": len(sanitized_objects),
            "sanitized_refs": sorted(sanitized_refs),
            "scan_scope": "every object reachable from every candidate ref",
        },
        "preserved_public_paths": [
            {
                "git_blob_oid": source_public_blob,
                "path": PRESERVED_PUBLIC_PATH,
            }
        ],
        "external_approval_gates": [
            {
                "approved": False,
                "gate": "private-remote history cutover",
                "required_action": "approve the exact ref update and reviewed old-to-new commit map",
            },
            {
                "approved": False,
                "gate": "repository publication",
                "required_action": "approve the exact OpenTofu visibility and main-protection plan",
            },
        ],
        "mutations_performed": {
            "history_cutover": False,
            "repository_visibility_changed": False,
            "remote_refs_changed": False,
        },
    }


def parse_args() -> argparse.Namespace:
    """Parse explicit local evidence paths."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-repository", type=Path, required=True)
    parser.add_argument("--candidate-source", type=Path, required=True)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--sanitized-repository", type=Path, required=True)
    parser.add_argument("--archive-repository", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    """Write deterministic, commit-bound local evidence."""
    arguments = parse_args()
    attestation = build_attestation(
        backup_repository=arguments.backup_repository.resolve(),
        candidate_source=arguments.candidate_source.resolve(),
        candidate_commit=arguments.candidate_commit,
        sanitized_repository=arguments.sanitized_repository.resolve(),
        archive_repository=arguments.archive_repository.resolve(),
    )
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(attestation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "publication history attested: "
        f"{attestation['candidate']['source_commit']} -> {attestation['candidate']['sanitized_commit']}"
    )


if __name__ == "__main__":
    main()
