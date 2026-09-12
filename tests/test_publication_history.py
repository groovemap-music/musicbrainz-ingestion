"""Regression tests for the local-only publication-history boundary."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "attest-publication-history.py"
SPEC = importlib.util.spec_from_file_location("publication_history_attestation", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
ATTESTATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ATTESTATION)


class PublicationHistoryBoundaryTests(unittest.TestCase):
    """Cover complete private roots and the intentionally public guide."""

    def test_private_planning_roots_and_descendants_are_rejected(self) -> None:
        private_paths = (
            ".planning",
            ".planning/roadmap.md",
            ".planning/nested/design.md",
            "docs/superpowers",
            "docs/superpowers/README.md",
            "docs/superpowers/plans/nested/plan.md",
            "docs/specs",
            "docs/specs/README.md",
            "docs/specs/nested/design.md",
        )

        for path in private_paths:
            with self.subTest(path=path):
                self.assertTrue(ATTESTATION.is_private_planning_path(path))

    def test_similar_and_public_paths_are_preserved(self) -> None:
        public_paths = (
            "docs/extraction.md",
            "docs/superpower/README.md",
            "docs/superpowers-old/README.md",
            "docs/specifications/README.md",
        )

        for path in public_paths:
            with self.subTest(path=path):
                self.assertFalse(ATTESTATION.is_private_planning_path(path))

    def test_rewrite_targets_complete_trees_but_not_public_guide(self) -> None:
        rehearsal = (ROOT / "scripts" / "rehearse-publication-history.sh").read_text(encoding="utf-8")

        self.assertIn("--path .planning/", rehearsal)
        self.assertIn("--path docs/superpowers/", rehearsal)
        self.assertIn("--path docs/specs/", rehearsal)
        self.assertNotIn("--path docs/extraction.md", rehearsal)
        self.assertIn("rev-list --objects --all", rehearsal)
        self.assertEqual(ATTESTATION.PRESERVED_PUBLIC_PATH, "docs/extraction.md")
        self.assertEqual(ATTESTATION.ARCHIVE_COMMIT, "daf82a149aaa382b3cebbd4b43d3c82e53d4128e")
        self.assertIn("reviewed-boundary-only", rehearsal)
        self.assertIn("archive-mapped", rehearsal)
        self.assertIn("unmapped-private", rehearsal)

    def test_changed_migration_wrapper_is_not_accepted_by_path(self) -> None:
        reviewed = (
            "docs/superpowers/README.md",
            "ca3b96ae188d756ef40549035cce987742e1ddcc",
            "fef85bb4804255946e49000752761e5480ded906d2109973d5e916e57e77925c",
        )
        rehearsal = (ROOT / "scripts" / "rehearse-publication-history.sh").read_text(encoding="utf-8")
        for identity_part in reviewed:
            self.assertIn(identity_part, rehearsal)
        ATTESTATION.require_reviewed_non_archival_blob(*reviewed)

        adversarial_identities = (
            (reviewed[0], "0" * 40, reviewed[2]),
            (reviewed[0], reviewed[1], "0" * 64),
            (reviewed[0], "0" * 40, "0" * 64),
        )
        for identity in adversarial_identities:
            with self.subTest(identity=identity):
                with self.assertRaisesRegex(AssertionError, "not mapped to the reviewed archive"):
                    ATTESTATION.require_reviewed_non_archival_blob(*identity)


if __name__ == "__main__":
    unittest.main()
