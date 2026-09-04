from __future__ import annotations

import subprocess
import unittest
from unittest import mock

from scripts import check_bump_preview


class BumpPreviewTests(unittest.TestCase):
    def test_accepts_valid_preview(self) -> None:
        self.assertTrue(check_bump_preview.accepted_result(0, "bump: version 0.1.0 -> 0.2.0"))

    def test_accepts_only_the_explicit_no_commits_result(self) -> None:
        output = "[NO_COMMITS_FOUND]\nNo new commits found."
        self.assertTrue(check_bump_preview.accepted_result(3, output))
        self.assertFalse(check_bump_preview.accepted_result(4, output))
        self.assertFalse(check_bump_preview.accepted_result(3, "No new commits found."))

    def test_accepts_only_the_explicit_release_gap_result(self) -> None:
        output = """bump: version 0.1.1 → 0.2.0
tag to create: v0.2.0
increment detected: MINOR
No tag found to do an incremental changelog
"""
        self.assertTrue(check_bump_preview.accepted_result(16, output))
        self.assertFalse(check_bump_preview.accepted_result(15, output))
        self.assertFalse(
            check_bump_preview.accepted_result(16, "No tag found to do an incremental changelog")
        )

    def test_accepts_only_the_explicit_not_eligible_result(self) -> None:
        output = """bump: version 0.2.1 -> 0.2.1
tag to create: v0.2.1

[NO_COMMITS_TO_BUMP]
The commits found are not eligible to be bumped
"""
        self.assertTrue(check_bump_preview.accepted_result(21, output))
        self.assertFalse(check_bump_preview.accepted_result(20, output))
        self.assertFalse(
            check_bump_preview.accepted_result(21, "The commits found are not eligible to be bumped")
        )

    def test_propagates_unexpected_commitizen_failure(self) -> None:
        result = subprocess.CompletedProcess(
            check_bump_preview.COMMAND,
            7,
            stdout="",
            stderr="invalid config\n",
        )
        with mock.patch.object(check_bump_preview.subprocess, "run", return_value=result):
            self.assertEqual(check_bump_preview.main(), 7)


if __name__ == "__main__":
    unittest.main()
