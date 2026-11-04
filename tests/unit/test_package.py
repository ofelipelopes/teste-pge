"""Smoke tests for the executable Python foundation."""

from __future__ import annotations

import sys
import unittest


class PackageFoundationTest(unittest.TestCase):
    """Verify the minimum package and interpreter contract."""

    def test_package_exposes_version(self) -> None:
        """The project package must be importable and versioned."""
        import pncp_etl

        self.assertEqual(pncp_etl.__version__, "0.1.0")

    def test_python_313_is_used(self) -> None:
        """The supported runtime for the initial implementation is Python 3.13."""
        self.assertEqual(sys.version_info[:2], (3, 13))


if __name__ == "__main__":
    unittest.main()
