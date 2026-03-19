from __future__ import annotations

import unittest

from scripts.tests.module_loader import load_script_module


builder = load_script_module(
    "polyalpha_build_btc_basis_backtest_db_uniqueness",
    "scripts/build_btc_basis_backtest_db.py",
)


class DuckdbUniquenessGuardTests(unittest.TestCase):
    class _FakeCursor:
        def __init__(self, duplicate_rows: int) -> None:
            self._duplicate_rows = duplicate_rows

        def fetchone(self) -> tuple[int]:
            return (self._duplicate_rows,)

    class _FakeConnection:
        def __init__(self, duplicate_rows: int) -> None:
            self.duplicate_rows = duplicate_rows
            self.seen_sql: str | None = None

        def execute(self, sql: str):
            self.seen_sql = sql
            return DuckdbUniquenessGuardTests._FakeCursor(self.duplicate_rows)

    def test_validate_unique_column_accepts_unique_keys(self) -> None:
        conn = self._FakeConnection(duplicate_rows=0)
        builder.validate_unique_column(conn, "sample", "ts_ms")
        self.assertIn("select ts_ms", conn.seen_sql or "")

    def test_validate_unique_column_rejects_duplicate_keys(self) -> None:
        conn = self._FakeConnection(duplicate_rows=2)
        with self.assertRaisesRegex(ValueError, "duplicated keys"):
            builder.validate_unique_column(conn, "sample", "ts_ms")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
