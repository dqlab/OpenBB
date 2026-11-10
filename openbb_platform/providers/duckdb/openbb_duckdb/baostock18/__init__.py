"""Offline BaoStock18 admission, independent of legacy a_share wide tables.

Importing this module performs no I/O. No provider acquisition code is used.
"""

from openbb_duckdb.baostock18.importer import import_candidate, set_visible
from openbb_duckdb.baostock18.manifest import prepare_manifest
from openbb_duckdb.baostock18.reader import read_bars

__all__ = ["import_candidate", "prepare_manifest", "read_bars", "set_visible"]
