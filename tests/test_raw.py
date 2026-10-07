"""Raw layer: source access is opt-in, read-only and never half-published."""

import re
from contextlib import contextmanager

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psycopg.conninfo import conninfo_to_dict

from retail_pricing import raw as io


def write_snapshot(path):
    for table in io.TABLES:
        pq.write_table(pa.table({"id": [1, 2]}), path / f"{table}.parquet")


def test_disabled_extraction_never_reads_credentials_or_connects(tmp_path, monkeypatch):
    monkeypatch.setattr(io, "source_config", lambda: pytest.fail("source accessed"))
    assert io.extract_snapshot(tmp_path, enabled=False) is None
    assert not list(tmp_path.iterdir())


def test_existing_file_blocks_entire_export_before_connection(tmp_path, monkeypatch):
    target = tmp_path / "weekly_prices.parquet"
    target.write_bytes(b"preserve")
    monkeypatch.setattr(io, "source_config", lambda: pytest.fail("source accessed"))
    with pytest.raises(FileExistsError):
        io.extract_snapshot(tmp_path, enabled=True)
    assert target.read_bytes() == b"preserve"


def test_manifest_detects_changes_and_does_not_invent_provenance(tmp_path):
    write_snapshot(tmp_path)
    manifest = io.validate_local_snapshot(tmp_path)
    assert manifest["provenance"]["source_extracted_at_utc"] is None
    assert len(manifest["files"]) == 7
    assert all(record["rows"] == 2 for record in manifest["files"])
    assert io.validate_local_snapshot(tmp_path) == manifest
    pq.write_table(pa.table({"id": [3, 4]}), tmp_path / "prices.parquet")
    with pytest.raises(ValueError, match="differ"):
        io.validate_local_snapshot(tmp_path)


def test_incomplete_snapshot_has_no_manifest(tmp_path):
    with pytest.raises(FileNotFoundError):
        io.validate_local_snapshot(tmp_path)
    assert not (tmp_path / "manifest.json").exists()


def test_credentials_roundtrip_and_readonly_options():
    config = {
        "host": "localhost",
        "port": "5432",
        "dbname": "case",
        "user": "a b",
        "password": "p ' \\ word",
    }
    actual = conninfo_to_dict(io.connection_info(config))
    assert all(actual[key] == value for key, value in config.items())
    assert "default_transaction_read_only=on" in actual["options"]
    assert "statement_timeout=30000" in actual["options"]
    extraction = conninfo_to_dict(io.connection_info(config, statement_timeout_ms=0))
    assert "statement_timeout=0" in extraction["options"]
    assert extraction["connect_timeout"] == "10"


def test_source_connection_closes_on_error(monkeypatch):
    config = {
        "host": "localhost",
        "port": "5432",
        "dbname": "case",
        "user": "test",
        "password": "test",
    }
    closed = []

    @contextmanager
    def fake_connect(*args, **kwargs):
        try:
            yield object()
        finally:
            closed.append(True)

    monkeypatch.setattr(io, "source_config", lambda: config)
    monkeypatch.setattr(io.psycopg, "connect", fake_connect)
    with pytest.raises(RuntimeError), io.source_connection():
        raise RuntimeError("test")
    assert closed == [True]


@pytest.mark.parametrize("mismatch", [False, True])
def test_export_reconciles_rows_and_publishes_only_complete_snapshot(
    tmp_path, monkeypatch, mismatch
):
    monkeypatch.setattr(io, "source_config", lambda: {"schema": "test schema"})

    class FakeConnection:
        def execute(self, query):
            # Accept SQL whitespace and escaped quotes without coupling the fake to formatting.
            match = re.search(r"\bTO\s+'((?:''|[^'])*)'\s*\(", query, re.IGNORECASE)
            assert match is not None
            destination = match.group(1).replace("''", "'")
            pq.write_table(pa.table({"id": [1, 2]}), destination)
            return self

        def fetchone(self):
            return (3 if mismatch else 2,)

    @contextmanager
    def fake_attachment(config):
        yield FakeConnection()

    monkeypatch.setattr(io, "attached_source", fake_attachment)
    if mismatch:
        with pytest.raises(ValueError, match="row count"):
            io.extract_snapshot(tmp_path, enabled=True)
        assert not list(tmp_path.iterdir())
    else:
        manifest = io.extract_snapshot(tmp_path, enabled=True)
        assert len(list(tmp_path.glob("*.parquet"))) == 7
        assert io.validate_local_snapshot(tmp_path) == manifest
