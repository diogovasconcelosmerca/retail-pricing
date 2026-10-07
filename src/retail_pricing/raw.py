"""Raw layer: read-only source access, snapshot extraction and validation.

Raw is the immutable copy of the seven PostgreSQL tables. Nothing downstream
edits it; a refresh is built in staging and replaces the snapshot only after it
validates.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb
import psycopg
import pyarrow.parquet as pq
from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from retail_pricing import PROJECT_ROOT, RAW_DIR

TABLES = (
    "locations",
    "nutritionals",
    "prices",
    "products",
    "weekly_prices",
    "weekly_prices_locations",
    "weekly_prices_products",
)

CONFIG_NAMES = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_SCHEMA")

# Discovery queries get a defensive timeout; a bulk export of the large tables may
# legitimately run longer, so its timeout is disabled (0).
DISCOVERY_STATEMENT_TIMEOUT_MS = 30_000
EXTRACTION_STATEMENT_TIMEOUT_MS = 0


def utc_now() -> str:
    """Return the current UTC timestamp in ISO format."""
    return datetime.now(UTC).isoformat()


def source_config() -> dict[str, str]:
    """Read configuration only when source access is explicitly requested."""
    missing = [name for name in CONFIG_NAMES if not os.getenv(name)]
    if missing:
        raise ValueError("Missing source configuration: " + ", ".join(missing))
    return {
        "host": os.environ["DB_HOST"],
        "port": os.environ["DB_PORT"],
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "schema": os.environ["DB_SCHEMA"],
    }


def quote_identifier(value: str) -> str:
    """Safely quote one SQL identifier."""
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value: object) -> str:
    """Safely quote one SQL literal."""
    return "'" + str(value).replace("'", "''") + "'"


def connection_info(
    config: dict[str, str],
    *,
    statement_timeout_ms: int = DISCOVERY_STATEMENT_TIMEOUT_MS,
) -> str:
    """Build a read-only libpq connection string with the given statement timeout."""
    return make_conninfo(
        **{key: config[key] for key in ("host", "port", "dbname", "user", "password")},
        connect_timeout=10,
        options=(
            "-c default_transaction_read_only=on "
            f"-c statement_timeout={statement_timeout_ms}"
        ),
    )


@contextmanager
def source_connection() -> Iterator[psycopg.Connection]:
    """Open a short-lived read-only PostgreSQL connection for discovery queries."""
    with psycopg.connect(
        connection_info(
            source_config(), statement_timeout_ms=DISCOVERY_STATEMENT_TIMEOUT_MS
        ),
        autocommit=True,
        row_factory=dict_row,
    ) as conn:
        yield conn


@contextmanager
def attached_source(config: dict[str, str]) -> Iterator[duckdb.DuckDBPyConnection]:
    """Attach PostgreSQL to DuckDB (read-only) for the explicit bulk extraction."""
    with duckdb.connect() as connection:
        connection.execute("INSTALL postgres")
        connection.execute("LOAD postgres")
        conninfo = connection_info(
            config, statement_timeout_ms=EXTRACTION_STATEMENT_TIMEOUT_MS
        )
        # This string contains credentials: never print or persist it.
        connection.execute(
            f"ATTACH {quote_literal(conninfo)} AS pg_source "
            f"(TYPE postgres, READ_ONLY, SCHEMA {quote_literal(config['schema'])})"
        )
        yield connection


def sha256_file(path: Path) -> str:
    """Calculate SHA-256 without loading the entire file into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_files(raw_dir: Path) -> list[dict]:
    """Read Parquet metadata and fingerprints; do not query PostgreSQL."""
    records = []
    for table in TABLES:
        path = Path(raw_dir) / f"{table}.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"Missing raw file: {path.name}")
        with pq.ParquetFile(path) as parquet:
            records.append(
                {
                    "table": table,
                    "file": path.name,
                    "rows": parquet.metadata.num_rows,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "schema": [
                        {
                            "name": field.name,
                            "type": str(field.type),
                            "nullable": field.nullable,
                        }
                        for field in parquet.schema_arrow
                    ],
                }
            )
    return records


def manifest_document(records: list[dict], provenance: dict) -> dict:
    """Build one Raw manifest document."""
    return {
        "manifest_version": 1,
        "recorded_at_utc": utc_now(),
        "provenance": provenance,
        "transactionally_consistent_across_tables": False,
        "files": records,
    }


def validate_local_snapshot(raw_dir: Path) -> dict:
    """Create an honest baseline for existing files or validate it on rerun."""
    raw_dir = Path(raw_dir)
    records = inspect_files(raw_dir)
    manifest_path = raw_dir / "manifest.json"

    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("manifest_version") != 1 or manifest.get("files") != records:
            raise ValueError(
                "Raw files differ from the recorded manifest; "
                "investigate before proceeding."
            )
        return manifest

    manifest = manifest_document(
        records,
        {
            "origin": "existing_local_files",
            "source_system": "PostgreSQL, as documented in the discovery notebook",
            "source_extracted_at_utc": None,
            "source_row_reconciliation": "unavailable_for_historical_extraction",
            "note": (
                "Baseline recorded during checkpoint preparation; "
                "original extraction time and source state are not inferred."
            ),
        },
    )
    with manifest_path.open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    return manifest


def _export_tables(config: dict[str, str], staging: Path) -> dict[str, int]:
    """Copy every source table to Parquet in staging; return the exported row counts."""
    exported_counts = {}
    with attached_source(config) as connection:
        for table in TABLES:
            print(f"Extracting source table: {table}")
            source = ".".join(
                quote_identifier(part)
                for part in ("pg_source", config["schema"], table)
            )
            result = connection.execute(
                f"COPY {source} TO {quote_literal(staging / f'{table}.parquet')} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            ).fetchone()
            exported_counts[table] = result[0]
            print(f"Extracted {table}: {result[0]:,} rows")
    return exported_counts


def _publish_by_hard_link(staging: Path, raw_dir: Path) -> None:
    """Link the staged files into place, manifest last; undo everything on failure.

    Hard links fail if the target already exists, so nothing is overwritten. Staging
    lives on the same filesystem. Publishing the manifest last means readers never
    see an apparently complete partial snapshot.
    """
    names = [f"{table}.parquet" for table in TABLES] + ["manifest.json"]
    published = []
    try:
        for name in names:
            destination = raw_dir / name
            os.link(staging / name, destination)
            published.append(destination)
    except Exception:
        for destination in reversed(published):
            destination.unlink()
        raise


def extract_snapshot(raw_dir: Path, *, enabled: bool = False) -> dict | None:
    """Export all source tables to staging, reconcile row counts, then publish.

    Extraction is opt-in (``enabled``): the discovery notebook and tests call it
    with ``enabled=False`` and get ``None``. Bulk extraction runs with the PostgreSQL
    statement timeout disabled because large COPY operations may legitimately exceed
    the discovery-query timeout. An existing snapshot is never overwritten.
    """
    if not enabled:
        return None

    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    destinations = [raw_dir / f"{table}.parquet" for table in TABLES]
    destinations.append(raw_dir / "manifest.json")
    if any(path.exists() for path in destinations):
        raise FileExistsError(
            "Raw snapshot already exists; choose a separate empty destination."
        )

    config = source_config()
    started = utc_now()

    with tempfile.TemporaryDirectory(prefix=".extract-", dir=raw_dir) as directory:
        staging = Path(directory)
        exported_counts = _export_tables(config, staging)

        records = inspect_files(staging)
        if any(
            record["rows"] != exported_counts[record["table"]] for record in records
        ):
            raise ValueError(
                "Export row count does not match "
                "Parquet metadata; snapshot not published."
            )

        manifest = manifest_document(
            records,
            {
                "origin": "source_export",
                "source_schema": config["schema"],
                "source_extraction_started_at_utc": started,
                "source_extraction_completed_at_utc": utc_now(),
                "source_row_reconciliation": (
                    "COPY returned rows equal local Parquet rows; "
                    "no separate source COUNT scan"
                ),
                "exported_rows": exported_counts,
            },
        )
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        _publish_by_hard_link(staging, raw_dir)

    return manifest


def _validate_table_contract(manifest: dict) -> None:
    """Confirm that the Raw snapshot contains exactly the expected source tables."""
    expected_tables = set(TABLES)
    actual_tables = {entry["table"] for entry in manifest["files"]}
    if actual_tables != expected_tables:
        raise RuntimeError(
            "Raw snapshot table contract failed. "
            f"Missing={sorted(expected_tables - actual_tables)}, "
            f"Unexpected={sorted(actual_tables - expected_tables)}"
        )


def _rebuild_raw_snapshot(raw_dir: Path) -> dict:
    """Re-extract Raw from PostgreSQL safely.

    Extraction first writes to a new staging directory. The current Raw snapshot
    is replaced only after the new snapshot passes validation.
    """
    load_dotenv(PROJECT_ROOT / ".env", override=True)

    suffix = uuid4().hex[:8]
    staging_dir = raw_dir.parent / f"_raw_rebuild_{suffix}"
    backup_dir = raw_dir.parent / f"_raw_backup_{suffix}"

    print(f"Extracting new Raw snapshot to {staging_dir.name}...")
    try:
        if extract_snapshot(staging_dir, enabled=True) is None:
            raise RuntimeError(
                "Raw rebuild was requested but extraction produced no manifest."
            )

        print("Validating newly extracted Raw snapshot...")
        _validate_table_contract(validate_local_snapshot(staging_dir))
        print("New Raw snapshot passed validation.")

        # Replace the published Raw snapshot only after validation succeeds.
        if raw_dir.exists():
            raw_dir.rename(backup_dir)
        try:
            staging_dir.rename(raw_dir)
        except Exception:
            # Restore the previous Raw snapshot if publication fails.
            if backup_dir.exists() and not raw_dir.exists():
                backup_dir.rename(raw_dir)
            raise

        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        return validate_local_snapshot(raw_dir)

    finally:
        # Clean abandoned staging only when it was not published.
        if staging_dir.exists():
            shutil.rmtree(staging_dir)


def run_raw_pipeline(*, rebuild: bool = False, raw_dir: Path = RAW_DIR) -> dict:
    """Validate the existing Raw snapshot or rebuild it safely from source."""
    started_at = perf_counter()

    if rebuild:
        print("Raw rebuild enabled.")
        manifest = _rebuild_raw_snapshot(raw_dir)
    else:
        print("Raw rebuild skipped; validating existing Raw snapshot.")
        manifest = validate_local_snapshot(raw_dir)
        _validate_table_contract(manifest)

    print("\nRaw pipeline complete.")
    for entry in sorted(manifest["files"], key=lambda item: item["table"]):
        print(f"{entry['table']}: {entry['rows']:,} rows")
    print(f"Validated tables: {len(manifest['files'])}")
    print(f"Duration: {perf_counter() - started_at:.1f}s")
    return manifest
