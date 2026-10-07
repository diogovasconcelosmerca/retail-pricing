"""Run the pipeline: Raw -> Silver -> Gold -> parity, from one command line.

    python -m retail_pricing.pipeline raw [--rebuild]
    python -m retail_pricing.pipeline silver
    python -m retail_pricing.pipeline gold
    python -m retail_pricing.pipeline parity [--update-baseline]
    python -m retail_pricing.pipeline all [--rebuild-raw]

``all`` runs every stage in its own process and stops on the first failure.
Silver is published stage-then-move: files are built and validated in a staging
directory, moved into place, and the manifest is written last. Readers trust only
a complete manifest whose ``code_sha256`` matches the current source code.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from retail_pricing import GOLD_DIR, PROJECT_ROOT, RAW_DIR, SILVER_DIR, SILVER_FILES
from retail_pricing.gold import (
    add_price_comparison_columns,
    attach_primary_categories,
    build_category_model,
    build_dim_date,
    build_dim_location,
    build_dim_nutrient,
    build_dim_product,
    build_dim_shop,
    create_fact_product_nutrition,
    create_fact_weekly_prices,
)
from retail_pricing.gold_validation import (
    validate_gold_model,
    validate_mart_reconciliation,
)
from retail_pricing.quality import run_cross_table_contracts
from retail_pricing.raw import run_raw_pipeline, sha256_file, validate_local_snapshot
from retail_pricing.silver_locations import (
    run_locations_pipeline,
    run_weekly_locations_pipeline,
)
from retail_pricing.silver_nutrition import run_nutritionals_pipeline
from retail_pricing.silver_prices import run_prices_pipeline, run_weekly_prices_pipeline
from retail_pricing.silver_products import (
    run_category_paths_pipeline,
    run_primary_categories_pipeline,
    run_products_pipeline,
    run_weekly_products_pipeline,
)

logger = logging.getLogger(__name__)

# The Silver datasets, in publication order (also the order of the manifest).
OUTPUTS = tuple(SILVER_FILES)

MANIFEST_NAME = "manifest.json"
STAGING_PREFIX = ".silver-build-"


# --- Silver: build, validate, publish -----------------------------------------


def code_fingerprint() -> str:
    """Identify the exact local Python implementation, including uncommitted work."""
    package = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for path in sorted(package.rglob("*.py")):
        digest.update(path.relative_to(package).as_posix().encode())
        # Line endings are normalised so a CRLF/LF checkout does not invalidate Silver.
        digest.update(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n"))
    return digest.hexdigest()


def _output_records(directory: Path) -> list[dict]:
    """Describe every Silver file: rows, size, hash and schema."""
    records = []
    for name in OUTPUTS:
        path = directory / SILVER_FILES[name]
        with pq.ParquetFile(path) as parquet:
            records.append(
                {
                    "dataset": name,
                    "file": path.name,
                    "rows": parquet.metadata.num_rows,
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                    "schema": [
                        {"name": f.name, "type": str(f.type)}
                        for f in parquet.schema_arrow
                    ],
                }
            )
    return records


def load_silver_manifest(silver_dir: Path = SILVER_DIR) -> dict:
    """Read only complete, current-code artifacts; never rebuild data implicitly.

    The manifest is written last, so a directory without a valid manifest is an
    unfinished or interrupted publication and is refused.
    """
    path = Path(silver_dir) / MANIFEST_NAME
    if not path.is_file():
        raise FileNotFoundError(
            "No validated Silver checkpoint. Run the official Silver pipeline first."
        )
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("manifest_version") != 1 or manifest.get("status") != "complete":
        raise RuntimeError("Silver manifest is not a completed supported checkpoint.")
    if manifest.get("code_sha256") != code_fingerprint():
        raise RuntimeError(
            "Silver was built with different source code; "
            "rebuild before interpreting results."
        )
    if manifest["files"] != _output_records(Path(silver_dir)):
        raise RuntimeError(
            "Silver files differ from the completed manifest; rebuild or investigate."
        )
    return manifest


def publish_silver(staging: Path, silver_dir: Path) -> None:
    """Move validated files into place and write the manifest last.

    The old manifest is removed first, so a reader never pairs new files with an
    old manifest: until the new manifest lands, the directory counts as unfinished.
    """
    manifest = silver_dir / MANIFEST_NAME
    manifest.unlink(missing_ok=True)
    for name in SILVER_FILES.values():
        os.replace(staging / name, silver_dir / name)
    os.replace(staging / MANIFEST_NAME, manifest)


def _silver_jobs(raw_dir: Path, staging: Path) -> list[tuple[str, Callable, dict]]:
    """Every Silver builder in dependency order, with the inputs it reads.

    Cleaned base tables come first; the category tables are derived from them and
    therefore read the staged files. Looked up at call time so tests can replace a
    builder.
    """
    staged = {name: staging / filename for name, filename in SILVER_FILES.items()}
    return [
        ("products", run_products_pipeline, {"raw_path": raw_dir / "products.parquet"}),
        (
            "locations",
            run_locations_pipeline,
            {"raw_path": raw_dir / "locations.parquet"},
        ),
        ("prices", run_prices_pipeline, {"raw_path": raw_dir / "prices.parquet"}),
        (
            "nutritionals",
            run_nutritionals_pipeline,
            {"raw_path": raw_dir / "nutritionals.parquet"},
        ),
        (
            "weekly_products",
            run_weekly_products_pipeline,
            {
                "raw_path": raw_dir / "weekly_prices_products.parquet",
                "fallback_path": staged["products"],
            },
        ),
        (
            "weekly_locations",
            run_weekly_locations_pipeline,
            {
                "raw_path": raw_dir / "weekly_prices_locations.parquet",
                "fallback_path": staged["locations"],
            },
        ),
        (
            "weekly_prices",
            run_weekly_prices_pipeline,
            {"raw_path": raw_dir / "weekly_prices.parquet"},
        ),
        (
            "product_category_paths",
            run_category_paths_pipeline,
            {
                "products_path": staged["products"],
                "weekly_products_path": staged["weekly_products"],
            },
        ),
        (
            "product_primary_categories",
            run_primary_categories_pipeline,
            {
                "products_path": staged["products"],
                "weekly_products_path": staged["weekly_products"],
                "nutritionals_path": staged["nutritionals"],
                "category_paths_path": staged["product_category_paths"],
            },
        ),
    ]


def _silver_manifest(
    *,
    started: str,
    source_hash: str,
    raw_manifest: dict,
    metrics: dict,
    timings: dict,
    staging: Path,
) -> dict:
    """Describe a finished, validated Silver build."""
    return {
        "manifest_version": 1,
        "status": "complete",
        "started_at_utc": started,
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "code_sha256": source_hash,
        "raw_files": raw_manifest["files"],
        "raw_provenance": raw_manifest["provenance"],
        "versions": {
            "python": platform.python_version(),
            **{
                name: importlib.metadata.version(name)
                for name in ["polars", "duckdb", "pyarrow"]
            },
        },
        "metrics": metrics,
        "table_seconds": timings,
        "files": _output_records(staging),
        "publication": "staged_validation_then_move_manifest_last",
    }


def run_silver_pipeline(
    raw_dir: Path = RAW_DIR, silver_dir: Path = SILVER_DIR
) -> dict[str, dict]:
    """Build all Silver datasets in isolation, validate, then publish together."""
    raw_dir, silver_dir = Path(raw_dir).resolve(), Path(silver_dir).resolve()
    if silver_dir.is_relative_to(raw_dir) or raw_dir.is_relative_to(silver_dir):
        raise ValueError(
            "Raw and Silver directories must be separate, non-nested paths."
        )
    if not (raw_dir / MANIFEST_NAME).is_file():
        raise FileNotFoundError(
            "Raw manifest is required; "
            "establish/validate the snapshot in discovery first."
        )
    raw_manifest = validate_local_snapshot(raw_dir)
    source_hash = code_fingerprint()
    started = datetime.now(UTC).isoformat()
    start = time.perf_counter()
    silver_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Building Silver from %s", raw_dir)

    staging = Path(tempfile.mkdtemp(prefix=STAGING_PREFIX, dir=silver_dir))
    try:
        metrics, timings = {}, {}
        for name, builder, inputs in _silver_jobs(raw_dir, staging):
            tick = time.perf_counter()
            metrics[name] = builder(**inputs, silver_path=staging / SILVER_FILES[name])
            timings[name] = round(time.perf_counter() - tick, 3)
            logger.info("%s | %.3fs | %s", name, timings[name], metrics[name])

        metrics["cross_table"] = run_cross_table_contracts(staging)

        # Detect concurrent edits to either input snapshot or implementation.
        if (
            validate_local_snapshot(raw_dir) != raw_manifest
            or code_fingerprint() != source_hash
        ):
            raise RuntimeError(
                "Inputs or source code changed during the build; nothing published."
            )

        manifest = _silver_manifest(
            started=started,
            source_hash=source_hash,
            raw_manifest=raw_manifest,
            metrics=metrics,
            timings=timings,
            staging=staging,
        )
        manifest["build_seconds"] = round(time.perf_counter() - start, 3)
        (staging / MANIFEST_NAME).write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        publish_silver(staging, silver_dir)
        logger.info("Silver complete | %.3fs", time.perf_counter() - start)
        return metrics
    finally:
        # The staging directory is our own temporary child of the output directory.
        shutil.rmtree(staging, ignore_errors=True)


# --- Gold: build, validate, write -----------------------------------------------


def _copy_to_parquet(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    """Write one DuckDB fact table to Parquet and return its row count."""
    con.execute(
        f"COPY {table} TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return con.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def run_gold_pipeline() -> None:
    """Build, validate and publish the complete Gold analytical model."""
    started_at = time.perf_counter()
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    silver = {name: SILVER_DIR / filename for name, filename in SILVER_FILES.items()}

    logger.info("Building Gold dimensions...")
    dim_date = build_dim_date(silver["weekly_prices"], silver["nutritionals"])
    dim_shop = build_dim_shop(silver["weekly_prices"], silver["nutritionals"])
    dim_product = build_dim_product(
        silver["weekly_products"],
        silver["nutritionals"],
        silver["products"],
        silver["weekly_prices"],
    )
    dim_product = attach_primary_categories(
        dim_product, silver["product_primary_categories"]
    )
    dim_location = build_dim_location(
        silver["weekly_locations"], silver["weekly_prices"]
    )
    dim_nutrient = build_dim_nutrient(silver["nutritionals"])

    logger.info("Building Gold category model...")
    dim_category, bridge_product_category = build_category_model(
        silver["product_category_paths"], dim_product
    )

    dimensions = {
        "dim_date": dim_date,
        "dim_shop": dim_shop,
        "dim_product": dim_product,
        "dim_category": dim_category,
        "bridge_product_category": bridge_product_category,
        "dim_location": dim_location,
        "dim_nutrient": dim_nutrient,
    }

    logger.info("Building Gold facts...")
    with duckdb.connect() as con:
        create_fact_weekly_prices(
            con,
            silver["weekly_prices"],
            dim_product,
            dim_shop,
            dim_location,
            dim_date,
        )
        add_price_comparison_columns(con)
        create_fact_product_nutrition(
            con, silver["nutritionals"], dim_product, dim_shop, dim_date, dim_nutrient
        )

        logger.info("Validating Gold model...")
        validate_mart_reconciliation(
            con,
            silver["weekly_prices"].as_posix(),
            silver["nutritionals"].as_posix(),
        )
        validate_gold_model(con, **dimensions)

        logger.info("Publishing Gold outputs...")
        for name, frame in dimensions.items():
            frame.write_parquet(GOLD_DIR / f"{name}.parquet")
        pricing_rows = _copy_to_parquet(
            con, "fact_weekly_prices", GOLD_DIR / "fact_weekly_prices.parquet"
        )
        nutrition_rows = _copy_to_parquet(
            con, "fact_product_nutrition", GOLD_DIR / "fact_product_nutrition.parquet"
        )

    print("\nGold pipeline complete.")
    for name, frame in dimensions.items():
        print(f"{name}: {frame.height:,}")
    print(f"fact_weekly_prices: {pricing_rows:,}")
    print(f"fact_product_nutrition: {nutrition_rows:,}")
    print(f"Duration: {time.perf_counter() - started_at:.1f}s")


# --- All stages -------------------------------------------------------------------


def _run_stage(*args: str) -> None:
    """Run one pipeline stage in a clean Python process."""
    command = [sys.executable, "-m", "retail_pricing.pipeline", *args]
    print(f"\n{'=' * 80}\nRunning: {' '.join(command)}\n{'=' * 80}")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def _reset_directory(path: Path) -> None:
    """Delete and recreate one derived data directory."""
    if path.exists():
        print(f"Removing existing directory: {path}")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    print(f"Created clean directory: {path}")


def run_all(*, rebuild_raw: bool = False) -> None:
    """Run Raw -> Silver -> Gold -> parity validation.

    Raw is either validated or safely rebuilt from PostgreSQL. Silver and Gold are
    always rebuilt from scratch, and only after Raw has passed.
    """
    started_at = time.perf_counter()
    banner = "=" * 80
    print(f"\n{banner}\nRETAIL END-TO-END DATA PIPELINE\n{banner}")

    print("\n[1/4] RAW")
    if rebuild_raw:
        print("Rebuilding Raw from PostgreSQL.")
        _run_stage("raw", "--rebuild")
    else:
        print("Using and validating existing Raw snapshot.")
        _run_stage("raw")

    print("\n[2/4] SILVER\nResetting existing Silver outputs.")
    _reset_directory(SILVER_DIR)
    _run_stage("silver")

    print("\n[3/4] GOLD\nResetting existing Gold outputs.")
    _reset_directory(GOLD_DIR)
    _run_stage("gold")

    print("\n[4/4] PARITY CHECK\nComparing rebuilt outputs with the reviewed baseline.")
    _run_stage("parity")

    print(f"\n{banner}\nFULL PIPELINE COMPLETE\n{banner}")
    for stage in ("Raw", "Silver", "Gold", "Parity"):
        print(f"{stage + ':':<11}PASS")
    print(f"Duration: {time.perf_counter() - started_at:.1f}s")


# --- Command line ---------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m retail_pricing.pipeline",
        description="Run one stage of the Raw -> Silver -> Gold pipeline.",
    )
    stages = parser.add_subparsers(dest="stage", required=True)

    raw = stages.add_parser("raw", help="Validate the Raw snapshot (or re-extract it).")
    raw.add_argument(
        "--rebuild",
        action="store_true",
        help=(
            "Re-extract all Raw source tables from PostgreSQL "
            "into a validated staging snapshot before replacing "
            "the current Raw layer."
        ),
    )

    silver = stages.add_parser("silver", help="Build and publish the Silver layer.")
    silver.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    silver.add_argument("--silver-dir", type=Path, default=SILVER_DIR)

    stages.add_parser("gold", help="Build and write the Gold model.")

    parity = stages.add_parser(
        "parity", help="Compare the persisted layers with the reviewed baseline."
    )
    parity.add_argument(
        "--update-baseline",
        action="store_true",
        help="Accept the current numbers as the reviewed baseline.",
    )

    everything = stages.add_parser(
        "all", help="Run raw, silver, gold and parity, each in its own process."
    )
    everything.add_argument(
        "--rebuild-raw",
        action="store_true",
        help="Re-extract Raw from PostgreSQL before rebuilding Silver and Gold.",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )

    if args.stage == "raw":
        run_raw_pipeline(rebuild=args.rebuild)
    elif args.stage == "silver":
        run_silver_pipeline(args.raw_dir, args.silver_dir)
    elif args.stage == "gold":
        run_gold_pipeline()
    elif args.stage == "parity":
        from retail_pricing.parity import run_parity_check

        run_parity_check(accept=args.update_baseline)
    else:
        run_all(rebuild_raw=args.rebuild_raw)


if __name__ == "__main__":
    main()
