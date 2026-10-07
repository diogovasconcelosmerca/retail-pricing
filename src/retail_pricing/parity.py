"""Regression check of the persisted layers against a reviewed baseline.

The published Raw, Silver and Gold files are compared with
``parity_baseline.json``: row counts, column lists, an order-independent content
fingerprint per file and the key business metrics. A second group, the
invariants (unique keys, fact grains, foreign keys, category bridge), must hold
by construction and needs no baseline.

The baseline records one reviewed snapshot. A legitimate change (new source
data, a deliberate model change) fails here until the new numbers are reviewed
and accepted with ``--update-baseline``. Fingerprints use DuckDB's ``hash`` and
are pinned to the locked environment (uv.lock); they ignore row order and
Parquet layout, which DuckDB does not guarantee between runs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from retail_pricing import GOLD_DIR, RAW_DIR, SILVER_DIR, SILVER_FILES
from retail_pricing.raw import TABLES as RAW_TABLES

BASELINE_PATH = Path(__file__).with_name("parity_baseline.json")

GOLD_TABLES = (
    "dim_date",
    "dim_shop",
    "dim_product",
    "dim_category",
    "bridge_product_category",
    "dim_location",
    "dim_nutrient",
    "fact_weekly_prices",
    "fact_product_nutrition",
)

# Key business metrics: a scalar SQL query per name, compared with the baseline.
METRICS = {
    # Silver grains established during discovery.
    "silver_products.distinct_daltix_id": (
        "SELECT COUNT(DISTINCT daltix_id) FROM silver_products"
    ),
    "silver_prices.distinct_source_grains": (
        "SELECT COUNT(DISTINCT (daltix_id, shop, location, downloaded_on)) "
        "FROM silver_prices"
    ),
    "silver_weekly_products.distinct_identities": (
        "SELECT COUNT(DISTINCT (daltix_id, shop, country)) FROM silver_weekly_products"
    ),
    "silver_weekly_locations.distinct_locations": (
        "SELECT COUNT(DISTINCT (shop, location)) FROM silver_weekly_locations"
    ),
    # Silver data-quality flags.
    "silver_nutritionals.mass_exceeds_portion_rows": (
        "SELECT COUNT(*) FILTER (WHERE dq_mass_exceeds_portion) "
        "FROM silver_nutritionals"
    ),
    "silver_nutritionals.negative_value_rows": (
        "SELECT COUNT(*) FILTER (WHERE dq_negative_nutrient_value) "
        "FROM silver_nutritionals"
    ),
    "silver_nutritionals.energy_mismatch_rows": (
        "SELECT COUNT(*) FILTER (WHERE dq_energy_kcal_mismatch) "
        "FROM silver_nutritionals"
    ),
    "silver_category_paths.duplicate_paths": (
        "SELECT COUNT(*) FILTER (WHERE dq_duplicate_category_path) "
        "FROM silver_product_category_paths"
    ),
    "silver_category_paths.repeated_label_paths": (
        "SELECT COUNT(*) FILTER (WHERE dq_repeated_category_label) "
        "FROM silver_product_category_paths"
    ),
    "silver_category_paths.prefix_paths": (
        "SELECT COUNT(*) FILTER (WHERE is_prefix_of_another_path) "
        "FROM silver_product_category_paths"
    ),
    "silver_category_paths.navigation_paths": (
        "SELECT COUNT(*) FILTER (WHERE dq_navigation_contamination) "
        "FROM silver_product_category_paths"
    ),
    # Gold natural keys.
    "dim_product.identified_natural_keys": (
        "SELECT COUNT(DISTINCT (daltix_id, shop, country)) FROM dim_product "
        "WHERE product_key <> -1"
    ),
    "dim_category.natural_keys": (
        "SELECT COUNT(DISTINCT (shop, country, category_path_normalized)) "
        "FROM dim_category"
    ),
    "dim_location.natural_keys": (
        "SELECT COUNT(DISTINCT (shop, location)) FROM dim_location"
    ),
    "dim_nutrient.natural_keys": (
        "SELECT COUNT(DISTINCT nutrient_name) FROM dim_nutrient"
    ),
    # Gold category bridge.
    "bridge.products_with_category": (
        "SELECT COUNT(DISTINCT product_key) FROM bridge_product_category"
    ),
    "bridge.products_with_multiple_categories": (
        "SELECT COUNT(*) FROM (SELECT product_key FROM bridge_product_category "
        "GROUP BY product_key HAVING COUNT(*) > 1)"
    ),
    "bridge.max_categories_per_product": (
        "SELECT MAX(n) FROM (SELECT COUNT(*) AS n FROM bridge_product_category "
        "GROUP BY product_key)"
    ),
    # Gold weekly-price rules.
    "fact_weekly_prices.intraweek_variation_rows": (
        "SELECT COUNT(*) FILTER (WHERE has_intraweek_price_variation) "
        "FROM fact_weekly_prices"
    ),
    "fact_weekly_prices.effective_price_variation_rows": (
        "SELECT COUNT(*) FILTER (WHERE has_effective_price_variation) "
        "FROM fact_weekly_prices"
    ),
    "fact_weekly_prices.promotion_state_conflict_rows": (
        "SELECT COUNT(*) FILTER (WHERE promotion_state_conflict) "
        "FROM fact_weekly_prices"
    ),
    "fact_weekly_prices.usable_effective_price_rows": (
        "SELECT COUNT(*) FILTER (WHERE effective_price IS NOT NULL) "
        "FROM fact_weekly_prices"
    ),
    "fact_weekly_prices.calculable_discount_rows": (
        "SELECT COUNT(*) FILTER (WHERE discount_pct IS NOT NULL) "
        "FROM fact_weekly_prices"
    ),
    "dim_product.inferred_members": (
        "SELECT COUNT(*) FROM dim_product WHERE is_inferred_product"
    ),
    "fact_weekly_prices.inferred_product_rows": (
        "SELECT COUNT(*) FROM fact_weekly_prices f "
        "INNER JOIN dim_product p USING (product_key) WHERE p.is_inferred_product"
    ),
    "fact_weekly_prices.unknown_product_rows": (
        "SELECT COUNT(*) FILTER (WHERE product_key = -1) FROM fact_weekly_prices"
    ),
    # Gold nutrition rules.
    "fact_product_nutrition.usable_value_rows": (
        "SELECT COUNT(*) FILTER (WHERE is_value_usable) FROM fact_product_nutrition"
    ),
    "fact_product_nutrition.unusable_value_rows": (
        "SELECT COUNT(*) FILTER (WHERE NOT is_value_usable) FROM fact_product_nutrition"
    ),
    "fact_product_nutrition.plausibility_flag_rows": (
        "SELECT COUNT(*) FILTER (WHERE dq_mass_exceeds_portion) "
        "FROM fact_product_nutrition"
    ),
    "fact_product_nutrition.negative_value_rows": (
        "SELECT COUNT(*) FILTER (WHERE dq_negative_nutrient_value) "
        "FROM fact_product_nutrition"
    ),
    "fact_product_nutrition.latest_observation_rows": (
        "SELECT COUNT(*) FILTER (WHERE is_latest_observation) "
        "FROM fact_product_nutrition"
    ),
    "fact_product_nutrition.energy_mismatch_rows": (
        "SELECT COUNT(*) FILTER (WHERE dq_energy_kcal_mismatch) "
        "FROM fact_product_nutrition"
    ),
    # Temporal coverage of each fact and the period they share.
    "pricing.first_week": (
        "SELECT CAST(MIN(d.date) AS VARCHAR) FROM fact_weekly_prices f "
        "JOIN dim_date d ON f.date_key = d.date_key"
    ),
    "pricing.last_week": (
        "SELECT CAST(MAX(d.date) AS VARCHAR) FROM fact_weekly_prices f "
        "JOIN dim_date d ON f.date_key = d.date_key"
    ),
    "nutrition.first_observation": (
        "SELECT CAST(MIN(d.date) AS VARCHAR) FROM fact_product_nutrition f "
        "JOIN dim_date d ON f.date_key = d.date_key"
    ),
    "nutrition.last_observation": (
        "SELECT CAST(MAX(d.date) AS VARCHAR) FROM fact_product_nutrition f "
        "JOIN dim_date d ON f.date_key = d.date_key"
    ),
    "shared_period.start": (
        "SELECT CAST(GREATEST("
        "(SELECT MIN(d.date) FROM fact_weekly_prices f "
        "JOIN dim_date d ON f.date_key = d.date_key), "
        "(SELECT MIN(d.date) FROM fact_product_nutrition f "
        "JOIN dim_date d ON f.date_key = d.date_key)) AS VARCHAR)"
    ),
    "shared_period.end": (
        "SELECT CAST(LEAST("
        "(SELECT MAX(d.date) FROM fact_weekly_prices f "
        "JOIN dim_date d ON f.date_key = d.date_key), "
        "(SELECT MAX(d.date) FROM fact_product_nutrition f "
        "JOIN dim_date d ON f.date_key = d.date_key)) AS VARCHAR)"
    ),
    # Every Unknown Product carries an explicit FMCG outcome.
    "dim_product.unknown_member_unclassified": (
        "SELECT COUNT(*) FROM dim_product WHERE product_key = -1 "
        "AND primary_fmcg_category_code = 'unclassified' "
        "AND fmcg_classification_status = 'unknown_product'"
    ),
}

# Each query counts violations and must return 0.
INVARIANTS = {
    "dim_date: date_key unique": (
        "SELECT COUNT(*) - COUNT(DISTINCT date_key) FROM dim_date"
    ),
    "dim_shop: shop_key unique": (
        "SELECT COUNT(*) - COUNT(DISTINCT shop_key) FROM dim_shop"
    ),
    "dim_product: product_key unique": (
        "SELECT COUNT(*) - COUNT(DISTINCT product_key) FROM dim_product"
    ),
    "dim_category: category_key unique": (
        "SELECT COUNT(*) - COUNT(DISTINCT category_key) FROM dim_category"
    ),
    "dim_location: location_key unique": (
        "SELECT COUNT(*) - COUNT(DISTINCT location_key) FROM dim_location"
    ),
    "dim_nutrient: nutrient_key unique": (
        "SELECT COUNT(*) - COUNT(DISTINCT nutrient_key) FROM dim_nutrient"
    ),
    "fact_weekly_prices: grain unique": (
        "SELECT COUNT(*) - COUNT(DISTINCT "
        "(source_daltix_id, shop_key, location_key, date_key)) "
        "FROM fact_weekly_prices"
    ),
    "fact_product_nutrition: grain unique": (
        "SELECT COUNT(*) - COUNT(DISTINCT "
        "(product_key, shop_key, date_key, nutrient_key, nutrition_basis)) "
        "FROM fact_product_nutrition"
    ),
    "fact_weekly_prices: orphan foreign keys": (
        "SELECT COUNT(*) FROM fact_weekly_prices f "
        "LEFT JOIN dim_product p ON f.product_key = p.product_key "
        "LEFT JOIN dim_shop s ON f.shop_key = s.shop_key "
        "LEFT JOIN dim_location l ON f.location_key = l.location_key "
        "LEFT JOIN dim_date d ON f.date_key = d.date_key "
        "WHERE p.product_key IS NULL OR s.shop_key IS NULL "
        "OR l.location_key IS NULL OR d.date_key IS NULL"
    ),
    "fact_product_nutrition: orphan foreign keys": (
        "SELECT COUNT(*) FROM fact_product_nutrition f "
        "LEFT JOIN dim_product p ON f.product_key = p.product_key "
        "LEFT JOIN dim_shop s ON f.shop_key = s.shop_key "
        "LEFT JOIN dim_date d ON f.date_key = d.date_key "
        "LEFT JOIN dim_nutrient n ON f.nutrient_key = n.nutrient_key "
        "WHERE p.product_key IS NULL OR s.shop_key IS NULL "
        "OR d.date_key IS NULL OR n.nutrient_key IS NULL"
    ),
    "bridge: duplicate memberships": (
        "SELECT COUNT(*) - COUNT(DISTINCT (product_key, category_key)) "
        "FROM bridge_product_category"
    ),
    "bridge: orphan product keys": (
        "SELECT COUNT(*) FROM bridge_product_category b "
        "LEFT JOIN dim_product p ON b.product_key = p.product_key "
        "WHERE p.product_key IS NULL"
    ),
    "bridge: orphan category keys": (
        "SELECT COUNT(*) FROM bridge_product_category b "
        "LEFT JOIN dim_category c ON b.category_key = c.category_key "
        "WHERE c.category_key IS NULL"
    ),
    "bridge: unused categories": (
        "SELECT COUNT(*) FROM dim_category c "
        "LEFT JOIN bridge_product_category b ON c.category_key = b.category_key "
        "WHERE b.category_key IS NULL"
    ),
    "bridge: Unknown Product memberships": (
        "SELECT COUNT(*) FROM bridge_product_category WHERE product_key = -1"
    ),
    "dim_product: FMCG decision differs from Silver": (
        "SELECT COUNT(*) FROM dim_product g "
        "LEFT JOIN silver_product_primary_categories s "
        "USING (daltix_id, shop, country) "
        "WHERE g.product_key <> -1 AND NOT g.is_inferred_product AND ("
        "s.daltix_id IS NULL "
        "OR g.primary_fmcg_category_code "
        "IS DISTINCT FROM s.primary_fmcg_category_code "
        "OR g.primary_fmcg_category_en "
        "IS DISTINCT FROM s.primary_fmcg_category_en "
        "OR g.fmcg_classification_status "
        "IS DISTINCT FROM s.fmcg_classification_status "
        "OR g.fmcg_mapping_version IS DISTINCT FROM s.fmcg_mapping_version)"
    ),
}


@dataclass
class Result:
    """One comparison line of the parity report."""

    section: str
    check: str
    actual: object
    expected: object
    passed: bool


def _file_table(silver_dir: Path, gold_dir: Path) -> dict[str, tuple[str, Path]]:
    """Map every persisted table name to its layer and file."""
    tables = {
        f"silver_{name}": ("silver", silver_dir / filename)
        for name, filename in SILVER_FILES.items()
    }
    tables.update(
        {name: ("gold", gold_dir / f"{name}.parquet") for name in GOLD_TABLES}
    )
    return tables


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> object:
    """Run one scalar query; a failure is recorded, not raised."""
    try:
        return con.sql(sql).fetchone()[0]
    except Exception as exc:  # noqa: BLE001 - report the failure and finish the run
        return f"ERROR: {exc}"


def content_fingerprint(con: duckdb.DuckDBPyConnection, table: str) -> str:
    """Order-independent fingerprint: row count, sum and xor of per-row hashes."""
    rows, total, xored = con.sql(
        f"SELECT count(*), sum(hash(t)), bit_xor(hash(t)) FROM {table} t"
    ).fetchone()
    return f"{rows}:{total:x}:{xored:x}"


def measure(
    raw_dir: Path = RAW_DIR,
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
) -> dict:
    """Read the persisted files and return everything the baseline covers."""
    actual: dict = {
        "raw": {},
        "silver": {},
        "gold": {},
        "metrics": {},
        "invariants": {},
    }
    for table in RAW_TABLES:
        path = raw_dir / f"{table}.parquet"
        actual["raw"][table] = (
            pq.ParquetFile(path).metadata.num_rows if path.is_file() else None
        )

    with duckdb.connect() as con:
        for name, (layer, path) in _file_table(silver_dir, gold_dir).items():
            if not path.is_file():
                actual[layer][name] = None
                continue
            con.execute(
                f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path.as_posix()}')"
            )
            actual[layer][name] = {
                "rows": pq.ParquetFile(path).metadata.num_rows,
                "columns": [row[0] for row in con.sql(f"DESCRIBE {name}").fetchall()],
                "fingerprint": content_fingerprint(con, name),
            }
        for name, sql in METRICS.items():
            actual["metrics"][name] = _scalar(con, sql)
        for name, sql in INVARIANTS.items():
            actual["invariants"][name] = _scalar(con, sql)
        actual["invariants"]["silver_product_primary_categories: contract"] = (
            _fmcg_contract(con)
        )
    return actual


def _fmcg_contract(con: duckdb.DuckDBPyConnection) -> object:
    """Run the Silver classification contract; 0 means it holds."""
    from retail_pricing.silver_products import validate_primary_categories

    try:
        validate_primary_categories(
            con.sql("SELECT * FROM silver_product_primary_categories").pl()
        )
        return 0
    except Exception as exc:  # noqa: BLE001 - report the failure and finish the run
        return f"ERROR: {exc}"


def compare(actual: dict, baseline: dict) -> list[Result]:
    """Turn measurements and the reviewed baseline into a list of results."""
    results: list[Result] = []

    def add(section: str, check: str, got: object, expected: object) -> None:
        results.append(Result(section, check, got, expected, got == expected))

    for table, expected_rows in baseline["raw"].items():
        add("Raw", f"{table} rows", actual["raw"].get(table), expected_rows)

    for layer in ("silver", "gold"):
        for table, expected in baseline[layer].items():
            found = actual[layer].get(table)
            add(layer.title(), f"{table} exists", found is not None, True)
            if found is None:
                continue
            add(layer.title(), f"{table} rows", found["rows"], expected["rows"])
            add(
                layer.title(), f"{table} columns", found["columns"], expected["columns"]
            )
            add(
                layer.title(),
                f"{table} content fingerprint",
                found["fingerprint"],
                expected["fingerprint"],
            )

    for name, expected in baseline["metrics"].items():
        add("Metrics", name, actual["metrics"].get(name), expected)
    for name, got in actual["invariants"].items():
        add("Invariants", name, got, 0)
    return results


def update_baseline(actual: dict, path: Path = BASELINE_PATH) -> None:
    """Accept the current numbers as the reviewed baseline."""
    broken = {k: v for k, v in actual["invariants"].items() if v != 0}
    if broken or any(v is None for v in actual["raw"].values()):
        raise SystemExit(
            f"Refusing to accept a baseline with broken invariants: {broken}"
        )
    baseline = {
        "baseline_version": 1,
        "raw": actual["raw"],
        "silver": actual["silver"],
        "gold": actual["gold"],
        "metrics": actual["metrics"],
    }
    path.write_text(json.dumps(baseline, indent=2) + "\n", encoding="utf-8")


def print_report(results: list[Result]) -> None:
    """Print a compact report, one line per check."""
    print()
    print("=" * 100)
    print("PERSISTED-OUTPUT PARITY CHECK")
    print("=" * 100)
    section = None
    for result in results:
        if result.section != section:
            section = result.section
            print(f"\n[{section}]")
        status = "PASS" if result.passed else "FAIL"
        shown = str(result.actual)
        shown = shown if len(shown) <= 60 else shown[:57] + "..."
        expected = str(result.expected)[:60]
        print(
            f"{status:<4} | {result.check:<55} | actual={shown} | expected={expected}"
        )
    failed = sum(not r.passed for r in results)
    print()
    print("=" * 100)
    print(f"Checks: {len(results)} | PASS: {len(results) - failed} | FAIL: {failed}")
    print("PARITY CHECK: FAIL" if failed else "PARITY CHECK: PASS")
    print("=" * 100)


def run_parity_check(*, accept: bool = False) -> None:
    """Measure the persisted layers and compare them with the baseline.

    Silver's own manifest is verified first: it must be complete, built from the
    current code and match every published file.
    """
    from retail_pricing.pipeline import load_silver_manifest

    actual = measure()
    if accept:
        update_baseline(actual)
        print(f"Baseline written to {BASELINE_PATH}")
        return
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    results = compare(actual, baseline)
    try:
        load_silver_manifest(SILVER_DIR)
        manifest_ok: object = True
    except Exception as exc:  # noqa: BLE001 - report the failure and finish the run
        manifest_ok = f"ERROR: {exc}"
    results.insert(
        0,
        Result(
            "Silver manifest",
            "complete, current and matches files",
            manifest_ok,
            True,
            manifest_ok is True,
        ),
    )
    print_report(results)
    if any(not result.passed for result in results):
        raise SystemExit(1)
