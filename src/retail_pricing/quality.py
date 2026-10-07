"""Cleaning rules and quality contracts shared by every Silver builder.

Small, explicit helpers (missing tokens, schema and key checks) plus the
cross-table contracts that run on the finished Silver set before publication.
"""

import logging
from pathlib import Path

import duckdb
import polars as pl

from retail_pricing import SILVER_FILES
from retail_pricing.raw import quote_literal

logger = logging.getLogger(__name__)


# Assessed tokens only. NAN is a real brand; NA can be a legitimate code.
SEMANTIC_NULLS = ("", "null", "none", "n/a", "#n/a")

# Extra "no value" placeholders of the descriptive product columns. Each one was
# found by counting whole-value matches in Raw (notebook 02, section 2), and a value
# only counts when it is the entire field: "x" is a placeholder description, but
# "*mag in vaatwasser" is a real one.
COLUMN_PLACEHOLDERS = {
    "name": ("not provided",),
    "brand": ("not provided", "no brand", "unknown", "*", ".", "geen"),
    "description": ("x", "*", "0", "-", "."),
}


def normalize_text(
    column: str, *, null_tokens: tuple[str, ...] | None = None
) -> pl.Expr:
    """Trim text and turn only the explicitly accepted missing tokens into NULL.

    By default those are the common tokens plus the placeholders known for this
    column.
    """
    if null_tokens is None:
        null_tokens = SEMANTIC_NULLS + COLUMN_PLACEHOLDERS.get(column, ())
    value = pl.col(column).cast(pl.String).str.strip_chars()
    return (
        pl.when(value.is_null() | value.str.to_lowercase().is_in(null_tokens))
        .then(None)
        .otherwise(value)
        .alias(column)
    )


def require_columns(
    df: pl.DataFrame, required: list[str], *, exact: bool = True
) -> None:
    """Reject unreviewed schema changes instead of silently dropping new fields."""
    missing = sorted(set(required) - set(df.columns))
    unexpected = sorted(set(df.columns) - set(required)) if exact else []
    if missing or unexpected:
        raise ValueError(
            f"Source schema changed: missing={missing}, unexpected={unexpected}"
        )


def require_unique(df: pl.DataFrame, key: list[str], name: str) -> None:
    """Validate uniqueness; key completeness is a separate contract."""
    groups = df.group_by(key).len().filter(pl.col("len") > 1).height
    if groups:
        raise ValueError(f"{name}: {groups} duplicated business-key groups found.")


def require_date(df: pl.DataFrame, column: str) -> None:
    """Do not silently truncate a new timestamp schema to dates."""
    if df.schema[column] != pl.Date:
        raise ValueError(
            f"{column}: expected Date, got {df.schema[column]}; review source drift."
        )


def require_finite(df: pl.DataFrame, columns: list[str]) -> None:
    """Allow genuine NULLs, but reject NaN and infinity in populated numbers."""
    for column in columns:
        if df.filter(pl.col(column).is_not_null() & ~pl.col(column).is_finite()).height:
            raise ValueError(f"{column}: populated numeric values must be finite.")


def protect_inputs(raw_path: Path, output_path: Path, *other_inputs: Path) -> None:
    """Never overwrite Raw or a fallback input, including through path aliases."""
    raw, output = raw_path.resolve(), output_path.resolve()
    if output.is_relative_to(raw.parent) or output in {
        p.resolve() for p in other_inputs
    }:
        raise ValueError("Silver destination overlaps a protected input.")


def write_dataset(
    frame: pl.DataFrame,
    path: Path,
    name: str,
    *,
    unique_key: list[str] | None = None,
) -> int:
    """Write one Silver dataset as ZSTD Parquet and verify what reached the disk.

    The file is read back for its row count and, when ``unique_key`` is given, for
    the uniqueness of that key. Returns the number of rows written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path, compression="zstd")
    checks = [pl.len().alias("rows")]
    if unique_key:
        checks.append(pl.struct(*unique_key).n_unique().alias("keys"))
    written = pl.scan_parquet(path).select(checks).collect().row(0, named=True)
    if written["rows"] != frame.height:
        raise RuntimeError(
            f"{name} post-write validation failed: "
            "row count changed during materialization."
        )
    if unique_key and written["keys"] != written["rows"]:
        raise RuntimeError(
            f"{name} post-write validation failed: "
            f"{' + '.join(unique_key)} is not unique."
        )
    return int(written["rows"])


def fail_if_false(checks: dict[str, bool], pipeline_name: str) -> None:
    """Stop on structural violations; log successful validation."""
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"{pipeline_name} contract failed: {failed}")
    logger.info("%s contract passed (%d checks)", pipeline_name, len(checks))


# --- Cross-table contracts -----------------------------------------------------


def _silver_file(silver_dir: Path, dataset: str) -> str:
    """Return the SQL literal of one published Silver file."""
    return quote_literal((silver_dir / SILVER_FILES[dataset]).as_posix())


def _first_row(con: duckdb.DuckDBPyConnection, sql: str) -> dict:
    """Run a one-row aggregate query and return it as a dict."""
    return con.sql(sql).pl().row(0, named=True)


def _check_shop_domain(con: duckdb.DuckDBPyConnection, silver_dir: Path) -> None:
    """Every published table uses the canonical retailer codes and names."""
    # Imported here because reference imports this module.
    from retail_pricing.reference import require_canonical_shops, require_shop_names

    for dataset in SILVER_FILES:
        source = _silver_file(silver_dir, dataset)
        codes = con.sql(f"SELECT DISTINCT shop FROM read_parquet({source})").fetchall()
        require_canonical_shops([row[0] for row in codes])
        require_shop_names(
            con.sql(f"SELECT DISTINCT shop, shop_name FROM read_parquet({source})").pl()
        )


def _weekly_price_product_relationship(
    con: duckdb.DuckDBPyConnection, weekly_prices: str, weekly_products: str
) -> dict:
    """Weekly Prices -> Weekly Products: coverage may be partial, cardinality safe."""
    return _first_row(
        con,
        f"""
        WITH prices AS (
            SELECT * FROM read_parquet({weekly_prices})
        ),

        products AS (
            SELECT daltix_id, shop FROM read_parquet({weekly_products})
        ),

        joined AS (
            SELECT
                p.*,
                pr.daltix_id AS matched_product_id,
                pr.shop AS product_shop

            FROM prices p

            LEFT JOIN products pr
                ON p.daltix_id = pr.daltix_id
        )

        SELECT
            (SELECT COUNT(*) FROM prices) AS price_rows,
            COUNT(*) AS joined_rows,

            COUNT(*) FILTER (
                WHERE matched_product_id IS NOT NULL
            ) AS matched_rows,

            COUNT(*) FILTER (
                WHERE matched_product_id IS NULL
            ) AS unmatched_rows,

            COUNT(*) FILTER (
                WHERE matched_product_id IS NOT NULL
                  AND shop IS DISTINCT FROM product_shop
            ) AS shop_context_mismatches,

            ROUND(
                100.0
                * COUNT(*) FILTER (WHERE matched_product_id IS NOT NULL)
                / COUNT(*),
                2
            ) AS coverage_pct

        FROM joined
        """,
    )


def _weekly_price_location_relationship(
    con: duckdb.DuckDBPyConnection, weekly_prices: str, weekly_locations: str
) -> dict:
    """Weekly Prices -> Weekly Locations: must be complete and add no rows."""
    return _first_row(
        con,
        f"""
        WITH prices AS (
            SELECT * FROM read_parquet({weekly_prices})
        ),

        locations AS (
            SELECT shop, location FROM read_parquet({weekly_locations})
        ),

        joined AS (
            SELECT
                p.*,
                l.location AS matched_location

            FROM prices p

            LEFT JOIN locations l
                ON p.shop = l.shop
               AND p.location = l.location
        )

        SELECT
            (SELECT COUNT(*) FROM prices) AS price_rows,
            COUNT(*) AS joined_rows,

            COUNT(*) FILTER (
                WHERE matched_location IS NOT NULL
            ) AS matched_rows,

            COUNT(*) FILTER (
                WHERE matched_location IS NULL
            ) AS unmatched_rows

        FROM joined
        """,
    )


def _nutrition_product_relationship(
    con: duckdb.DuckDBPyConnection, nutritionals: str, weekly_products: str
) -> dict:
    """Nutritionals -> Weekly Products: overlap may be limited, context must agree."""
    return _first_row(
        con,
        f"""
        WITH nutrition_products AS (
            SELECT DISTINCT daltix_id, shop, country
            FROM read_parquet({nutritionals})
        ),

        weekly_products_ref AS (
            SELECT daltix_id, shop, country
            FROM read_parquet({weekly_products})
        )

        SELECT
            COUNT(DISTINCT n.daltix_id) FILTER (
                WHERE wp.daltix_id IS NOT NULL
            ) AS matched_products,

            COUNT(*) FILTER (
                WHERE wp.daltix_id IS NOT NULL
                  AND (
                      n.shop IS DISTINCT FROM wp.shop
                      OR n.country IS DISTINCT FROM wp.country
                  )
            ) AS context_mismatches

        FROM nutrition_products n

        LEFT JOIN weekly_products_ref wp
            ON n.daltix_id = wp.daltix_id
        """,
    )


def run_cross_table_contracts(silver_dir: Path) -> dict[str, float | int]:
    """Validate critical relationships across the completed Silver layer.

    Hard failures:
        - unexpected row multiplication
        - incomplete weekly location references
        - product-context contradictions

    Coverage limitations are returned as metrics rather than hidden.
    """
    # Imported here because silver_products imports this module.
    from retail_pricing.silver_products import validate_primary_categories

    with duckdb.connect() as con:
        _check_shop_domain(con, silver_dir)

        primary_categories = _silver_file(silver_dir, "product_primary_categories")
        validate_primary_categories(
            con.sql(f"SELECT * FROM read_parquet({primary_categories})").pl()
        )

        weekly_prices = _silver_file(silver_dir, "weekly_prices")
        weekly_products = _silver_file(silver_dir, "weekly_products")
        product = _weekly_price_product_relationship(
            con, weekly_prices, weekly_products
        )
        location = _weekly_price_location_relationship(
            con, weekly_prices, _silver_file(silver_dir, "weekly_locations")
        )
        nutrition = _nutrition_product_relationship(
            con, _silver_file(silver_dir, "nutritionals"), weekly_products
        )

    # Only genuinely structural relationship failures are hard contracts.
    fail_if_false(
        {
            "weekly_prices_not_empty": product["price_rows"] > 0,
            "weekly_product_join_no_row_explosion": product["joined_rows"]
            == product["price_rows"],
            "weekly_product_context_consistent": product["shop_context_mismatches"]
            == 0,
            "weekly_location_join_no_row_explosion": location["joined_rows"]
            == location["price_rows"],
            "weekly_location_reference_complete": location["unmatched_rows"] == 0,
            "nutrition_product_context_consistent": nutrition["context_mismatches"]
            == 0,
        },
        "Cross-Table Silver",
    )

    return {
        "weekly_price_rows": int(product["price_rows"]),
        "weekly_product_joined_rows": int(product["joined_rows"]),
        "weekly_product_matched_rows": int(product["matched_rows"]),
        "weekly_product_context_mismatches": int(product["shop_context_mismatches"]),
        "weekly_location_joined_rows": int(location["joined_rows"]),
        "nutrition_product_context_mismatches": int(nutrition["context_mismatches"]),
        "weekly_product_coverage_pct": float(product["coverage_pct"]),
        "weekly_product_unmatched_rows": int(product["unmatched_rows"]),
        "weekly_location_unmatched_rows": int(location["unmatched_rows"]),
        "matched_nutritional_products": int(nutrition["matched_products"]),
    }
