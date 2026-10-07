"""Structural contracts of the Gold model (Belgian client mart).

These checks must hold for any snapshot of the data: unique surrogate and natural
keys, fact grains, foreign keys, an intact category bridge, a mart without unused
members and a consistent latest-observation flag. They run before anything is
written.

Snapshot-specific numbers (row counts, reviewed metrics) are deliberately not
here: they live in ``parity_baseline.json`` and are checked by ``parity``, so a new
Raw snapshot needs a reviewed baseline, not a code change.
"""

from __future__ import annotations

import duckdb
import polars as pl

# Surrogate key and natural key of every dimension.
DIMENSION_KEYS = {
    "dim_date": ("date_key", ["date"]),
    "dim_shop": ("shop_key", ["shop"]),
    "dim_product": ("product_key", ["daltix_id", "shop", "country"]),
    "dim_category": ("category_key", ["shop", "country", "category_path_normalized"]),
    "dim_location": ("location_key", ["shop", "location"]),
    "dim_nutrient": ("nutrient_key", ["nutrient_name"]),
}

UNKNOWN_PRODUCT_KEY = -1
LOCATION_TYPES = ("National price (online)", "Store")


def _require(condition: bool, message: str) -> None:
    """Raise a clear error when a Gold contract is violated."""
    if not condition:
        raise ValueError(f"Gold validation failed: {message}")


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Run a query that returns one number."""
    return con.sql(sql).fetchone()[0]


# --- Dimensions ----------------------------------------------------------------


def _validate_dimension_keys(dimensions: dict[str, pl.DataFrame]) -> None:
    """Every dimension is non-empty and unique on its surrogate and natural key."""
    for name, (surrogate, natural) in DIMENSION_KEYS.items():
        frame = dimensions[name]
        _require(frame.height > 0, f"{name} is empty")
        _require(
            frame[surrogate].n_unique() == frame.height,
            f"{name} {surrogate} is not unique",
        )
        # The reserved Unknown product has no natural key.
        members = (
            frame.filter(pl.col(surrogate) != UNKNOWN_PRODUCT_KEY)
            if name == "dim_product"
            else frame
        )
        distinct = members.select(pl.struct(natural).n_unique()).item()
        _require(distinct == members.height, f"{name} natural key is not unique")

    dim_product = dimensions["dim_product"]
    _require(
        dim_product.filter(pl.col("product_key") == UNKNOWN_PRODUCT_KEY).height == 1,
        "dim_product must contain exactly one Unknown member",
    )
    _require(
        "categories" not in dim_product.columns,
        "categories must not be embedded in dim_product",
    )


def _validate_category_model(
    con: duckdb.DuckDBPyConnection,
    dim_product: pl.DataFrame,
    dim_category: pl.DataFrame,
    bridge_product_category: pl.DataFrame,
) -> None:
    """The bridge holds each membership once and points only at existing members."""
    con.register("dim_product_validation", dim_product)
    con.register("dim_category_validation", dim_category)
    con.register("bridge_product_category_validation", bridge_product_category)

    duplicate_memberships = _count(
        con,
        """
        SELECT COUNT(*) - COUNT(DISTINCT (product_key, category_key))
        FROM bridge_product_category_validation
        """,
    )
    orphan_products = _count(
        con,
        """
        SELECT COUNT(*)
        FROM bridge_product_category_validation b
        LEFT JOIN dim_product_validation p ON b.product_key = p.product_key
        WHERE p.product_key IS NULL
        """,
    )
    orphan_categories = _count(
        con,
        """
        SELECT COUNT(*)
        FROM bridge_product_category_validation b
        LEFT JOIN dim_category_validation c ON b.category_key = c.category_key
        WHERE c.category_key IS NULL
        """,
    )
    unused_categories = _count(
        con,
        """
        SELECT COUNT(*)
        FROM dim_category_validation c
        LEFT JOIN bridge_product_category_validation b
            ON c.category_key = b.category_key
        WHERE b.category_key IS NULL
        """,
    )
    unknown_memberships = _count(
        con,
        f"""
        SELECT COUNT(*)
        FROM bridge_product_category_validation
        WHERE product_key = {UNKNOWN_PRODUCT_KEY}
        """,
    )

    _require(duplicate_memberships == 0, "bridge contains duplicate memberships")
    _require(orphan_products == 0, "bridge contains orphan product keys")
    _require(orphan_categories == 0, "bridge contains orphan category keys")
    _require(unused_categories == 0, "dim_category contains unused categories")
    _require(unknown_memberships == 0, "Unknown Product must not be in the bridge")


# --- Facts ---------------------------------------------------------------------


def _validate_fact_contracts(
    con: duckdb.DuckDBPyConnection, dimensions: dict[str, pl.DataFrame]
) -> None:
    """Fact grains are unique and every foreign key finds its dimension member."""
    for name, frame in dimensions.items():
        con.register(f"{name}_validation", frame)

    price_rows, price_grains, unknown_product_rows = con.sql(
        f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT (source_daltix_id, shop_key, location_key, date_key)),
            COUNT(*) FILTER (WHERE product_key = {UNKNOWN_PRODUCT_KEY})
        FROM fact_weekly_prices
        """
    ).fetchone()
    nutrition_rows, nutrition_grains = con.sql(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT (product_key, date_key, nutrient_key, nutrition_basis))
        FROM fact_product_nutrition
        """
    ).fetchone()

    _require(price_rows > 0, "fact_weekly_prices is empty")
    _require(price_grains == price_rows, "fact_weekly_prices grain is not unique")
    _require(
        unknown_product_rows == 0,
        "Unknown Product must not be used by any price row",
    )
    _require(nutrition_rows > 0, "fact_product_nutrition is empty")
    _require(
        nutrition_grains == nutrition_rows, "fact_product_nutrition grain is not unique"
    )

    pricing_fk = _count(
        con,
        """
        SELECT COUNT(*)
        FROM fact_weekly_prices f
        LEFT JOIN dim_product_validation p ON f.product_key = p.product_key
        LEFT JOIN dim_shop_validation s ON f.shop_key = s.shop_key
        LEFT JOIN dim_location_validation l ON f.location_key = l.location_key
        LEFT JOIN dim_date_validation d ON f.date_key = d.date_key
        WHERE p.product_key IS NULL
           OR s.shop_key IS NULL
           OR l.location_key IS NULL
           OR d.date_key IS NULL
        """,
    )
    nutrition_fk = _count(
        con,
        """
        SELECT COUNT(*)
        FROM fact_product_nutrition f
        LEFT JOIN dim_product_validation p ON f.product_key = p.product_key
        LEFT JOIN dim_shop_validation s ON f.shop_key = s.shop_key
        LEFT JOIN dim_date_validation d ON f.date_key = d.date_key
        LEFT JOIN dim_nutrient_validation n ON f.nutrient_key = n.nutrient_key
        WHERE p.product_key IS NULL
           OR s.shop_key IS NULL
           OR d.date_key IS NULL
           OR n.nutrient_key IS NULL
        """,
    )
    _require(pricing_fk == 0, "fact_weekly_prices contains invalid foreign keys")
    _require(nutrition_fk == 0, "fact_product_nutrition contains invalid foreign keys")


# --- Mart definition -----------------------------------------------------------


def _validate_mart_scope(
    con: duckdb.DuckDBPyConnection,
    dimensions: dict[str, pl.DataFrame],
    market: str,
) -> None:
    """One market, inferred products flagged, and no member without a fact."""
    for name, frame in dimensions.items():
        con.register(f"{name}_scope", frame)

    inferred = dimensions["dim_product"].filter(pl.col("is_inferred_product"))
    _require(
        set(inferred["fmcg_classification_status"].unique()) <= {"missing_evidence"},
        "inferred products must be classified as missing_evidence",
    )

    location_types = ", ".join(f"'{value}'" for value in LOCATION_TYPES)
    (
        foreign_countries,
        ids_crossing_shops,
        unused_products,
        unused_shops,
        unused_locations,
        unused_nutrients,
        unlabelled_locations,
    ) = con.sql(
        f"""
        SELECT
            (
                SELECT COUNT(*)
                FROM dim_product_scope
                WHERE product_key <> {UNKNOWN_PRODUCT_KEY}
                  AND country IS DISTINCT FROM '{market}'
            ),

            (
                SELECT COUNT(*)
                FROM (
                    SELECT source_daltix_id
                    FROM fact_weekly_prices
                    GROUP BY source_daltix_id
                    HAVING COUNT(DISTINCT shop_key) > 1
                )
            ),

            (
                SELECT COUNT(*)
                FROM dim_product_scope
                WHERE product_key <> {UNKNOWN_PRODUCT_KEY}
                  AND product_key NOT IN (
                      SELECT product_key FROM fact_weekly_prices
                  )
                  AND product_key NOT IN (
                      SELECT product_key FROM fact_product_nutrition
                  )
            ),

            (
                SELECT COUNT(*)
                FROM dim_shop_scope
                WHERE shop_key NOT IN (SELECT shop_key FROM fact_weekly_prices)
                  AND shop_key NOT IN (SELECT shop_key FROM fact_product_nutrition)
            ),

            (
                SELECT COUNT(*)
                FROM dim_location_scope
                WHERE location_key NOT IN (
                    SELECT location_key FROM fact_weekly_prices
                )
            ),

            (
                SELECT COUNT(*)
                FROM dim_nutrient_scope
                WHERE nutrient_key NOT IN (
                    SELECT nutrient_key FROM fact_product_nutrition
                )
            ),

            (
                SELECT COUNT(*)
                FROM dim_location_scope
                WHERE location_type IS NULL
                   OR location_type NOT IN ({location_types})
            )
        """
    ).fetchone()

    _require(foreign_countries == 0, f"mart products must all be from '{market}'")
    _require(ids_crossing_shops == 0, "one source id must never appear in two shops")
    _require(unused_products == 0, "dim_product holds members no fact refers to")
    _require(unused_shops == 0, "dim_shop holds members no fact refers to")
    _require(unused_locations == 0, "dim_location holds unpriced locations")
    _require(unused_nutrients == 0, "dim_nutrient holds unused nutrients")
    _require(unlabelled_locations == 0, "every location needs a valid location_type")


def _validate_latest_observation(con: duckdb.DuckDBPyConnection) -> None:
    """Every product, nutrient and basis must have exactly one latest date flagged."""
    without_latest, several_dates = con.sql(
        """
        SELECT
            COUNT(*) FILTER (WHERE latest_rows = 0),
            COUNT(*) FILTER (WHERE latest_dates > 1)

        FROM (
            SELECT
                COUNT(*) FILTER (WHERE is_latest_observation) AS latest_rows,
                COUNT(DISTINCT date_key) FILTER (WHERE is_latest_observation)
                    AS latest_dates

            FROM fact_product_nutrition

            GROUP BY product_key, nutrient_key, nutrition_basis
        )
        """
    ).fetchone()

    _require(without_latest == 0, "a nutrient series has no latest observation")
    _require(several_dates == 0, "a nutrient series has two latest dates")


# --- Entry points --------------------------------------------------------------


def validate_mart_reconciliation(
    con: duckdb.DuckDBPyConnection,
    weekly_prices_path: str,
    nutritionals_path: str,
    market: str = "be",
) -> None:
    """Every Silver row of the mart market must reach its fact table."""
    silver_weeks, silver_nutrition = con.sql(
        f"""
        SELECT
            (
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT daltix_id, shop, location, week
                    FROM read_parquet('{weekly_prices_path}')
                )
            ),
            (
                SELECT COUNT(*)
                FROM read_parquet('{nutritionals_path}')
                WHERE country = '{market}'
            )
        """
    ).fetchone()
    weeks = _count(con, "SELECT COUNT(*) FROM fact_weekly_prices")
    nutrition = _count(con, "SELECT COUNT(*) FROM fact_product_nutrition")

    _require(weeks == silver_weeks, "weekly grains were lost between Silver and Gold")
    _require(
        nutrition == silver_nutrition,
        "Belgian nutrition rows were lost between Silver and Gold",
    )


def validate_gold_model(
    con: duckdb.DuckDBPyConnection,
    *,
    dim_date: pl.DataFrame,
    dim_shop: pl.DataFrame,
    dim_product: pl.DataFrame,
    dim_category: pl.DataFrame,
    bridge_product_category: pl.DataFrame,
    dim_location: pl.DataFrame,
    dim_nutrient: pl.DataFrame,
    market: str = "be",
) -> None:
    """Run the structural Gold contracts; the facts live in the connection."""
    dimensions = {
        "dim_date": dim_date,
        "dim_shop": dim_shop,
        "dim_product": dim_product,
        "dim_category": dim_category,
        "dim_location": dim_location,
        "dim_nutrient": dim_nutrient,
    }
    _validate_dimension_keys(dimensions)
    _validate_category_model(con, dim_product, dim_category, bridge_product_category)
    _validate_fact_contracts(con, dimensions)
    _validate_mart_scope(con, dimensions, market)
    _validate_latest_observation(con)
