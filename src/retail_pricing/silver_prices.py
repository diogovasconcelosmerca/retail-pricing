"""Silver price datasets: daily prices and weekly prices.

Only exact duplicate observations are collapsed. Different prices inside one
weekly grain are all kept and flagged; Silver never picks a "true" price.
"""

from pathlib import Path

import duckdb
import polars as pl

from retail_pricing.quality import (
    SEMANTIC_NULLS,
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_date,
    require_finite,
    require_unique,
    write_dataset,
)
from retail_pricing.raw import quote_literal
from retail_pricing.reference import (
    add_shop_names,
    canonical_shop_sql,
    canonicalize_shop,
    shop_name_sql,
)

# --- Prices --------------------------------------------------------------------


PRICES_REQUIRED_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "location",
    "price",
    "promo_price",
    "unit_std",
    "currency",
    "downloaded_on",
]


PRICES_TEXT_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "location",
    "unit_std",
    "currency",
]


PRICES_BUSINESS_KEY = [
    "daltix_id",
    "shop",
    "location",
    "downloaded_on",
]


def _standardise_prices(prices_raw: pl.DataFrame) -> pl.DataFrame:
    """Decode the shop, normalise text and type the prices.

    A NULL ``promo_price`` is preserved: in the source it means "no promotion".
    """
    prices = (
        canonicalize_shop(prices_raw)
        .with_columns([normalize_text(column) for column in PRICES_TEXT_COLUMNS])
        .with_columns(
            [
                pl.col("downloaded_on").cast(pl.Date, strict=True),
                pl.col("price").cast(pl.Float64, strict=True),
                pl.col("promo_price").cast(pl.Float64, strict=True),
            ]
        )
    )
    require_finite(prices, ["price", "promo_price"])
    return prices


def _flag_prices(prices: pl.DataFrame) -> pl.DataFrame:
    """Make the promotion rule explicit and flag odd prices without correcting them."""
    return prices.with_columns(
        [
            (
                pl.col("promo_price").is_not_null()
                & (pl.col("promo_price") < pl.col("price"))
            )
            .fill_null(False)
            .alias("is_promotion"),
            (pl.col("price") <= 0).fill_null(False).alias("dq_non_positive_price"),
            (pl.col("promo_price").is_not_null() & (pl.col("promo_price") <= 0))
            .fill_null(False)
            .alias("dq_non_positive_promo_price"),
            (
                pl.col("promo_price").is_not_null()
                & (pl.col("promo_price") >= pl.col("price"))
            )
            .fill_null(False)
            .alias("dq_promo_not_below_price"),
        ]
    )


def _validate_prices(prices: pl.DataFrame, prices_raw: pl.DataFrame) -> None:
    """Stop before writing if the business grain or a structural rule fails.

    Duplicate observations at the business key would make the source ambiguous.
    ``promo_price`` itself may stay NULL.
    """
    require_unique(prices, PRICES_BUSINESS_KEY, "Prices")
    fail_if_false(
        {
            "row_count_preserved": prices.height == prices_raw.height,
            "daltix_id_complete": prices["daltix_id"].null_count() == 0,
            "shop_complete": prices["shop"].null_count() == 0,
            "country_complete": prices["country"].null_count() == 0,
            "location_complete": prices["location"].null_count() == 0,
            "downloaded_on_complete": prices["downloaded_on"].null_count() == 0,
            "price_complete": prices["price"].null_count() == 0,
            "unit_std_complete": prices["unit_std"].null_count() == 0,
            "currency_complete": prices["currency"].null_count() == 0,
            "business_grain_unique": (
                prices.select(PRICES_BUSINESS_KEY).unique().height == prices.height
            ),
            "regular_prices_positive": prices["dq_non_positive_price"].sum() == 0,
            "populated_promo_prices_positive": prices[
                "dq_non_positive_promo_price"
            ].sum()
            == 0,
        },
        "Prices",
    )


def run_prices_pipeline(
    raw_path: Path,
    silver_path: Path,
) -> dict[str, int | float]:
    """
    Build the Silver Prices dataset.

    Grain:
        One historical price observation per
        daltix_id + shop + location + downloaded_on.

    Business key:
        daltix_id + shop + location + downloaded_on.

    promo_price NULL is preserved because it represents meaningful
    source semantics rather than a value that should be imputed.
    """
    protect_inputs(raw_path, silver_path)
    prices_raw = pl.read_parquet(raw_path)
    require_columns(prices_raw, PRICES_REQUIRED_COLUMNS)
    require_date(prices_raw, "downloaded_on")

    prices = _flag_prices(_standardise_prices(prices_raw))
    _validate_prices(prices, prices_raw)

    prices = add_shop_names(prices)
    written_rows = write_dataset(prices, silver_path, "Prices")

    return {
        "rows": written_rows,
        "rows_without_promo_price": int(prices["promo_price"].is_null().sum()),
        "promotion_rows": int(prices["is_promotion"].sum()),
        "unexpected_promo_rows": int(prices["dq_promo_not_below_price"].sum()),
        "distinct_products": prices["daltix_id"].n_unique(),
        "distinct_locations": prices.select(["shop", "location"]).unique().height,
    }


# --- Weekly prices -------------------------------------------------------------

WEEKLY_PRICES_REQUIRED_COLUMNS = {
    "daltix_id",
    "shop",
    "location",
    "week",
    "price",
    "price_promo",
}

# Sorted by the full observation key so that the file is identical on every run.
WEEKLY_PRICES_SORT = "daltix_id, shop, location, week, price, price_promo"


def _check_weekly_prices_source(con: duckdb.DuckDBPyConnection, raw_sql: str) -> None:
    """Validate the source schema without loading 19M rows into Python."""
    schema = {
        row[0]: row[1]
        for row in con.sql(f"DESCRIBE SELECT * FROM read_parquet({raw_sql})").fetchall()
    }
    if set(schema) != WEEKLY_PRICES_REQUIRED_COLUMNS:
        raise ValueError(
            "Weekly Prices: source columns changed; review before deduplicating."
        )
    if schema["week"] != "DATE":
        raise ValueError("Weekly Prices: week must be Date; timestamps require review.")


def _nullable_text_sql(column: str, null_tokens: str) -> str:
    """Trim a text column and turn the semantic null tokens into NULL."""
    text = f"trim(CAST({column} AS VARCHAR))"
    return f"CASE WHEN lower({text}) IN ({null_tokens}) THEN NULL ELSE {text} END"


def _weekly_prices_query(raw_sql: str) -> str:
    """SQL of the Silver table: standardise, collapse exact duplicates, profile."""
    null_tokens = ", ".join(quote_literal(token) for token in SEMANTIC_NULLS)

    # Standardize source fields directly from Parquet.
    clean_query = f"""
    SELECT
        {_nullable_text_sql("daltix_id", null_tokens)} AS daltix_id,
        {canonical_shop_sql()} AS shop,
        {_nullable_text_sql("location", null_tokens)} AS location,
        CAST(week AS DATE) AS week,
        CAST(price AS DOUBLE) AS price,
        CAST(price_promo AS DOUBLE) AS price_promo

    FROM read_parquet({raw_sql})
    """

    # Collapse only completely identical observations.
    deduped_query = f"""
    WITH cleaned AS ({clean_query})
    SELECT daltix_id, shop, location, week, price, price_promo,
        count(*) > 1 AS dq_source_exact_duplicate
    FROM cleaned
    GROUP BY daltix_id, shop, location, week, price, price_promo
    """

    # Preserve different observations within the same weekly grain
    # and make the resulting ambiguity explicit.
    silver_query = f"""
    WITH deduped AS (
        {deduped_query}
    ),

    profiled AS (
        SELECT
            *,

            -- After exact duplicates are collapsed, every remaining row of a grain
            -- is a different price pair, so the row count is the version count.
            COUNT(*) OVER (
                PARTITION BY daltix_id, shop, location, week
            ) AS price_version_count

        FROM deduped
    )

    SELECT
        daltix_id,
        shop,
        location,
        week,
        price,
        price_promo,

        md5(
            concat_ws(
                '|',
                daltix_id,
                shop,
                location,
                CAST(week AS VARCHAR),
                CAST(price AS VARCHAR),
                CAST(price_promo AS VARCHAR)
            )
        ) AS price_observation_id,

        price_promo < price
            AS is_promotion,

        price <= 0
            AS dq_non_positive_price,

        price_promo <= 0
            AS dq_non_positive_promo_price,

        price_promo > price
            AS dq_promo_above_price,

        dq_source_exact_duplicate,

        price_version_count,

        price_version_count > 1 AS has_multiple_price_versions,

        CASE
            WHEN price_version_count > 1
                THEN 'multiple_price_observations'

            WHEN dq_source_exact_duplicate
                THEN 'exact_duplicate_collapsed'

            ELSE 'single_observation'
        END AS price_resolution_rule

    FROM profiled
    """

    # Add names after all business transformations; shop remains the key.
    return (
        f"SELECT *, {shop_name_sql()} AS shop_name FROM ({silver_query}) named_source"
    )


def _validate_weekly_prices(con: duckdb.DuckDBPyConnection, silver_sql: str) -> None:
    """Validate the persisted table without recomputing its windows."""
    contract_result = con.sql(f"""
    WITH silver AS (
        SELECT * FROM read_parquet({silver_sql})
    )

    SELECT
        COUNT(*) > 0
            AS dataset_not_empty,

        COUNT(*) FILTER (
            WHERE daltix_id IS NULL
               OR shop IS NULL
               OR location IS NULL
               OR week IS NULL
               OR price IS NULL
               OR price_promo IS NULL
        ) = 0
            AS required_fields_complete,

        COUNT(*) FILTER (
            WHERE NOT isfinite(price) OR NOT isfinite(price_promo)
        ) = 0 AS prices_finite,

        COUNT(*) = COUNT(DISTINCT price_observation_id)
            AS observation_fingerprint_unique,

        COUNT(*) FILTER (
            WHERE dq_non_positive_price
        ) = 0
            AS regular_prices_positive,

        COUNT(*) FILTER (
            WHERE dq_non_positive_promo_price
        ) = 0
            AS promo_prices_positive,

        COUNT(*) - COUNT(
            DISTINCT struct_pack(
                daltix_id := daltix_id,
                shop := shop,
                location := location,
                week := week,
                price := price,
                price_promo := price_promo
            )
        ) = 0
            AS exact_duplicates_removed,

        COUNT(*) FILTER (WHERE has_multiple_price_versions
            IS DISTINCT FROM (price_version_count > 1)) = 0
            AS multiple_version_flag_consistent

    FROM silver
    """).pl()
    fail_if_false(
        {
            column: bool(contract_result[column][0])
            for column in contract_result.columns
        },
        "Weekly Prices",
    )


def _weekly_prices_metrics(
    con: duckdb.DuckDBPyConnection, silver_sql: str, raw_sql: str
) -> dict[str, int]:
    """Business and quality metrics read from the persisted Silver file."""
    metrics = (
        con.sql(f"""
        WITH silver AS (
            SELECT *
            FROM read_parquet({silver_sql})
        ),

        raw AS (
            SELECT *
            FROM read_parquet({raw_sql})
        )

        SELECT
            (SELECT COUNT(*) FROM raw)
                AS raw_rows,

            COUNT(*)
                AS silver_rows,

            (SELECT COUNT(*) FROM raw)
                - COUNT(*)
                AS exact_duplicates_removed,

            COUNT(*) FILTER (
                WHERE has_multiple_price_versions
            ) AS multiple_version_rows,

            COUNT(
                DISTINCT CASE
                    WHEN has_multiple_price_versions
                    THEN struct_pack(
                        daltix_id := daltix_id,
                        shop := shop,
                        location := location,
                        week := week
                    )
                END
            ) AS multiple_version_grains,

            COUNT(*) FILTER (
                WHERE is_promotion
            ) AS promotion_rows,

            COUNT(*) FILTER (
                WHERE dq_promo_above_price
            ) AS promo_above_price_rows

        FROM silver
    """)
        .pl()
        .row(0, named=True)
    )
    return {
        "raw_rows": int(metrics["raw_rows"]),
        "rows": int(metrics["silver_rows"]),
        "exact_duplicates_removed": int(metrics["exact_duplicates_removed"]),
        "multiple_price_version_rows": int(metrics["multiple_version_rows"]),
        "multiple_price_version_grains": int(metrics["multiple_version_grains"]),
        "promotion_rows": int(metrics["promotion_rows"]),
        "promo_above_price_rows": int(metrics["promo_above_price_rows"]),
    }


def run_weekly_prices_pipeline(
    raw_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """Build the Silver Weekly Prices dataset.

    Expected business grain:
        daltix_id + shop + location + week.

    Exact duplicate rows:
        Collapsed.

    Different prices within the same weekly grain:
        Preserved as multiple price versions. These may reflect real intra-week
        variation; the weekly timestamp cannot establish the cause or sequence.
        ``has_multiple_price_versions`` describes that structure, not invalidity.
    """
    protect_inputs(raw_path, silver_path)
    raw_sql = quote_literal(raw_path.as_posix())
    silver_sql = quote_literal(silver_path.as_posix())

    with duckdb.connect() as con:
        _check_weekly_prices_source(con, raw_sql)

        # The orchestrator publishes only after every staged output passes its
        # contracts.
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        con.sql(
            f"COPY (SELECT * FROM ({_weekly_prices_query(raw_sql)}) "
            f"ORDER BY {WEEKLY_PRICES_SORT}) "
            f"TO {silver_sql} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        _validate_weekly_prices(con, silver_sql)
        return _weekly_prices_metrics(con, silver_sql, raw_sql)
