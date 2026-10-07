"""Gold builders: the Belgian client mart with its dimensions, categories and facts.

Silver keeps every market (BE, NL, LU, DE). Gold publishes the mart of one client
market: the weekly-price process is already Belgian, nutrition is limited to
products whose country is ``MART_COUNTRY``, and each dimension holds only the
members that a fact table refers to (plus the reserved Unknown Product).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from retail_pricing.fmcg_taxonomy import CATEGORY_LABELS, MAPPING_VERSION
from retail_pricing.reference import require_shop_names
from retail_pricing.silver_products import (
    IDENTITY,
    REPORT_FIELDS,
    validate_primary_categories,
)

# Country of the client mart: the weekly-price process is Belgian, and nutrition
# rows count only for products whose own country is Belgium.
MART_COUNTRY = "be"

# Descriptive source of a product that exists only because prices refer to it.
INFERRED_SOURCE = "inferred_from_prices"


def _sql_path(path: str | Path) -> str:
    """Return a DuckDB-safe POSIX path."""
    return Path(path).as_posix().replace("'", "''")


# --- Dimensions ----------------------------------------------------------------
# Surrogate keys number the rows of the sorted natural identity, so they are
# repeatable for the same population.


def _add_surrogate_key(frame: pl.DataFrame, key: str) -> pl.DataFrame:
    """Number the rows 1..n in their current order and put the key first."""
    return frame.with_row_index(key, offset=1).with_columns(pl.col(key).cast(pl.Int64))


def build_dim_date(
    weekly_prices_path: str | Path,
    nutritionals_path: str | Path,
) -> pl.DataFrame:
    """Build the shared calendar dimension covering all required calendar years."""

    weekly_prices = _sql_path(weekly_prices_path)
    nutritionals = _sql_path(nutritionals_path)

    with duckdb.connect() as con:
        return con.sql(
            f"""
            WITH source_bounds AS (
                SELECT
                    LEAST(
                        (
                            SELECT MIN(week)
                            FROM read_parquet('{weekly_prices}')
                        ),
                        (
                            SELECT MIN(download_date)
                            FROM read_parquet('{nutritionals}')
                        )
                    ) AS min_date,

                    GREATEST(
                        (
                            SELECT MAX(week)
                            FROM read_parquet('{weekly_prices}')
                        ),
                        (
                            SELECT MAX(download_date)
                            FROM read_parquet('{nutritionals}')
                        )
                    ) AS max_date
            ),

            calendar_bounds AS (
                SELECT
                    CAST(
                        date_trunc('year', min_date)
                        AS DATE
                    ) AS calendar_start,

                    CAST(
                        date_trunc('year', max_date)
                        + INTERVAL '1 year'
                        - INTERVAL '1 day'
                        AS DATE
                    ) AS calendar_end

                FROM source_bounds
            ),

            calendar AS (
                SELECT
                    CAST(gs AS DATE) AS date

                FROM calendar_bounds,
                generate_series(
                    calendar_start,
                    calendar_end,
                    INTERVAL '1 day'
                ) AS t(gs)
            )

            SELECT
                CAST(
                    strftime(date, '%Y%m%d')
                    AS INTEGER
                ) AS date_key,

                date,

                CAST(year(date) AS INTEGER) AS year,
                CAST(quarter(date) AS INTEGER) AS quarter,
                CONCAT(year(date), '-Q', quarter(date)) AS quarter_label,
                CAST(year(date) * 10 + quarter(date) AS INTEGER)
                    AS year_quarter_key,

                CAST(month(date) AS INTEGER) AS month,
                strftime(date, '%B') AS month_name,

                CAST(
                    year(date) * 100 + month(date)
                    AS INTEGER
                ) AS year_month_key,

                strftime(date, '%Y-%m') AS year_month,

                CAST(isoyear(date) AS INTEGER) AS iso_year,
                CAST(week(date) AS INTEGER) AS iso_week,

                printf(
                    '%04d-W%02d',
                    isoyear(date),
                    week(date)
                ) AS year_week,

                CAST(
                    date_trunc('week', date)
                    AS DATE
                ) AS week_start_date,

                CAST(
                    date_trunc('week', date)
                    + INTERVAL '6 days'
                    AS DATE
                ) AS week_end_date,

                CAST(day(date) AS INTEGER) AS day_of_month,
                CAST(isodow(date) AS INTEGER) AS day_of_week,
                strftime(date, '%A') AS day_name,

                isodow(date) IN (6, 7) AS is_weekend

            FROM calendar

            ORDER BY date
            """
        ).pl()


def build_dim_shop(
    weekly_prices_path: str | Path,
    nutritionals_path: str | Path,
) -> pl.DataFrame:
    """Build the conformed retailer dimension."""

    weekly_prices = _sql_path(weekly_prices_path)
    nutritionals = _sql_path(nutritionals_path)

    with duckdb.connect() as con:
        shops = con.sql(
            f"""
            SELECT DISTINCT shop, shop_name
            FROM (
                SELECT shop, shop_name
                FROM read_parquet('{weekly_prices}')

                UNION

                SELECT shop, shop_name
                FROM read_parquet('{nutritionals}')
                WHERE country = '{MART_COUNTRY}'
            )

            WHERE shop IS NOT NULL

            ORDER BY shop
            """
        ).pl()

    require_shop_names(shops)
    if shops["shop"].n_unique() != shops.height:
        raise ValueError("Conflicting Silver names for one shop code.")

    return _add_surrogate_key(shops, "shop_key").select(
        ["shop_key", "shop", "shop_name"]
    )


def _product_identities(
    con: duckdb.DuckDBPyConnection,
    weekly_products: str,
    nutritionals: str,
    weekly_prices: str,
) -> pl.DataFrame:
    """Step 1: identities of the mart, from weekly products and Belgian nutrition."""
    return con.sql(
        f"""
        WITH weekly_identities AS (
            SELECT DISTINCT
                daltix_id,
                shop,
                country,
                language

            FROM read_parquet('{weekly_products}')
        ),

        nutrition_identities AS (
            SELECT DISTINCT
                daltix_id,
                shop,
                country,
                language

            FROM read_parquet('{nutritionals}')

            WHERE country = '{MART_COUNTRY}'
        ),

        priced_identities AS (
            SELECT DISTINCT
                daltix_id,
                shop

            FROM read_parquet('{weekly_prices}')
        ),

        identity_base AS (
            SELECT
                COALESCE(w.daltix_id, n.daltix_id) AS daltix_id,
                COALESCE(w.shop, n.shop) AS shop,
                COALESCE(w.country, n.country) AS country,
                COALESCE(w.language, n.language) AS language,

                w.daltix_id IS NOT NULL AS is_in_weekly_products,
                n.daltix_id IS NOT NULL AS is_in_nutrition

            FROM weekly_identities w

            FULL OUTER JOIN nutrition_identities n
                USING (daltix_id, shop, country)
        )

        SELECT b.*

        FROM identity_base b

        -- A product belongs to the mart only if a fact table refers to it.
        WHERE b.is_in_nutrition
           OR EXISTS (
                SELECT 1
                FROM priced_identities p
                WHERE p.daltix_id = b.daltix_id
                  AND p.shop = b.shop
           )

        ORDER BY
            b.shop,
            b.country,
            b.daltix_id
        """
    ).pl()


def _describe_products(
    con: duckdb.DuckDBPyConnection, weekly_products: str, products: str
) -> pl.DataFrame:
    """Step 2: name, brand and description; weekly products win over the reference."""
    return con.sql(
        f"""
        SELECT
            b.daltix_id,
            b.shop,
            b.country,
            b.language,

            COALESCE(w.name, p.name) AS name,
            COALESCE(w.brand, p.brand) AS brand,
            COALESCE(w.description, p.description) AS description,

            CASE
                WHEN w.daltix_id IS NOT NULL
                    THEN 'weekly_products'
                WHEN p.daltix_id IS NOT NULL
                    THEN 'products'
                ELSE 'identity_only'
            END AS descriptive_source

        FROM product_identity_base b

        LEFT JOIN read_parquet('{weekly_products}') w
            ON b.daltix_id = w.daltix_id
           AND b.shop = w.shop
           AND b.country = w.country

        LEFT JOIN read_parquet('{products}') p
            ON b.daltix_id = p.daltix_id
           AND b.shop = p.shop
           AND b.country = p.country

        ORDER BY
            b.shop,
            b.country,
            b.daltix_id
        """
    ).pl()


def _recover_priced_products(
    con: duckdb.DuckDBPyConnection, weekly_prices: str, products: str
) -> pl.DataFrame:
    """Step 3: priced identities that only the products reference can describe.

    A price identity carries no country. It is recovered only when the products
    reference lists it under exactly one country.
    """
    return con.sql(
        f"""
        WITH pricing_identities AS (
            SELECT DISTINCT
                daltix_id,
                shop

            FROM read_parquet('{weekly_prices}')
        ),

        known_product_identities AS (
            SELECT DISTINCT
                daltix_id,
                shop

            FROM product_descriptive_base
        ),

        unresolved_pricing AS (
            SELECT
                p.daltix_id,
                p.shop

            FROM pricing_identities p

            ANTI JOIN known_product_identities k
                USING (daltix_id, shop)
        ),

        product_reference AS (
            SELECT
                daltix_id,
                shop,

                CASE
                    WHEN COUNT(DISTINCT country) = 1
                        THEN MIN(country)
                END AS country,

                CASE
                    WHEN COUNT(DISTINCT language)
                         FILTER (WHERE language IS NOT NULL) <= 1
                        THEN MIN(language)
                END AS language,

                CASE
                    WHEN COUNT(DISTINCT name)
                         FILTER (WHERE name IS NOT NULL) <= 1
                        THEN MIN(name)
                END AS name,

                CASE
                    WHEN COUNT(DISTINCT brand)
                         FILTER (WHERE brand IS NOT NULL) <= 1
                        THEN MIN(brand)
                END AS brand,

                CASE
                    WHEN COUNT(DISTINCT description)
                         FILTER (WHERE description IS NOT NULL) <= 1
                        THEN MIN(description)
                END AS description,

                COUNT(DISTINCT country) AS country_versions

            FROM read_parquet('{products}')

            GROUP BY
                daltix_id,
                shop
        )

        SELECT
            u.daltix_id,
            u.shop,
            r.country,
            r.language,
            r.name,
            r.brand,
            r.description,
            'products' AS descriptive_source

        FROM unresolved_pricing u

        INNER JOIN product_reference r
            ON u.daltix_id = r.daltix_id
           AND u.shop = r.shop

        WHERE r.country_versions = 1

        ORDER BY
            u.shop,
            r.country,
            u.daltix_id
        """
    ).pl()


def _infer_priced_products(
    con: duckdb.DuckDBPyConnection, weekly_prices: str
) -> pl.DataFrame:
    """Step 4: a member of its own for every price identity still without a product.

    Prices therefore never share the Unknown member. Country comes from the Belgian
    weekly-pricing process; nothing else is known about these products.
    """
    return con.sql(
        f"""
        WITH pricing_identities AS (
            SELECT DISTINCT
                daltix_id,
                shop,
                shop_name

            FROM read_parquet('{weekly_prices}')

            WHERE daltix_id IS NOT NULL
        ),

        known_product_identities AS (
            SELECT daltix_id, shop
            FROM product_descriptive_base

            UNION

            SELECT daltix_id, shop
            FROM recovered_pricing_products
        )

        SELECT
            p.daltix_id,
            p.shop,
            '{MART_COUNTRY}' AS country,
            CAST(NULL AS VARCHAR) AS language,
            'Unmatched product (' || p.shop_name || ' ' || p.daltix_id || ')'
                AS name,
            CAST(NULL AS VARCHAR) AS brand,
            CAST(NULL AS VARCHAR) AS description,
            '{INFERRED_SOURCE}' AS descriptive_source

        FROM pricing_identities p

        ANTI JOIN known_product_identities k
            USING (daltix_id, shop)

        ORDER BY
            p.shop,
            p.daltix_id
        """
    ).pl()


def _unknown_product() -> pl.DataFrame:
    """The reserved member ``product_key = -1`` for a price without any id."""
    return pl.DataFrame(
        {
            "product_key": [-1],
            "daltix_id": [None],
            "shop": [None],
            "country": [None],
            "language": [None],
            "name": ["Unknown / Unmatched Product"],
            "brand": [None],
            "description": [None],
            "descriptive_source": ["unknown"],
            "is_inferred_product": [False],
        },
        schema_overrides={
            "product_key": pl.Int64,
            "daltix_id": pl.String,
            "shop": pl.String,
            "country": pl.String,
            "language": pl.String,
            "name": pl.String,
            "brand": pl.String,
            "description": pl.String,
            "descriptive_source": pl.String,
            "is_inferred_product": pl.Boolean,
        },
    )


def build_dim_product(
    weekly_products_path: str | Path,
    nutritionals_path: str | Path,
    products_path: str | Path,
    weekly_prices_path: str | Path,
) -> pl.DataFrame:
    """Build the conformed product dimension of the Belgian mart.

    Members, in order of evidence: products described by the weekly-product
    reference (or the products reference), priced products recovered from the
    products reference, and one inferred member per price identity nobody
    describes. The reserved Unknown member comes first.
    """
    weekly_products = _sql_path(weekly_products_path)
    nutritionals = _sql_path(nutritionals_path)
    products = _sql_path(products_path)
    weekly_prices = _sql_path(weekly_prices_path)

    with duckdb.connect() as con:
        # Each step reads the frames of the previous ones by name.
        con.register(
            "product_identity_base",
            _product_identities(con, weekly_products, nutritionals, weekly_prices),
        )
        described = _describe_products(con, weekly_products, products)
        con.register("product_descriptive_base", described)
        recovered = _recover_priced_products(con, weekly_prices, products)
        con.register("recovered_pricing_products", recovered)
        inferred = _infer_priced_products(con, weekly_prices)

    identified_products = (
        _add_surrogate_key(
            pl.concat([described, recovered, inferred], how="vertical").sort(
                ["shop", "country", "daltix_id"]
            ),
            "product_key",
        )
        .with_columns(
            (pl.col("descriptive_source") == INFERRED_SOURCE).alias(
                "is_inferred_product"
            )
        )
        .select(
            [
                "product_key",
                "daltix_id",
                "shop",
                "country",
                "language",
                "name",
                "brand",
                "description",
                "descriptive_source",
                "is_inferred_product",
            ]
        )
    )
    return pl.concat([_unknown_product(), identified_products], how="vertical")


def build_dim_location(
    weekly_locations_path: str | Path,
    weekly_prices_path: str | Path,
) -> pl.DataFrame:
    """Build the retailer-location dimension of the price contexts actually observed."""

    weekly_locations = _sql_path(weekly_locations_path)
    weekly_prices = _sql_path(weekly_prices_path)

    with duckdb.connect() as con:
        location_base = con.sql(
            f"""
            SELECT
                l.shop,
                l.location,
                l.location_name,
                l.shop_type,

                l.geolocation_latitude,
                l.geolocation_longitude,
                l.locality,
                l.postcode,
                l.state,

                l.location_type,
                l.region,

                l.dq_postcode_conflict,
                l.dq_latitude_conflict,
                l.dq_longitude_conflict,
                l.dq_enrichment_context_mismatch,

                l.is_postcode_enriched,
                l.is_coordinates_enriched,

                l.dq_invalid_latitude,
                l.dq_invalid_longitude,
                l.dq_incomplete_coordinates,
                l.dq_outside_benelux

            FROM read_parquet('{weekly_locations}') l

            -- Only locations that have weekly prices belong to the mart.
            WHERE EXISTS (
                SELECT 1
                FROM read_parquet('{weekly_prices}') w
                WHERE w.shop = l.shop
                  AND w.location = l.location
            )

            ORDER BY
                l.shop,
                l.location
            """
        ).pl()

    return _add_surrogate_key(location_base, "location_key").select(
        ["location_key", *location_base.columns]
    )


def build_dim_nutrient(
    nutritionals_path: str | Path,
) -> pl.DataFrame:
    """Build the canonical nutrient dimension."""

    nutritionals = _sql_path(nutritionals_path)

    with duckdb.connect() as con:
        nutrient_base = con.sql(
            f"""
            SELECT DISTINCT
                nutrient_name,

                CASE
                    WHEN nutrient_name = 'energy'
                        THEN 'kJ'
                    WHEN nutrient_name = 'kilocalories'
                        THEN 'kcal'
                    ELSE 'g'
                END AS standard_unit

            FROM read_parquet('{nutritionals}')

            WHERE nutrient_name IS NOT NULL
              AND country = '{MART_COUNTRY}'

            ORDER BY nutrient_name
            """
        ).pl()

    return _add_surrogate_key(nutrient_base, "nutrient_key").select(
        ["nutrient_key", "nutrient_name", "standard_unit"]
    )


# --- Product-category model ----------------------------------------------------


def _category_memberships(
    category_paths: str, dim_product: pl.DataFrame
) -> pl.DataFrame:
    """One row per mart product and eligible retailer path.

    A path is eligible unless it is a duplicate, the ancestor of another path, site
    navigation or a repeated label.
    """
    with duckdb.connect() as con:
        con.register("dim_product", dim_product)

        eligible_category_paths = con.sql(
            f"""
            SELECT
                source_dataset,
                daltix_id,
                shop,
                country,
                language,
                category_source_dataset,
                category_path,
                category_path_normalized,
                category_path_depth,
                product_maximal_path_count

            FROM read_parquet('{category_paths}')

            WHERE dq_duplicate_category_path = FALSE
              AND is_prefix_of_another_path = FALSE
              AND dq_navigation_contamination = FALSE
              AND dq_repeated_category_label = FALSE
            """
        ).pl()
        con.register("eligible_category_paths", eligible_category_paths)

        return con.sql(
            """
            SELECT DISTINCT
                p.product_key,
                e.shop,
                e.country,
                e.language,
                e.category_path,
                e.category_path_normalized,
                e.category_path_depth

            FROM eligible_category_paths e

            INNER JOIN dim_product p
                ON e.daltix_id = p.daltix_id
               AND e.shop = p.shop
               AND e.country = p.country
               AND p.product_key <> -1

            ORDER BY
                p.product_key,
                e.shop,
                e.country,
                e.category_path_normalized
            """
        ).pl()


def _category_dimension(memberships: pl.DataFrame) -> pl.DataFrame:
    """One category per retailer, country and normalised path, keyed in sorted order."""
    identity = ["shop", "country", "category_path_normalized"]
    return (
        memberships.select(
            [
                "shop",
                "country",
                "language",
                "category_path",
                "category_path_normalized",
                "category_path_depth",
            ]
        )
        .unique(subset=identity)
        .sort(identity)
        .with_row_index("category_key", offset=1)
        .with_columns(
            pl.col("category_key").cast(pl.Int64),
            pl.col("category_path_normalized").list.last().alias("category_leaf"),
            pl.col("category_path_normalized")
            .list.join(" > ")
            .alias("category_path_text"),
        )
        .select(
            [
                "category_key",
                "shop",
                "country",
                "language",
                "category_path",
                "category_path_normalized",
                "category_path_text",
                "category_leaf",
                "category_path_depth",
            ]
        )
    )


def _category_bridge(
    memberships: pl.DataFrame, dim_category: pl.DataFrame
) -> pl.DataFrame:
    """Product-to-category pairs, so a product can sit in several categories."""
    with duckdb.connect() as con:
        con.register("gold_category_memberships_base", memberships)
        con.register("dim_category", dim_category)

        return con.sql(
            """
            SELECT
                m.product_key,
                c.category_key

            FROM gold_category_memberships_base m

            INNER JOIN dim_category c
                ON m.shop = c.shop
               AND m.country = c.country
               AND m.category_path_normalized
                   = c.category_path_normalized

            ORDER BY
                m.product_key,
                c.category_key
            """
        ).pl()


def build_category_model(
    category_paths_path: str | Path,
    dim_product: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build dim_category and bridge_product_category."""
    memberships = _category_memberships(_sql_path(category_paths_path), dim_product)
    dim_category = _category_dimension(memberships)
    return dim_category, _category_bridge(memberships, dim_category)


# --- Primary FMCG category on Product ------------------------------------------


def attach_primary_categories(
    dim_product: pl.DataFrame, classification_path: Path
) -> pl.DataFrame:
    """Strict many-to-one contextual join; preserve keys, row order and old values."""
    lookup = pl.read_parquet(classification_path)
    validate_primary_categories(lookup)
    result = dim_product.join(
        lookup.select(IDENTITY + REPORT_FIELDS),
        on=IDENTITY,
        how="left",
        validate="m:1",
        maintain_order="left",
    )
    # Unknown and inferred members are not in the Silver lookup: their category
    # evidence is missing by construction, and they say so explicitly.
    has_no_decision = (pl.col("product_key") == -1) | pl.col("is_inferred_product")
    if result.filter(~has_no_decision & pl.col(REPORT_FIELDS[0]).is_null()).height:
        raise ValueError(
            "Identified Gold product is missing its Silver classification decision."
        )
    result = result.with_columns(
        pl.when(pl.col("product_key") == -1)
        .then(pl.lit(unknown))
        .when(pl.col("is_inferred_product"))
        .then(pl.lit(inferred))
        .otherwise(pl.col(field))
        .alias(field)
        for field, unknown, inferred in zip(
            REPORT_FIELDS,
            (
                "unclassified",
                CATEGORY_LABELS["unclassified"],
                "unknown_product",
                MAPPING_VERSION,
            ),
            (
                "unclassified",
                CATEGORY_LABELS["unclassified"],
                "missing_evidence",
                MAPPING_VERSION,
            ),
            strict=True,
        )
    )
    if not result.select(dim_product.columns).equals(dim_product):
        raise ValueError("Category enrichment changed existing Gold Product data.")
    return result


# --- Facts ---------------------------------------------------------------------


def create_fact_weekly_prices(
    con: duckdb.DuckDBPyConnection,
    weekly_prices_path: str | Path,
    dim_product: pl.DataFrame,
    dim_shop: pl.DataFrame,
    dim_location: pl.DataFrame,
    dim_date: pl.DataFrame,
) -> None:
    """Create fact_weekly_prices inside the supplied DuckDB connection.

    Grain: product x retailer x location x week. Silver keeps every distinct price
    seen in that week; here they are grouped into one row without choosing a price.
    A regular, promotional or effective price is filled only when every observation
    of the week agrees; the min/max range, the version counts and the variation
    flags always stay. The effective price is the promotional price during a
    promotion, otherwise the regular price. Price identities without a product
    fall back to the Unknown member (none occur in this snapshot).
    """

    weekly_prices = _sql_path(weekly_prices_path)

    con.register("dim_product", dim_product)
    con.register("dim_shop", dim_shop)
    con.register("dim_location", dim_location)
    con.register("dim_date", dim_date)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE fact_weekly_prices AS

        WITH product_map AS (
            SELECT
                daltix_id,
                shop,
                MIN(product_key) AS product_key

            FROM dim_product

            WHERE product_key <> -1

            GROUP BY
                daltix_id,
                shop

            HAVING COUNT(*) = 1
        ),

        weekly_price_aggregation AS (
            SELECT
                daltix_id,
                shop,
                location,
                week,

                COUNT(*) AS price_observation_versions,

                COUNT(DISTINCT price)
                    AS regular_price_versions,

                MIN(price)
                    AS regular_price_min,

                MAX(price)
                    AS regular_price_max,

                COUNT(DISTINCT price_promo)
                    FILTER (WHERE is_promotion = TRUE)
                    AS promo_price_versions,

                MIN(price_promo)
                    FILTER (WHERE is_promotion = TRUE)
                    AS promo_price_min,

                MAX(price_promo)
                    FILTER (WHERE is_promotion = TRUE)
                    AS promo_price_max,

                COUNT(DISTINCT is_promotion)
                    AS promotion_state_versions,

                BOOL_OR(is_promotion)
                    AS any_promotion,

                COUNT(
                    DISTINCT CASE
                        WHEN is_promotion = TRUE
                            THEN price_promo
                        ELSE price
                    END
                ) AS effective_price_versions,

                MIN(
                    CASE
                        WHEN is_promotion = TRUE
                            THEN price_promo
                        ELSE price
                    END
                ) AS effective_price_min,

                MAX(
                    CASE
                        WHEN is_promotion = TRUE
                            THEN price_promo
                        ELSE price
                    END
                ) AS effective_price_max,

                BOOL_OR(has_multiple_price_versions)
                    AS has_intraweek_price_variation

            FROM read_parquet('{weekly_prices}')

            GROUP BY
                daltix_id,
                shop,
                location,
                week
        ),

        price_measures AS (
            SELECT
                *,

                CASE
                    WHEN regular_price_versions = 1
                        THEN regular_price_min
                END AS regular_price,

                CASE
                    WHEN promotion_state_versions = 1
                     AND any_promotion = TRUE
                     AND promo_price_versions = 1
                        THEN promo_price_min
                END AS promo_price,

                CASE
                    WHEN effective_price_versions = 1
                        THEN effective_price_min
                END AS effective_price,

                CASE
                    WHEN promotion_state_versions = 1
                        THEN any_promotion
                END AS is_promotion,

                effective_price_versions > 1
                    AS has_effective_price_variation,

                promotion_state_versions > 1
                    AS promotion_state_conflict

            FROM weekly_price_aggregation
        )

        SELECT
            CAST(
                COALESCE(pm.product_key, -1)
                AS BIGINT
            ) AS product_key,

            CAST(s.shop_key AS BIGINT)
                AS shop_key,

            CAST(l.location_key AS BIGINT)
                AS location_key,

            CAST(d.date_key AS INTEGER)
                AS date_key,

            m.daltix_id
                AS source_daltix_id,

            m.regular_price,
            m.promo_price,
            m.effective_price,

            CASE
                WHEN m.promotion_state_versions = 1
                 AND m.any_promotion = TRUE
                 AND m.regular_price_versions = 1
                 AND m.promo_price_versions = 1
                    THEN m.regular_price_min - m.promo_price_min
            END AS discount_amount,

            CASE
                WHEN m.promotion_state_versions = 1
                 AND m.any_promotion = TRUE
                 AND m.regular_price_versions = 1
                 AND m.promo_price_versions = 1
                 AND m.regular_price_min > 0
                    THEN (
                        m.regular_price_min
                        - m.promo_price_min
                    ) / m.regular_price_min
            END AS discount_pct,

            m.regular_price_min,
            m.regular_price_max,

            m.promo_price_min,
            m.promo_price_max,

            m.effective_price_min,
            m.effective_price_max,

            m.price_observation_versions,
            m.regular_price_versions,
            m.promo_price_versions,
            m.effective_price_versions,

            m.is_promotion,

            m.has_intraweek_price_variation,
            m.has_effective_price_variation,
            m.promotion_state_conflict

        FROM price_measures m

        LEFT JOIN product_map pm
            ON m.daltix_id = pm.daltix_id
           AND m.shop = pm.shop

        INNER JOIN dim_shop s
            ON m.shop = s.shop

        INNER JOIN dim_location l
            ON m.shop = l.shop
           AND m.location = l.location

        INNER JOIN dim_date d
            ON m.week = d.date

        ORDER BY
            d.date_key,
            s.shop_key,
            l.location_key,
            source_daltix_id
        """
    )


def create_fact_product_nutrition(
    con: duckdb.DuckDBPyConnection,
    nutritionals_path: str | Path,
    dim_product: pl.DataFrame,
    dim_shop: pl.DataFrame,
    dim_date: pl.DataFrame,
    dim_nutrient: pl.DataFrame,
) -> None:
    """Create fact_product_nutrition inside the supplied DuckDB connection.

    Grain: product x observation date x nutrient x basis, limited to the products
    of the mart. The basis is per 100 g or per 100 ml (anything else is ``other``).
    Rows with an incompatible unit keep their quality flags and a NULL value.
    ``is_latest_observation`` marks the newest date of each product, nutrient and
    basis, so benchmarks can weigh every product once.
    """

    nutritionals = _sql_path(nutritionals_path)

    con.register("dim_product", dim_product)
    con.register("dim_shop", dim_shop)
    con.register("dim_date", dim_date)
    con.register("dim_nutrient", dim_nutrient)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE fact_product_nutrition AS

        WITH observations AS (
        SELECT
            CAST(p.product_key AS BIGINT)
                AS product_key,

            CAST(s.shop_key AS BIGINT)
                AS shop_key,

            CAST(d.date_key AS INTEGER)
                AS date_key,

            CAST(n.nutrient_key AS BIGINT)
                AS nutrient_key,

            x.daltix_id
                AS source_daltix_id,

            CASE
                WHEN x.portion_value = 100
                 AND lower(x.portion_unit) = 'g'
                    THEN 'per_100g'

                WHEN x.portion_value = 100
                 AND lower(x.portion_unit) = 'ml'
                    THEN 'per_100ml'

                ELSE 'other'
            END AS nutrition_basis,

            x.nutrient_value,

            x.nutrient_value IS NOT NULL
                AS is_value_usable,

            x.dq_conflicting_nutrition,
            x.dq_missing_nutrient_value,
            x.dq_non_numeric_value,
            x.dq_incompatible_nutrient_unit,
            x.dq_mass_exceeds_portion,

            x.dq_negative_nutrient_value,
            x.energy_pair_difference_kj,
            x.dq_energy_kcal_mismatch,

            x.nutrition_selection_reason

        FROM read_parquet('{nutritionals}') x

        INNER JOIN dim_product p
            ON x.daltix_id = p.daltix_id
           AND x.shop = p.shop
           AND x.country = p.country
           AND p.product_key <> -1

        INNER JOIN dim_shop s
            ON x.shop = s.shop

        INNER JOIN dim_date d
            ON x.download_date = d.date

        INNER JOIN dim_nutrient n
            ON x.nutrient_name = n.nutrient_name
        )

        SELECT
            *,

            -- The most recent observation date of each product, nutrient and
            -- basis, so that benchmarks can weigh every product equally instead
            -- of counting a product once per label that was downloaded.
            date_key = MAX(date_key) OVER (
                PARTITION BY
                    product_key,
                    nutrient_key,
                    nutrition_basis
            ) AS is_latest_observation

        FROM observations

        ORDER BY
            product_key,
            date_key,
            nutrient_key,
            nutrition_basis
        """
    )


def add_price_comparison_columns(con: duckdb.DuckDBPyConnection) -> None:
    """Add like-for-like price ratios to ``fact_weekly_prices`` (same grain).

    Three nullable DOUBLE columns are appended to the existing rows; no row is added,
    dropped or reordered, so the semantic model only has to average them instead of
    joining 18.7M weekly prices at query time:

    - ``log_paid_yoy_ratio``: ln(effective price / effective price of the same
      product and location exactly 364 days (52 weeks) earlier).
    - ``log_shelf_yoy_ratio``: the same with the regular (shelf) price, so the gap
      between the two is the effect of promotions.
    - ``log_store_vs_national_ratio``: only on rows of a ``Store`` location,
      ln(effective price / effective price of the same product, retailer and week at
      the retailer's ``National price (online)`` location).

    A ratio is NULL when its other week or national price is missing, or when either
    price is NULL or not above zero. The Unknown product (``product_key = -1``) is
    never compared, and a comparison price that is not unique is ignored, so a join
    can never multiply rows.

    Ratios are stored as natural logs because the geometric mean of price relatives,
    exp(mean(log ratio)), then adds up over any slice of weeks, retailers or
    categories. Every weekly source price carries a random +-10% perturbation
    (factor 0.9, 1.0 or 1.1; 94% of the week-to-week ratios of one product in one
    store are one of seven values), which only averages out over many pairs. That is
    why the semantic model reports a slice only from 500 pairs.

    Requires ``fact_weekly_prices``, ``dim_date`` and ``dim_location`` in ``con``.
    """

    rows_before = con.sql("SELECT COUNT(*) FROM fact_weekly_prices").fetchone()[0]

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE fact_weekly_prices_compared AS

        WITH priced AS (
            SELECT
                f.product_key,
                f.shop_key,
                f.location_key,
                f.date_key,
                f.source_daltix_id,
                CAST(d.date AS DATE) AS week_date,
                f.effective_price,
                f.regular_price,
                l.location_type

            FROM fact_weekly_prices f

            INNER JOIN dim_date d
                ON f.date_key = d.date_key

            INNER JOIN dim_location l
                ON f.location_key = l.location_key

            WHERE f.product_key <> -1
        ),

        one_year_earlier AS (
            SELECT
                product_key,
                location_key,
                week_date,
                MIN(effective_price) AS effective_price,
                MIN(regular_price) AS regular_price

            FROM priced

            GROUP BY
                product_key,
                location_key,
                week_date

            HAVING COUNT(*) = 1
        ),

        yoy AS (
            SELECT
                c.source_daltix_id,
                c.shop_key,
                c.location_key,
                c.date_key,

                CASE
                    WHEN c.effective_price > 0 AND p.effective_price > 0
                        THEN ln(c.effective_price / p.effective_price)
                END AS log_paid_yoy_ratio,

                CASE
                    WHEN c.regular_price > 0 AND p.regular_price > 0
                        THEN ln(c.regular_price / p.regular_price)
                END AS log_shelf_yoy_ratio

            FROM priced c

            INNER JOIN one_year_earlier p
                ON c.product_key = p.product_key
               AND c.location_key = p.location_key
               AND p.week_date = c.week_date - 364
        ),

        national AS (
            SELECT
                product_key,
                shop_key,
                date_key,
                MIN(effective_price) AS effective_price

            FROM priced

            WHERE location_type = 'National price (online)'

            GROUP BY
                product_key,
                shop_key,
                date_key

            HAVING COUNT(*) = 1
        ),

        store_vs_national AS (
            SELECT
                s.source_daltix_id,
                s.shop_key,
                s.location_key,
                s.date_key,

                CASE
                    WHEN s.effective_price > 0 AND n.effective_price > 0
                        THEN ln(s.effective_price / n.effective_price)
                END AS log_store_vs_national_ratio

            FROM priced s

            INNER JOIN national n
                ON s.product_key = n.product_key
               AND s.shop_key = n.shop_key
               AND s.date_key = n.date_key

            WHERE s.location_type = 'Store'
        )

        SELECT
            f.*,
            CAST(y.log_paid_yoy_ratio AS DOUBLE) AS log_paid_yoy_ratio,
            CAST(y.log_shelf_yoy_ratio AS DOUBLE) AS log_shelf_yoy_ratio,
            CAST(s.log_store_vs_national_ratio AS DOUBLE)
                AS log_store_vs_national_ratio

        FROM fact_weekly_prices f

        LEFT JOIN yoy y
            ON f.source_daltix_id = y.source_daltix_id
           AND f.shop_key = y.shop_key
           AND f.location_key = y.location_key
           AND f.date_key = y.date_key

        LEFT JOIN store_vs_national s
            ON f.source_daltix_id = s.source_daltix_id
           AND f.shop_key = s.shop_key
           AND f.location_key = s.location_key
           AND f.date_key = s.date_key

        ORDER BY
            f.date_key,
            f.shop_key,
            f.location_key,
            f.source_daltix_id
        """
    )
    rows_after = con.sql("SELECT COUNT(*) FROM fact_weekly_prices_compared").fetchone()[
        0
    ]
    if rows_after != rows_before:
        raise ValueError("Price comparison columns changed the number of weekly rows.")

    con.execute("DROP TABLE fact_weekly_prices")
    con.execute("ALTER TABLE fact_weekly_prices_compared RENAME TO fact_weekly_prices")
