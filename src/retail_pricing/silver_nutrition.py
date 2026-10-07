"""Silver nutritional observations: one row per product, date and nutrient."""

from pathlib import Path

import duckdb
import polars as pl

from retail_pricing.quality import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_date,
)
from retail_pricing.raw import quote_literal
from retail_pricing.reference import (
    canonicalize_shop,
    get_nutrient_mapping,
    shop_name_sql,
)

# Monitoring tolerance, not a regulatory or nutritional validity threshold.
ENERGY_KJ_PER_KCAL = 4.184
ENERGY_RELATIVE_TOLERANCE = 0.02
ENERGY_ABSOLUTE_TOLERANCE_KJ = 1.0

NUTRITIONALS_REQUIRED_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "download_date",
    "nutritional_values_std",
]

NUTRITIONALS_TEXT_COLUMNS = ["daltix_id", "shop", "country", "language"]
NUTRITIONALS_EXACT_KEY = NUTRITIONALS_REQUIRED_COLUMNS

# One label per product, shop, country and date. When the source holds several, the
# most complete one wins (numeric nutrients, then nutrients, then units); a hash
# breaks the remaining ties, so the choice is reproducible, not proven true.
CANONICAL_QUERY = """
        WITH nutrient_stats AS (
            SELECT
                n._source_row_id,

                COUNT(nutrient.key)
                    AS nutrient_count,

                COUNT(*) FILTER (
                    WHERE TRY_CAST(
                        json_extract_string(
                            nutrient.value,
                            '$.value'
                        )
                        AS DOUBLE
                    ) IS NOT NULL
                ) AS numeric_nutrient_count,

                COUNT(*) FILTER (
                    WHERE NULLIF(
                        TRIM(
                            json_extract_string(
                                nutrient.value,
                                '$.unit'
                            )
                        ),
                        ''
                    ) IS NOT NULL
                ) AS populated_unit_count

            FROM nutritionals_deduped_source n

            LEFT JOIN LATERAL json_each(
                n.nutritional_values_std,
                '$.nutrients'
            ) nutrient
                ON TRUE

            GROUP BY n._source_row_id
        ),

        grain_stats AS (
            SELECT
                daltix_id,
                shop,
                country,
                download_date,

                COUNT(*) AS grain_row_count,

                COUNT(DISTINCT nutritional_values_std)
                    AS nutritional_versions,

                COUNT(DISTINCT language)
                    AS language_versions

            FROM nutritionals_deduped_source

            GROUP BY
                daltix_id,
                shop,
                country,
                download_date
        ),

        ranked AS (
            SELECT
                n.*,

                s.nutrient_count,
                s.numeric_nutrient_count,
                s.populated_unit_count,

                g.grain_row_count,

                g.grain_row_count > 1
                    AS dq_repeated_grain,

                g.nutritional_versions > 1
                    AS dq_conflicting_nutrition,

                g.language_versions > 1
                    AS dq_language_conflict,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        n.daltix_id,
                        n.shop,
                        n.country,
                        n.download_date

                    ORDER BY
                        s.numeric_nutrient_count DESC,
                        s.nutrient_count DESC,
                        s.populated_unit_count DESC,
                        md5(n.nutritional_values_std) ASC,
                        COALESCE(n.language, '') ASC,
                        n._source_row_id ASC
                ) AS canonical_rank,
                COUNT(*) OVER (
                    PARTITION BY n.daltix_id, n.shop, n.country, n.download_date,
                        s.numeric_nutrient_count, s.nutrient_count, s.populated_unit_count
                ) AS completeness_tie_count

            FROM nutritionals_deduped_source n

            INNER JOIN nutrient_stats s
                USING (_source_row_id)

            INNER JOIN grain_stats g
                USING (
                    daltix_id,
                    shop,
                    country,
                    download_date
                )
        )

        SELECT
            *,

            CASE
                WHEN grain_row_count > 1
                    THEN 'completeness_first'

                WHEN dq_source_exact_duplicate
                    THEN 'exact_duplicate_collapsed'

                ELSE 'single_observation'
            END AS nutrition_resolution_rule

        FROM ranked

        WHERE canonical_rank = 1
        """


METRIC_KEYS = [
    "rows",
    "distinct_products",
    "distinct_nutrients",
    "missing_numeric_values",
    "non_numeric_values",
    "missing_units",
    "rows_from_resolved_conflicts",
    "unconvertible_unit_rows",
    "identity_unit_rows",
    "converted_unit_rows",
    "canonical_numeric_rows",
    "canonical_unit_mismatch_rows",
    "mass_exceeds_portion_rows",
    "mass_exceeds_portion_products",
    "negative_nutrient_rows",
    "negative_source_nutrient_rows",
    "comparable_energy_observations",
    "energy_mismatch_observations",
    "energy_mismatch_nutrient_rows",
]


def _read_clean_nutritionals(raw_path: Path) -> pl.DataFrame:
    """Read Raw, reject schema drift and standardise the identifying text fields."""
    raw = pl.read_parquet(raw_path)
    require_columns(raw, NUTRITIONALS_REQUIRED_COLUMNS)
    require_date(raw, "download_date")
    clean = canonicalize_shop(raw).with_columns(
        [normalize_text(c) for c in NUTRITIONALS_TEXT_COLUMNS]
    )
    fail_if_false(
        {
            f"{c}_complete": clean[c].null_count() == 0
            for c in NUTRITIONALS_TEXT_COLUMNS
            + ["download_date", "nutritional_values_std"]
        },
        "Nutritionals source",
    )
    return clean


def _check_nutrition_source(con: duckdb.DuckDBPyConnection) -> None:
    """Every Raw label must be valid JSON with mappable, numeric nutrients.

    All Raw versions are checked, including those not selected later, so unknown
    pairs cannot disappear through ranking or an inner join.
    """
    valid = con.sql(
        "SELECT count(*) FILTER (WHERE NOT json_valid(nutritional_values_std)) "
        "FROM source"
    ).fetchone()[0]
    fail_if_false({"json_valid": valid == 0}, "Nutritionals JSON")

    # A label needs at least one nutrient and a positive, typed portion.
    portion = (
        "try_cast(json_extract_string(nutritional_values_std, '$.portion.value')"
        " AS DOUBLE)"
    )
    structure = con.sql(f"""
        SELECT count(*) FILTER (WHERE
            json_type(nutritional_values_std, '$.nutrients') IS DISTINCT FROM 'OBJECT'
            OR coalesce(len(json_keys(nutritional_values_std, '$.nutrients')), 0) = 0
            OR {portion} IS NULL
            OR NOT isfinite({portion})
            OR {portion} <= 0
            OR nullif(
                trim(json_extract_string(nutritional_values_std, '$.portion.unit')), ''
            ) IS NULL
        ) FROM source
    """).fetchone()[0]
    fail_if_false(
        {"nonempty_nutrients_and_portion": structure == 0}, "Nutritionals structure"
    )

    con.register("nutrient_mapping", get_nutrient_mapping())
    unmapped = con.sql("""
        SELECT DISTINCT nutrient.key AS raw_nutrient_name,
            json_extract_string(nutrient.value, '$.unit') AS raw_unit
        FROM source n
        CROSS JOIN LATERAL json_each(n.nutritional_values_std, '$.nutrients') nutrient
        ANTI JOIN nutrient_mapping m ON nutrient.key = m.raw_nutrient_name
            AND json_extract_string(nutrient.value, '$.unit') = m.raw_unit
        LIMIT 20
    """).fetchall()
    if unmapped:
        raise RuntimeError(
            f"Unmapped nutrient/unit combinations (up to 20): {unmapped}"
        )

    invalid_values = con.sql("""
        SELECT count(*) FROM source n
        CROSS JOIN LATERAL json_each(n.nutritional_values_std, '$.nutrients') nutrient
        WHERE try_cast(json_extract_string(nutrient.value, '$.value') AS DOUBLE) IS NULL
            OR NOT isfinite(try_cast(json_extract_string(nutrient.value, '$.value') AS DOUBLE))
    """).fetchone()[0]
    fail_if_false(
        {"source_values_numeric_and_finite": invalid_values == 0},
        "Nutritionals values",
    )


def _choose_canonical_versions(
    con: duckdb.DuckDBPyConnection, clean: pl.DataFrame
) -> tuple[dict, int]:
    """Collapse exact duplicates, then keep one label per product, shop and date.

    Creates the ``canonical`` temp table. Returns the resolution statistics and the
    number of exact duplicates removed.
    """
    deduped = (
        clean.with_row_index("_source_row_id")
        .with_columns(
            pl.len().over(NUTRITIONALS_EXACT_KEY).alias("_exact_duplicate_count")
        )
        .unique(subset=NUTRITIONALS_EXACT_KEY, keep="first", maintain_order=True)
        .with_columns(
            (pl.col("_exact_duplicate_count") > 1).alias("dq_source_exact_duplicate")
        )
    )
    con.register("nutritionals_deduped_source", deduped)
    # Compute expensive JSON completeness/ranking once for metrics and export.
    con.execute("CREATE TEMP TABLE canonical AS " + CANONICAL_QUERY)
    resolution = (
        con.sql("""
        SELECT count(*) AS canonical_observations,
            count(*) FILTER (WHERE dq_repeated_grain) AS repeated_grains,
            count(*) FILTER (WHERE dq_conflicting_nutrition) AS conflicts,
            count(*) FILTER (WHERE dq_language_conflict) AS language_conflicts,
            count(*) FILTER (
                WHERE dq_repeated_grain AND completeness_tie_count > 1
            ) AS hash_tiebreaks,
            sum(nutrient_count) AS expected_nutrient_rows,
            sum(grain_row_count) AS represented_source_rows
        FROM canonical
    """)
        .pl()
        .row(0, named=True)
    )
    fail_if_false(
        {
            "canonical_observations_not_empty": resolution["canonical_observations"]
            > 0,
            "all_deduped_observations_accounted_for": resolution[
                "represented_source_rows"
            ]
            == deduped.height,
        },
        "Nutritionals reconciliation",
    )
    return resolution, clean.height - deduped.height


def _nutrient_rows_query() -> str:
    """SQL of the Silver table: one row per canonical label and nutrient."""
    query = f"""
        WITH source_long AS (
            SELECT n.daltix_id, n.shop, n.country, n.language, n.download_date,
                -- Energy in both units of the same label, to cross-check them.
                CASE WHEN json_extract_string(
                        n.nutritional_values_std, '$.nutrients.energy.unit'
                    ) = 'kJ'
                    THEN try_cast(json_extract_string(
                        n.nutritional_values_std, '$.nutrients.energy.value'
                    ) AS DOUBLE)
                END AS _energy_kj,
                CASE WHEN json_extract_string(
                        n.nutritional_values_std, '$.nutrients.kilocalories.unit'
                    ) = 'kcal'
                    THEN try_cast(json_extract_string(
                        n.nutritional_values_std, '$.nutrients.kilocalories.value'
                    ) AS DOUBLE)
                END AS _energy_kcal,
                nutrient.key AS nutrient_name_raw,
                json_extract_string(nutrient.value, '$.value') AS nutrient_value_raw,
                json_extract_string(nutrient.value, '$.unit') AS nutrient_unit_raw,
                cast(json_extract_string(
                    n.nutritional_values_std, '$.portion.value'
                ) AS DOUBLE) AS portion_value,
                json_extract_string(
                    n.nutritional_values_std, '$.portion.unit'
                ) AS portion_unit,
                md5(n.nutritional_values_std) AS source_payload_hash,
                n.nutrient_count, n.numeric_nutrient_count, n.populated_unit_count,
                n.dq_source_exact_duplicate, n.dq_repeated_grain,
                n.dq_conflicting_nutrition, n.dq_language_conflict,
                CASE WHEN n.dq_repeated_grain AND n.completeness_tie_count > 1
                    THEN 'deterministic_hash_tiebreak'
                    ELSE n.nutrition_resolution_rule END AS nutrition_selection_reason
            FROM canonical n
            CROSS JOIN LATERAL json_each(n.nutritional_values_std, '$.nutrients') nutrient
        ), standardized AS (
            SELECT s.*, m.canonical_nutrient_name AS nutrient_name,
                CASE WHEN m.mapping_status = 'incompatible_unit' THEN NULL
                    ELSE cast(s.nutrient_value_raw AS DOUBLE) * m.conversion_factor
                    END AS nutrient_value,
                m.canonical_unit AS nutrient_unit,
                m.conversion_factor AS unit_conversion_factor,
                m.mapping_status AS unit_standardization_status
            FROM source_long s
            JOIN nutrient_mapping m ON s.nutrient_name_raw = m.raw_nutrient_name
                AND s.nutrient_unit_raw = m.raw_unit
        )
        SELECT * EXCLUDE (_energy_kj, _energy_kcal),
            cast(nutrient_value_raw AS DOUBLE) < 0 AS dq_negative_nutrient_value,
            CASE WHEN _energy_kj >= 0 AND _energy_kcal >= 0
                THEN abs(_energy_kj - _energy_kcal * {ENERGY_KJ_PER_KCAL})
            END AS energy_pair_difference_kj,
            coalesce(
                _energy_kj >= 0 AND _energy_kcal >= 0
                AND abs(_energy_kj - _energy_kcal * {ENERGY_KJ_PER_KCAL}) >
                    greatest(
                        {ENERGY_ABSOLUTE_TOLERANCE_KJ},
                        abs(_energy_kcal * {ENERGY_KJ_PER_KCAL})
                            * {ENERGY_RELATIVE_TOLERANCE}
                    ),
                FALSE
            ) AS dq_energy_kcal_mismatch,
            nutrient_value_raw IS NULL AS dq_missing_nutrient_value,
            nutrient_value_raw IS NOT NULL
                AND try_cast(nutrient_value_raw AS DOUBLE) IS NULL
                AS dq_non_numeric_value,
            unit_standardization_status = 'incompatible_unit'
                AS dq_incompatible_nutrient_unit,
            coalesce(nutrient_unit = 'g' AND portion_unit = 'g'
                AND nutrient_value > portion_value, FALSE) AS dq_mass_exceeds_portion
        FROM standardized
    """
    # Add names after all business transformations; shop remains the key.
    return f"SELECT *, {shop_name_sql()} AS shop_name FROM ({query}) named_source"


def _written_metrics(con: duckdb.DuckDBPyConnection) -> dict:
    """Quality and business metrics read from the persisted Silver file.

    Expects the view ``written`` over the file and the ``nutrient_mapping`` table.
    """
    metrics = (
        con.sql("""
        SELECT count(*) AS rows, count(DISTINCT daltix_id) AS distinct_products,
            count(DISTINCT nutrient_name) AS distinct_nutrients,
            count(*) FILTER (WHERE nutrient_value IS NULL) AS missing_numeric_values,
            count(*) FILTER (WHERE dq_non_numeric_value) AS non_numeric_values,
            count(*) FILTER (WHERE nutrient_unit IS NULL) AS missing_units,
            count(*) FILTER (WHERE dq_incompatible_nutrient_unit)
                AS unconvertible_unit_rows,
            count(*) FILTER (WHERE unit_standardization_status = 'identity')
                AS identity_unit_rows,
            count(*) FILTER (WHERE unit_standardization_status = 'converted')
                AS converted_unit_rows,
            count(*) FILTER (WHERE nutrient_value IS NOT NULL)
                AS canonical_numeric_rows,
            count(*) FILTER (WHERE dq_mass_exceeds_portion)
                AS mass_exceeds_portion_rows,
            count(DISTINCT daltix_id) FILTER (WHERE dq_mass_exceeds_portion)
                AS mass_exceeds_portion_products,
            count(*) FILTER (WHERE nutrient_value < 0) AS negative_nutrient_rows,
            count(*) FILTER (WHERE dq_negative_nutrient_value)
                AS negative_source_nutrient_rows,
            count(DISTINCT (daltix_id, shop, country, download_date))
                FILTER (WHERE energy_pair_difference_kj IS NOT NULL)
                AS comparable_energy_observations,
            count(DISTINCT (daltix_id, shop, country, download_date))
                FILTER (WHERE dq_energy_kcal_mismatch)
                AS energy_mismatch_observations,
            count(*) FILTER (WHERE dq_energy_kcal_mismatch)
                AS energy_mismatch_nutrient_rows,
            count(*) FILTER (
                WHERE dq_negative_nutrient_value
                        IS DISTINCT FROM (cast(nutrient_value_raw AS DOUBLE) < 0)
                    OR (dq_energy_kcal_mismatch AND energy_pair_difference_kj IS NULL)
            ) AS invalid_quality_flags,
            count(*) FILTER (
                WHERE nutrient_name_raw IS NULL OR nutrient_value_raw IS NULL
                    OR nutrient_unit_raw IS NULL OR source_payload_hash IS NULL
            ) AS missing_lineage_rows,
            count(*) FILTER (WHERE
                (dq_incompatible_nutrient_unit
                    AND (nutrient_value IS NOT NULL
                         OR unit_conversion_factor IS NOT NULL))
                OR (NOT dq_incompatible_nutrient_unit AND
                    (nutrient_value IS NULL OR unit_conversion_factor IS NULL
                     OR NOT isfinite(unit_conversion_factor)
                     OR unit_conversion_factor <= 0
                     OR nutrient_value IS DISTINCT FROM
                        cast(nutrient_value_raw AS DOUBLE) * unit_conversion_factor))
            ) AS invalid_conversion_rows,
            count(*) FILTER (WHERE dq_conflicting_nutrition)
                AS rows_from_resolved_conflicts,
            count(*) FILTER (
                WHERE nutrient_value IS NOT NULL AND NOT isfinite(nutrient_value)
            ) AS non_finite_values,
            count(*) FILTER (
                WHERE nutrient_name IS NULL OR trim(nutrient_name) = ''
            ) AS missing_nutrient_names,
            count(*) - count(DISTINCT (
                daltix_id, shop, country, download_date, nutrient_name
            )) AS duplicate_grains,
            count(DISTINCT (daltix_id, shop, country, download_date))
                AS written_observations
        FROM written
    """)
        .pl()
        .row(0, named=True)
    )
    metrics["canonical_unit_mismatch_rows"] = con.sql("""
        SELECT count(*) FROM written w
        ANTI JOIN (SELECT DISTINCT canonical_nutrient_name, canonical_unit FROM nutrient_mapping) m
            ON w.nutrient_name = m.canonical_nutrient_name AND w.nutrient_unit = m.canonical_unit
    """).fetchone()[0]
    return metrics


def _validate_nutrition_output(metrics: dict, resolution: dict) -> None:
    """The written table must reconcile with the chosen labels and stay consistent."""
    fail_if_false(
        {
            "row_count_reconciled": metrics["rows"]
            == resolution["expected_nutrient_rows"],
            "observation_count_reconciled": metrics["written_observations"]
            == resolution["canonical_observations"],
            "normalized_grain_unique": metrics["duplicate_grains"] == 0,
            "quality_flags_consistent": metrics["invalid_quality_flags"] == 0,
            "numeric_values_finite": metrics["non_finite_values"] == 0,
            "nutrient_names_complete": metrics["missing_nutrient_names"] == 0,
            "canonical_units_complete_and_consistent": metrics[
                "canonical_unit_mismatch_rows"
            ]
            == 0,
            "raw_lineage_preserved": metrics["missing_lineage_rows"] == 0,
            "conversion_or_explicit_unknown": metrics["invalid_conversion_rows"] == 0,
            "only_incompatible_units_have_unknown_values": metrics[
                "missing_numeric_values"
            ]
            == metrics["unconvertible_unit_rows"],
        },
        "Nutritionals output",
    )


def run_nutritionals_pipeline(raw_path: Path, silver_path: Path) -> dict[str, int]:
    """Keep the completeness/hash policy; preserve uncertainty and nutrient context.

    Source grain: product/shop/country/date. Silver adds nutrient_name.
    A selected version is reproducible, not proven true. Raw retains all versions.
    Canonical NULLs mean known incompatible units; source values remain available.
    """
    protect_inputs(raw_path, silver_path)
    clean = _read_clean_nutritionals(raw_path)

    with duckdb.connect() as con:
        con.register("source", clean)
        _check_nutrition_source(con)
        resolution, exact_removed = _choose_canonical_versions(con, clean)

        silver_path.parent.mkdir(parents=True, exist_ok=True)
        # The orchestrator supplies a staging path; the 9.5M rows stay out of Python.
        # Sorted by the long grain so that the file is identical on every run.
        con.execute(
            f"COPY (SELECT * FROM ({_nutrient_rows_query()}) "
            "ORDER BY daltix_id, shop, country, download_date, nutrient_name) "
            f"TO {quote_literal(silver_path.as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        con.read_parquet(str(silver_path)).create_view("written")
        metrics = _written_metrics(con)
        _validate_nutrition_output(metrics, resolution)

    return {
        **{k: int(metrics[k]) for k in METRIC_KEYS},
        "exact_duplicates_removed": exact_removed,
        "canonical_observations": int(resolution["canonical_observations"]),
        # These count canonicalized labels; a chosen label is reproducible, not proven.
        "resolved_repeated_grains": int(resolution["repeated_grains"]),
        "resolved_nutritional_conflicts": int(resolution["conflicts"]),
        "resolved_language_conflicts": int(resolution["language_conflicts"]),
        "hash_tiebreak_grains": int(resolution["hash_tiebreaks"]),
    }
