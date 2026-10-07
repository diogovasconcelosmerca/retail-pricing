"""Silver location datasets: locations and weekly locations.

Coordinates and postcodes stay nullable. A location key that appears twice is
flagged and excluded from enrichment, never silently de-duplicated.
"""

from pathlib import Path

import polars as pl

from retail_pricing.quality import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_finite,
    require_unique,
    write_dataset,
)
from retail_pricing.reference import (
    REVERSED_SHOP_TYPES,
    SHOP_TYPE_MAPPING,
    add_shop_names,
    belgian_region,
    canonicalize_shop,
    location_type,
    outside_benelux,
    require_canonical_shops,
)

# --- Locations -----------------------------------------------------------------


LOCATION_REQUIRED_COLUMNS = [
    "shop",
    "country_code",
    "id",
    "type",
    "geolocation_latitude",
    "geolocation_longitude",
    "postcode",
    "sources",
]


LOCATION_TEXT_COLUMNS = [
    "shop",
    "country_code",
    "id",
    "type",
    "postcode",
    "sources",
]


def _standardise_locations(locations_raw: pl.DataFrame) -> pl.DataFrame:
    """Decode the shop, normalise text and type the coordinates as Float64."""
    standard = (
        canonicalize_shop(locations_raw)
        .with_columns([normalize_text(column) for column in LOCATION_TEXT_COLUMNS])
        .with_columns(
            [
                pl.col("geolocation_latitude").cast(pl.Float64, strict=True),
                pl.col("geolocation_longitude").cast(pl.Float64, strict=True),
            ]
        )
    )
    require_finite(standard, ["geolocation_latitude", "geolocation_longitude"])
    return standard


def _drop_empty_type(locations: pl.DataFrame) -> pl.DataFrame:
    """Drop `type`, which the source leaves empty, but never silently.

    If a future extract starts populating it, the build stops for a review.
    """
    if locations["type"].drop_nulls().len() > 0:
        raise RuntimeError(
            "Locations: `type` now contains information "
            "and must be reviewed before being removed."
        )
    return locations.drop("type")


def _flag_locations(locations: pl.DataFrame) -> pl.DataFrame:
    """Flag ambiguous keys and geographic problems; never drop or move a row.

    Ambiguous shop + id records stay available but are excluded from automatic
    enrichment. Missing coordinates and legitimate geographic extremes are not
    errors.
    """
    return (
        locations.with_columns(
            pl.len().over(["shop", "id"]).alias("_business_key_count")
        )
        .with_columns(
            [
                (pl.col("_business_key_count") > 1).alias("dq_business_key_collision"),
            ]
        )
        .drop("_business_key_count")
        .with_columns(
            [
                (
                    pl.col("geolocation_latitude").is_not_null()
                    & (
                        (pl.col("geolocation_latitude") < -90)
                        | (pl.col("geolocation_latitude") > 90)
                    )
                ).alias("dq_invalid_latitude"),
                (
                    pl.col("geolocation_longitude").is_not_null()
                    & (
                        (pl.col("geolocation_longitude") < -180)
                        | (pl.col("geolocation_longitude") > 180)
                    )
                ).alias("dq_invalid_longitude"),
                (
                    pl.col("geolocation_latitude").is_null()
                    != pl.col("geolocation_longitude").is_null()
                ).alias("dq_incomplete_coordinates"),
                # A review flag only: the row and its coordinates are kept as they are.
                outside_benelux().alias("dq_outside_benelux"),
            ]
        )
    )


def _validate_locations(locations: pl.DataFrame, locations_raw: pl.DataFrame) -> int:
    """Stop before writing if a structural rule fails; return the flagged rows.

    A missing postcode or a complete missing coordinate pair is allowed.
    """
    collision_groups = (
        locations.group_by(["shop", "id"]).len().filter(pl.col("len") > 1)
    )
    actual_collision_rows = (
        int(collision_groups["len"].sum()) if collision_groups.height > 0 else 0
    )
    flagged_collision_rows = int(locations["dq_business_key_collision"].sum())

    fail_if_false(
        {
            "row_count_preserved": locations.height == locations_raw.height,
            "id_complete": locations["id"].null_count() == 0,
            "shop_complete": locations["shop"].null_count() == 0,
            "country_complete": locations["country_code"].null_count() == 0,
            "empty_type_removed_safely": "type" not in locations.columns,
            "business_key_collisions_fully_flagged": flagged_collision_rows
            == actual_collision_rows,
            "valid_latitudes": locations["dq_invalid_latitude"].sum() == 0,
            "valid_longitudes": locations["dq_invalid_longitude"].sum() == 0,
            "coordinate_pairs_consistent": locations["dq_incomplete_coordinates"].sum()
            == 0,
        },
        "Locations",
    )
    return flagged_collision_rows


def run_locations_pipeline(
    raw_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """
    Build the Silver Locations dataset.

    Working grain:
        One location record within retailer context.

    Working business key:
        shop + id.

    Known business-key collisions are preserved and flagged rather than
    arbitrarily deduplicated.
    """
    protect_inputs(raw_path, silver_path)
    locations_raw = pl.read_parquet(raw_path)
    require_columns(locations_raw, LOCATION_REQUIRED_COLUMNS)

    locations = _flag_locations(_drop_empty_type(_standardise_locations(locations_raw)))
    flagged_collision_rows = _validate_locations(locations, locations_raw)

    locations = add_shop_names(locations)
    written_rows = write_dataset(locations, silver_path, "Locations")

    return {
        "rows": written_rows,
        "business_key_collision_rows": flagged_collision_rows,
        "missing_postcode": locations["postcode"].null_count(),
        "missing_coordinates": locations["geolocation_latitude"].null_count(),
        "invalid_latitudes": int(locations["dq_invalid_latitude"].sum()),
        "invalid_longitudes": int(locations["dq_invalid_longitude"].sum()),
        "outside_benelux": int(locations["dq_outside_benelux"].sum()),
    }


# --- Weekly locations ----------------------------------------------------------


WEEKLY_LOCATION_REQUIRED_COLUMNS = [
    "shop",
    "location",
    "location_name",
    "shop_type",
    "geolocation_latitude",
    "geolocation_longitude",
    "locality",
    "postcode",
    "state",
]

WEEKLY_LOCATION_TEXT_COLUMNS = [
    "shop",
    "location",
    "location_name",
    "shop_type",
    "locality",
    "postcode",
    "state",
]

COORDINATE_TOLERANCE = 0.00001


FALLBACK_REQUIRED_COLUMNS = [
    "shop",
    "id",
    "country_code",
    "postcode",
    "geolocation_latitude",
    "geolocation_longitude",
    "dq_business_key_collision",
]


def _standardise_weekly_locations(raw: pl.DataFrame) -> pl.DataFrame:
    """Trim text, keep the raw shop type, then decode only the assessed labels."""
    # Keep the exact source spelling before trimming, null handling or decoding.
    clean = (
        canonicalize_shop(raw)
        .with_columns(pl.col("shop_type").cast(pl.String).alias("shop_type_raw"))
        .with_columns([normalize_text(c) for c in WEEKLY_LOCATION_TEXT_COLUMNS])
        .with_columns(
            [
                pl.col("geolocation_latitude").cast(pl.Float64),
                pl.col("geolocation_longitude").cast(pl.Float64),
            ]
        )
    )
    # Mixed readable/reversed inputs are safe.
    clean = clean.with_columns(pl.col("shop_type").str.to_lowercase())
    unknown = (
        clean.filter(
            pl.col("shop_type").is_not_null()
            & ~pl.col("shop_type").is_in(list(SHOP_TYPE_MAPPING))
        )["shop_type"]
        .unique()
        .sort()
        .to_list()
    )
    if unknown:
        raise ValueError(
            f"Weekly Locations: unknown shop_type values require review: {unknown[:20]}"
        )
    return clean.with_columns(
        pl.col("shop_type")
        .is_in(list(REVERSED_SHOP_TYPES))
        .fill_null(False)
        .alias("is_shop_type_reversed"),
        pl.col("shop_type")
        .replace_strict(SHOP_TYPE_MAPPING, return_dtype=pl.String)
        .alias("shop_type"),
    )


def _match_fallback(clean: pl.DataFrame, fallback: pl.DataFrame) -> pl.DataFrame:
    """Join the enrichment-safe locations reference and flag contradictions.

    Contradictions are detected on the original primary attributes, before any
    filling, and a contradicting location is never enriched.
    """
    safe = (
        fallback.filter(~pl.col("dq_business_key_collision"))
        .select(
            [
                "shop",
                "id",
                "country_code",
                "postcode",
                "geolocation_latitude",
                "geolocation_longitude",
            ]
        )
        .rename(
            {
                "id": "location",
                "country_code": "fallback_country_code",
                "postcode": "fallback_postcode",
                "geolocation_latitude": "fallback_latitude",
                "geolocation_longitude": "fallback_longitude",
            }
        )
    )
    joined = clean.join(safe, on=["shop", "location"], how="left", validate="1:1")

    return (
        joined.with_columns(
            [
                (
                    pl.col("postcode").is_not_null()
                    & pl.col("fallback_postcode").is_not_null()
                    & (pl.col("postcode") != pl.col("fallback_postcode"))
                )
                .fill_null(False)
                .alias("dq_postcode_conflict"),
                (
                    pl.col("geolocation_latitude").is_not_null()
                    & pl.col("fallback_latitude").is_not_null()
                    & (
                        (
                            pl.col("geolocation_latitude") - pl.col("fallback_latitude")
                        ).abs()
                        > COORDINATE_TOLERANCE
                    )
                )
                .fill_null(False)
                .alias("dq_latitude_conflict"),
                (
                    pl.col("geolocation_longitude").is_not_null()
                    & pl.col("fallback_longitude").is_not_null()
                    & (
                        (
                            pl.col("geolocation_longitude")
                            - pl.col("fallback_longitude")
                        ).abs()
                        > COORDINATE_TOLERANCE
                    )
                )
                .fill_null(False)
                .alias("dq_longitude_conflict"),
            ]
        )
        .with_columns(
            pl.any_horizontal(
                "dq_postcode_conflict", "dq_latitude_conflict", "dq_longitude_conflict"
            ).alias("dq_enrichment_context_mismatch")
        )
        .with_columns(
            (
                pl.col("fallback_country_code").is_not_null()
                & ~pl.col("dq_enrichment_context_mismatch")
            ).alias("_context_safe")
        )
    )


def _fill_from_fallback(joined: pl.DataFrame) -> pl.DataFrame:
    """Fill a missing postcode or coordinate pair only from a context-safe match."""
    enriched = joined.with_columns(
        [
            (
                pl.col("_context_safe")
                & pl.col("postcode").is_null()
                & pl.col("fallback_postcode").is_not_null()
            ).alias("is_postcode_enriched"),
            (
                pl.col("_context_safe")
                & pl.col("geolocation_latitude").is_null()
                & pl.col("geolocation_longitude").is_null()
                & pl.col("fallback_latitude").is_not_null()
                & pl.col("fallback_longitude").is_not_null()
            ).alias("is_coordinates_enriched"),
            pl.when(pl.col("_context_safe"))
            .then(pl.col("fallback_country_code"))
            .otherwise(None)
            .alias("country_code"),
        ]
    ).with_columns(
        [
            pl.when(pl.col("is_postcode_enriched"))
            .then(pl.col("fallback_postcode"))
            .otherwise(pl.col("postcode"))
            .alias("postcode"),
            pl.when(pl.col("is_coordinates_enriched"))
            .then(pl.col("fallback_latitude"))
            .otherwise(pl.col("geolocation_latitude"))
            .alias("geolocation_latitude"),
            pl.when(pl.col("is_coordinates_enriched"))
            .then(pl.col("fallback_longitude"))
            .otherwise(pl.col("geolocation_longitude"))
            .alias("geolocation_longitude"),
        ]
    )
    require_finite(enriched, ["geolocation_latitude", "geolocation_longitude"])
    return enriched


def _flag_geography(enriched: pl.DataFrame) -> pl.DataFrame:
    """Add coordinate flags plus the deterministic location type and region."""
    # Flags describe final geography, including any accepted fallback values.
    flagged = enriched.with_columns(
        [
            (~pl.col("geolocation_latitude").is_between(-90, 90))
            .fill_null(False)
            .alias("dq_invalid_latitude"),
            (~pl.col("geolocation_longitude").is_between(-180, 180))
            .fill_null(False)
            .alias("dq_invalid_longitude"),
            (
                pl.col("geolocation_latitude").is_null()
                != pl.col("geolocation_longitude").is_null()
            ).alias("dq_incomplete_coordinates"),
            outside_benelux().alias("dq_outside_benelux"),
            # Deterministic attributes from a small reviewed reference (reference.py).
            location_type().alias("location_type"),
            belgian_region().alias("region"),
        ]
    )
    return flagged.drop(
        [
            "fallback_country_code",
            "fallback_postcode",
            "fallback_latitude",
            "fallback_longitude",
            "_context_safe",
        ]
    )


def _validate_weekly_locations(
    result: pl.DataFrame, clean: pl.DataFrame, raw: pl.DataFrame
) -> None:
    """Structural contract: no row lost, raw labels kept, no unsafe enrichment."""
    fail_if_false(
        {
            "row_count_preserved": result.height == raw.height,
            "shop_type_raw_preserved": clean["shop_type_raw"].equals(
                raw["shop_type"].cast(pl.String)
            ),
            "shop_type_missingness_preserved": result["shop_type"].null_count()
            == raw.select(normalize_text("shop_type"))["shop_type"].null_count(),
            "shop_types_readable": result["shop_type"]
            .drop_nulls()
            .is_in(list(REVERSED_SHOP_TYPES.values()))
            .all(),
            "shop_complete": result["shop"].null_count() == 0,
            "location_complete": result["location"].null_count() == 0,
            "business_key_unique": result.select("shop", "location").unique().height
            == result.height,
            "location_type_complete": result["location_type"].null_count() == 0,
            "region_only_for_four_digit_postcodes": result.filter(
                pl.col("region").is_not_null()
                & ~pl.col("postcode").str.contains(r"^\d{4}$").fill_null(False)
            ).height
            == 0,
            "valid_latitudes": result["dq_invalid_latitude"].sum() == 0,
            "valid_longitudes": result["dq_invalid_longitude"].sum() == 0,
            "coordinate_pairs_consistent": result["dq_incomplete_coordinates"].sum()
            == 0,
            "no_unsafe_enrichment": result.filter(
                pl.col("dq_enrichment_context_mismatch")
                & (
                    pl.col("is_postcode_enriched")
                    | pl.col("is_coordinates_enriched")
                    | pl.col("country_code").is_not_null()
                )
            ).height
            == 0,
        },
        "Weekly Locations",
    )


def run_weekly_locations_pipeline(
    raw_path: Path,
    fallback_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """Preserve one row per shop/location and never overwrite primary attributes."""
    protect_inputs(raw_path, silver_path, fallback_path)
    raw = pl.read_parquet(raw_path)
    fallback = pl.read_parquet(fallback_path)
    require_canonical_shops(fallback["shop"].unique().to_list())
    require_columns(raw, WEEKLY_LOCATION_REQUIRED_COLUMNS)
    require_columns(fallback, FALLBACK_REQUIRED_COLUMNS, exact=False)

    clean = _standardise_weekly_locations(raw)
    require_unique(clean, ["shop", "location"], "Weekly Locations")
    require_finite(clean, ["geolocation_latitude", "geolocation_longitude"])

    joined = _match_fallback(clean, fallback)
    result = _flag_geography(_fill_from_fallback(joined))
    _validate_weekly_locations(result, clean, raw)

    result = add_shop_names(result)
    rows = write_dataset(
        result, silver_path, "Weekly Locations", unique_key=["shop", "location"]
    )
    return {
        "rows": rows,
        "safe_fallback_matches": int(joined["_context_safe"].sum()),
        "context_mismatches": int(result["dq_enrichment_context_mismatch"].sum()),
        "postcodes_enriched": int(result["is_postcode_enriched"].sum()),
        "coordinates_enriched": int(result["is_coordinates_enriched"].sum()),
        "countries_enriched": result["country_code"].len()
        - result["country_code"].null_count(),
        "remaining_missing_postcode": result["postcode"].null_count(),
        "remaining_missing_coordinates": result["geolocation_latitude"].null_count(),
        "missing_shop_type": result["shop_type"].null_count(),
        "outside_benelux": int(result["dq_outside_benelux"].sum()),
        "national_price_contexts": int(
            (result["location_type"] == "National price (online)").sum()
        ),
        "regions_assigned": result["region"].len() - result["region"].null_count(),
        "shop_types_reversed": int(result["is_shop_type_reversed"].sum()),
        "shop_types_already_readable": result.filter(
            pl.col("shop_type").is_not_null() & ~pl.col("is_shop_type_reversed")
        ).height,
    }
