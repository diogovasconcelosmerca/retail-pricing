"""Reviewed reference data: shop codes and names, shop types, nutrient units.

Every mapping here was assessed against the complete Raw domain. Unknown values
stop the build for review instead of being guessed.
"""

import math

import polars as pl

from retail_pricing.quality import SEMANTIC_NULLS, normalize_text
from retail_pricing.raw import quote_identifier, quote_literal

# --- Retailer codes and names --------------------------------------------------


# Complete distinct domain observed across all seven Raw sources, not examples.
ENCODED_SHOPS = ("frc", "gc", "ha", "idla", "ldil", "lld", "obmuj", "plc", "raps")


CANONICAL_SHOPS = tuple(code[::-1] for code in ENCODED_SHOPS)


if set(ENCODED_SHOPS) & set(CANONICAL_SHOPS):
    raise ValueError("Shop orientations overlap; review decoding before extending it.")


# Reviewed names are distinct from deterministic source-code decoding.
# See docs/silver-data-quality.md for the evidence and the reviewed cg attribution.
SHOP_NAMES = {
    "ah": "Albert Heijn",
    "aldi": "Aldi",
    "cg": "Collect&Go",  # Reviewed case attribution; retain cg as the identity.
    "clp": "Colruyt Lowest Prices",
    "crf": "Carrefour",
    "dll": "Delhaize",
    "jumbo": "Jumbo",
    "lidl": "Lidl",
    "spar": "SPAR",
}


if set(SHOP_NAMES) != set(CANONICAL_SHOPS):
    raise ValueError("Every reviewed shop code requires a reporting label.")


def add_shop_names(frame: pl.DataFrame) -> pl.DataFrame:
    """Add reporting names without changing source codes, rows or other fields."""
    require_canonical_shops(frame["shop"].drop_nulls().unique().to_list())
    return frame.with_columns(
        pl.col("shop")
        .cast(pl.String)
        .replace_strict(SHOP_NAMES, return_dtype=pl.String)
        .alias("shop_name")
    )


def shop_name_sql(column: str = "shop") -> str:
    """Use the same reviewed names in native DuckDB without Python materialization."""
    value = quote_identifier(column)
    cases = " ".join(
        f"WHEN {value} = {quote_literal(code)} THEN {quote_literal(name)}"
        for code, name in SHOP_NAMES.items()
    )
    unknown = "error('Unknown canonical shop requires name review')"
    return f"CASE WHEN {value} IS NULL THEN NULL {cases} ELSE {unknown} END"


def require_shop_names(frame: pl.DataFrame) -> None:
    """Reject missing or inconsistent labels; never repair them inside Gold."""
    if "shop_name" not in frame.columns:
        raise ValueError("Missing Silver shop_name; rebuild Silver before Gold.")
    expected = add_shop_names(frame.select("shop"))["shop_name"]
    if not frame["shop_name"].equals(expected):
        raise ValueError(
            "Shop names differ from the reviewed reference; rebuild Silver."
        )


def canonicalize_shop(frame: pl.DataFrame) -> pl.DataFrame:
    """Preserve NULLs/rows and decode only reviewed source codes, idempotently."""
    clean = frame.with_columns(normalize_text("shop"))
    values = clean["shop"].drop_nulls().unique().to_list()
    unknown = sorted(set(values) - set(ENCODED_SHOPS) - set(CANONICAL_SHOPS))
    if unknown:
        raise ValueError(f"Unknown shop values require review: {unknown[:20]}")
    return clean.with_columns(
        pl.when(pl.col("shop").is_in(ENCODED_SHOPS))
        .then(pl.col("shop").str.reverse())
        .otherwise(pl.col("shop"))
        .alias("shop")
    )


def require_canonical_shops(values: list[str | None]) -> None:
    """Reject stale encoded Silver references before joins or publication."""
    unexpected = sorted({v for v in values if v is not None} - set(CANONICAL_SHOPS))
    if unexpected:
        raise ValueError(
            f"Silver shop values must be canonical; rebuild upstream: {unexpected[:20]}"
        )


def canonical_shop_sql(column: str = "shop") -> str:
    """Equivalent native DuckDB expression; large prices never enter Python."""
    # Trim edge whitespace only; never change casing or expand abbreviations.
    value = (
        f"trim(regexp_replace(CAST({quote_identifier(column)} AS VARCHAR), "
        r"'^\s+|\s+$', '', 'g'))"
    )
    encoded = ", ".join(quote_literal(v) for v in ENCODED_SHOPS)
    canonical = ", ".join(quote_literal(v) for v in CANONICAL_SHOPS)
    nulls = ", ".join(quote_literal(v) for v in SEMANTIC_NULLS)
    return f"""CASE
        WHEN {value} IS NULL OR lower({value}) IN ({nulls}) THEN NULL
        WHEN {value} IN ({encoded}) THEN reverse({value})
        WHEN {value} IN ({canonical}) THEN {value}
        ELSE error('Unknown shop values require review')
    END"""


# --- Weekly-location shop types ------------------------------------------------


REVERSED_SHOP_TYPES = {
    "da": "ad",
    "hgadtsem": "mestdagh",
    "og & pohs": "shop & go",
    "reileta hserf": "fresh atelier",
    "sserpxe": "express",
    "tekram": "market",
    "tekramrepus": "supermarket",
    "tkramrepyh": "hypermarkt",
    "yxorp": "proxy",
}


SHOP_TYPE_MAPPING = {
    **REVERSED_SHOP_TYPES,
    **{label: label for label in REVERSED_SHOP_TYPES.values()},
}


# --- Nutrient / unit mapping ---------------------------------------------------


MAPPING_COLUMNS = [
    "raw_nutrient_name",
    "raw_unit",
    "canonical_nutrient_name",
    "canonical_unit",
    "conversion_factor",
    "mapping_status",
]


# Source name/unit, canonical name/unit, multiplier, interpretation.
NUTRIENT_MAPPING = (
    ("carbohydrates", "g", "carbohydrates", "g", 1.0, "identity"),
    ("energy", "kJ", "energy", "kJ", 1.0, "identity"),
    ("fats", "g", "fats", "g", 1.0, "identity"),
    ("fibers", "g", "fibers", "g", 1.0, "identity"),
    ("kilocalories", "kcal", "kilocalories", "kcal", 1.0, "identity"),
    ("monounsaturated_fats", "g", "monounsaturated_fats", "g", 1.0, "identity"),
    ("polyols", "g", "polyols", "g", 1.0, "identity"),
    ("polyunsaturated_fats", "g", "polyunsaturated_fats", "g", 1.0, "identity"),
    ("proteins", "g", "proteins", "g", 1.0, "identity"),
    ("salt", "g", "salt", "g", 1.0, "identity"),
    ("saturated_fats", "g", "saturated_fats", "g", 1.0, "identity"),
    ("starch", "g", "starch", "g", 1.0, "identity"),
    ("sugars", "g", "sugars", "g", 1.0, "identity"),
    ("unsaturated_fats", "g", "unsaturated_fats", "g", 1.0, "identity"),
    ("energy", "g", "energy", "kJ", None, "incompatible_unit"),
    ("fats", "kJ", "fats", "g", None, "incompatible_unit"),
    (
        "polyunsaturated_fats",
        "kJ",
        "polyunsaturated_fats",
        "g",
        None,
        "incompatible_unit",
    ),
    ("salt", "ml", "salt", "g", None, "incompatible_unit"),
    ("sugars", "ml", "sugars", "g", None, "incompatible_unit"),
    ("unsaturated_fats", "kJ", "unsaturated_fats", "g", None, "incompatible_unit"),
)


def validate_nutrient_mapping(mapping: pl.DataFrame) -> None:
    """Reject ambiguous keys, inconsistent canonical units and invented factors."""
    if mapping.columns != MAPPING_COLUMNS or mapping.is_empty():
        raise ValueError("Nutrient mapping must have the declared columns and rows")
    keys = set()
    canonical_units = {}
    for name, unit, canonical, target_unit, factor, status in mapping.iter_rows():
        if any(
            not isinstance(v, str) or not v.strip()
            for v in (name, unit, canonical, target_unit)
        ):
            raise ValueError("Nutrient mapping names and units must be populated")
        if (name, unit) in keys:
            raise ValueError(f"Duplicate nutrient mapping key: {(name, unit)}")
        keys.add((name, unit))
        if canonical in canonical_units and canonical_units[canonical] != target_unit:
            raise ValueError(f"Inconsistent canonical unit for {canonical}")
        canonical_units[canonical] = target_unit
        if status == "incompatible_unit":
            if factor is not None or unit == target_unit:
                raise ValueError(
                    "Incompatible units require a NULL factor and distinct units"
                )
        elif status in {"identity", "converted"}:
            if factor is None or not math.isfinite(factor) or factor <= 0:
                raise ValueError("Conversion factors must be finite and positive")
            if status == "identity" and (factor != 1.0 or unit != target_unit):
                raise ValueError("Identity mappings must preserve values and units")
            if status == "converted" and unit == target_unit:
                raise ValueError("Conversions must explicitly change the source unit")
        else:
            raise ValueError(f"Unknown nutrient mapping status: {status}")


def get_nutrient_mapping() -> pl.DataFrame:
    """Return the same validated reference used by the pipeline and notebook."""
    mapping = pl.DataFrame(
        NUTRIENT_MAPPING,
        schema=[
            (c, pl.Float64 if c == "conversion_factor" else pl.String)
            for c in MAPPING_COLUMNS
        ],
        orient="row",
    )
    validate_nutrient_mapping(mapping)
    return mapping


# --- Geography and price contexts ------------------------------------------------

# Rough bounding box of Belgium, the Netherlands and Luxembourg together. A location
# whose coordinates fall outside it is flagged for review, never moved or dropped.
BENELUX_BOUNDING_BOX = {"latitude": (49.4, 53.6), "longitude": (2.5, 7.3)}

# Belgian regions by four-digit postcode. A postcode of any other shape (a Dutch
# "5657AH", a five-digit foreign code, a missing value) gets no region.
BELGIAN_POSTCODE_REGIONS = (
    (1000, 1299, "Brussels"),
    (1300, 1499, "Wallonia"),
    (1500, 3999, "Flanders"),
    (4000, 7999, "Wallonia"),
    (8000, 9999, "Flanders"),
)

# The source names the base price context of a retailer "Default".
NATIONAL_PRICE_LOCATION_NAME = "default"
NATIONAL_PRICE = "National price (online)"
STORE = "Store"


def outside_benelux(
    latitude: str = "geolocation_latitude", longitude: str = "geolocation_longitude"
) -> pl.Expr:
    """True when coordinates exist and lie outside the Benelux bounding box."""
    lat_low, lat_high = BENELUX_BOUNDING_BOX["latitude"]
    lon_low, lon_high = BENELUX_BOUNDING_BOX["longitude"]
    inside = pl.col(latitude).is_between(lat_low, lat_high) & pl.col(
        longitude
    ).is_between(lon_low, lon_high)
    return (~inside).fill_null(False)


def belgian_region(postcode: str = "postcode") -> pl.Expr:
    """Region of a four-digit Belgian postcode, NULL for any other value."""
    code = pl.col(postcode).str.extract(r"^(\d{4})$", 1).cast(pl.Int32)
    region = pl.lit(None, dtype=pl.String)
    for low, high, name in reversed(BELGIAN_POSTCODE_REGIONS):
        region = (
            pl.when(code.is_between(low, high)).then(pl.lit(name)).otherwise(region)
        )
    return region


def location_type(location_name: str = "location_name") -> pl.Expr:
    """A "Default" location is a base price context; every other one is a store."""
    is_base = (
        pl.col(location_name).str.strip_chars().str.to_lowercase()
        == NATIONAL_PRICE_LOCATION_NAME
    ).fill_null(False)
    return pl.when(is_base).then(pl.lit(NATIONAL_PRICE)).otherwise(pl.lit(STORE))
