"""Silver product datasets: products, weekly products, category paths, FMCG class.

Products keep their contextual identity ``(daltix_id, shop, country)``. Missing
descriptive attributes stay NULL; the products reference only fills gaps in the
weekly reference when shop, country and language agree.
"""

import hashlib
import json
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import duckdb
import polars as pl

from retail_pricing.fmcg_name_rules import classify_name
from retail_pricing.fmcg_taxonomy import (
    CATEGORY_LABELS,
    MAPPING_VERSION,
    REVIEW_EXCLUSIONS,
    classify_path,
)
from retail_pricing.quality import (
    fail_if_false,
    normalize_text,
    protect_inputs,
    require_columns,
    require_unique,
    write_dataset,
)
from retail_pricing.raw import quote_literal
from retail_pricing.reference import (
    add_shop_names,
    canonicalize_shop,
    require_canonical_shops,
)

# --- Products ------------------------------------------------------------------


PRODUCT_REQUIRED_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "name",
    "brand",
    "description",
    "categories",
]


PRODUCT_TEXT_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "name",
    "brand",
    "description",
    "categories",
]


def _validate_products(products: pl.DataFrame, products_raw: pl.DataFrame) -> None:
    """Stop before writing if the business key or a structural rule fails.

    Products must stay safe to use later as an enrichment source.
    """
    require_unique(products, ["daltix_id"], "Products")
    fail_if_false(
        {
            "row_count_preserved": products.height == products_raw.height,
            "daltix_id_complete": products["daltix_id"].null_count() == 0,
            "shop_complete": products["shop"].null_count() == 0,
            "country_complete": products["country"].null_count() == 0,
            "language_complete": products["language"].null_count() == 0,
            "daltix_id_unique": products["daltix_id"].n_unique() == products.height,
        },
        "Products",
    )


def run_products_pipeline(
    raw_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """
    Build the Silver Products dataset.

    Grain:
        One row per daltix_id.

    Business key:
        daltix_id.

    Returns:
        A small dictionary of pipeline metrics for orchestration/logging.
    """
    protect_inputs(raw_path, silver_path)
    products_raw = pl.read_parquet(raw_path)
    require_columns(products_raw, PRODUCT_REQUIRED_COLUMNS)

    # Semantic missing values become real NULLs; missing attributes stay NULL.
    products = canonicalize_shop(products_raw).with_columns(
        [normalize_text(column) for column in PRODUCT_TEXT_COLUMNS]
    )
    _validate_products(products, products_raw)

    products = add_shop_names(products)
    written_rows = write_dataset(
        products, silver_path, "Products", unique_key=["daltix_id"]
    )

    return {
        "rows": written_rows,
        "missing_name": products["name"].null_count(),
        "missing_brand": products["brand"].null_count(),
        "missing_description": products["description"].null_count(),
        "missing_categories": products["categories"].null_count(),
    }


# --- Weekly products -----------------------------------------------------------


WEEKLY_PRODUCT_REQUIRED_COLUMNS = [
    "daltix_id",
    "shop",
    "country",
    "language",
    "name",
    "brand",
    "description",
    "categories",
]

# Every source column is text.
WEEKLY_PRODUCT_TEXT_COLUMNS = WEEKLY_PRODUCT_REQUIRED_COLUMNS

# Attributes the products reference may fill when the weekly value is missing.
ENRICHMENT_ATTRIBUTES = ["name", "brand", "description", "categories"]

FALLBACK_CONTEXT = ["shop", "country", "language"]


def _match_fallback_products(
    weekly_clean: pl.DataFrame, fallback: pl.DataFrame
) -> pl.DataFrame:
    """Join the products reference by id and mark whether its context agrees.

    Fallback attributes get explicit ``fallback_`` names to preserve provenance.
    """
    fallback_prepared = fallback.select(
        ["daltix_id", *FALLBACK_CONTEXT, *ENRICHMENT_ATTRIBUTES]
    ).rename(
        {
            column: f"fallback_{column}"
            for column in [*FALLBACK_CONTEXT, *ENRICHMENT_ATTRIBUTES]
        }
    )

    # Join by daltix_id while enforcing one-to-one cardinality.
    return (
        weekly_clean.join(
            fallback_prepared,
            on="daltix_id",
            how="left",
            validate="1:1",
        )
        .with_columns(
            [
                pl.col("fallback_shop").is_not_null().alias("_has_fallback"),
                (
                    (pl.col("shop") == pl.col("fallback_shop"))
                    & (pl.col("country") == pl.col("fallback_country"))
                    & (pl.col("language") == pl.col("fallback_language"))
                )
                .fill_null(False)
                .alias("_context_safe"),
            ]
        )
        .with_columns(
            (pl.col("_has_fallback") & ~pl.col("_context_safe")).alias(
                "dq_enrichment_context_mismatch"
            )
        )
    )


def _enrich_from_fallback(joined: pl.DataFrame) -> pl.DataFrame:
    """Fill only missing attributes from context-compatible fallback records.

    Populated weekly values are never overwritten; a differing fallback value is
    recorded as a conflict instead.
    """
    enriched = joined
    for attribute in ENRICHMENT_ATTRIBUTES:
        fallback_column = f"fallback_{attribute}"
        enriched = enriched.with_columns(
            [
                (
                    pl.col(attribute).is_null()
                    & pl.col(fallback_column).is_not_null()
                    & pl.col("_context_safe")
                ).alias(f"is_{attribute}_enriched"),
                (
                    pl.col(attribute).is_not_null()
                    & pl.col(fallback_column).is_not_null()
                    & pl.col("_context_safe")
                    & (pl.col(attribute) != pl.col(fallback_column))
                ).alias(f"dq_{attribute}_conflict"),
            ]
        ).with_columns(
            pl.when(pl.col(f"is_{attribute}_enriched"))
            .then(pl.col(fallback_column))
            .otherwise(pl.col(attribute))
            .alias(attribute)
        )
    return enriched


def _validate_weekly_products(
    weekly_silver: pl.DataFrame, weekly_raw: pl.DataFrame
) -> None:
    """Enrichment may improve completeness but must never change the product grain."""
    fail_if_false(
        {
            "row_count_preserved": weekly_silver.height == weekly_raw.height,
            "daltix_id_complete": weekly_silver["daltix_id"].null_count() == 0,
            "shop_complete": weekly_silver["shop"].null_count() == 0,
            "country_complete": weekly_silver["country"].null_count() == 0,
            "language_complete": weekly_silver["language"].null_count() == 0,
            "daltix_id_unique": weekly_silver["daltix_id"].n_unique()
            == weekly_silver.height,
            "no_unsafe_enrichment": (
                weekly_silver.filter(
                    pl.col("dq_enrichment_context_mismatch")
                    & (
                        pl.col("is_name_enriched")
                        | pl.col("is_brand_enriched")
                        | pl.col("is_description_enriched")
                        | pl.col("is_categories_enriched")
                    )
                ).height
                == 0
            ),
        },
        "Weekly Products",
    )


def run_weekly_products_pipeline(
    raw_path: Path,
    fallback_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """Build the Silver Weekly Products dataset.

    Grain:
        One row per daltix_id.

    Primary source:
        weekly_prices_products.

    Fallback:
        Silver products.

    Existing weekly values always remain authoritative.
    """
    # Load the authoritative weekly source and validated fallback source.
    protect_inputs(raw_path, silver_path, fallback_path)
    weekly_raw = pl.read_parquet(raw_path)
    fallback = pl.read_parquet(fallback_path)
    require_canonical_shops(fallback["shop"].unique().to_list())
    require_columns(weekly_raw, WEEKLY_PRODUCT_REQUIRED_COLUMNS)

    # Normalize text and semantic NULL representations.
    weekly_clean = canonicalize_shop(weekly_raw).with_columns(
        [normalize_text(column) for column in WEEKLY_PRODUCT_TEXT_COLUMNS]
    )
    # The weekly product identifier must remain unique before enrichment.
    require_unique(weekly_clean, ["daltix_id"], "Weekly Products")

    joined = _match_fallback_products(weekly_clean, fallback)
    enriched = _enrich_from_fallback(joined)

    name_case_only = enriched.filter(
        pl.col("dq_name_conflict")
        & (
            pl.col("name").str.to_lowercase()
            == pl.col("fallback_name").str.to_lowercase()
        )
    ).height
    fallback_matches = int(joined["_has_fallback"].sum())
    safe_matches = int(joined["_context_safe"].sum())

    # Remove temporary fallback fields while retaining enrichment/DQ provenance.
    weekly_silver = enriched.drop(
        [
            *(
                f"fallback_{column}"
                for column in [*FALLBACK_CONTEXT, *ENRICHMENT_ATTRIBUTES]
            ),
            "_has_fallback",
            "_context_safe",
        ]
    )
    _validate_weekly_products(weekly_silver, weekly_raw)

    weekly_silver = add_shop_names(weekly_silver)
    rows = write_dataset(
        weekly_silver, silver_path, "Weekly Products", unique_key=["daltix_id"]
    )

    return {
        "rows": rows,
        "fallback_matches": fallback_matches,
        "safe_fallback_matches": safe_matches,
        "name_literal_differences": int(weekly_silver["dq_name_conflict"].sum()),
        "name_case_only_differences": name_case_only,
        "brand_literal_differences": int(weekly_silver["dq_brand_conflict"].sum()),
        "description_literal_differences": int(
            weekly_silver["dq_description_conflict"].sum()
        ),
        "categories_literal_differences": int(
            weekly_silver["dq_categories_conflict"].sum()
        ),
        "context_mismatches": int(
            weekly_silver["dq_enrichment_context_mismatch"].sum()
        ),
        "remaining_missing_name": weekly_silver["name"].null_count(),
        "names_enriched": int(weekly_silver["is_name_enriched"].sum()),
        "brands_enriched": int(weekly_silver["is_brand_enriched"].sum()),
        "descriptions_enriched": int(weekly_silver["is_description_enriched"].sum()),
        "categories_enriched": int(weekly_silver["is_categories_enriched"].sum()),
        "remaining_missing_brand": weekly_silver["brand"].null_count(),
        "remaining_missing_description": weekly_silver["description"].null_count(),
        "remaining_missing_categories": weekly_silver["categories"].null_count(),
    }


# --- Category paths ------------------------------------------------------------


# Exact source navigation signature observed in 3,183 weekly paths. Detection
# flags the whole path; it does not strip nodes or infer a business taxonomy.
FRC_NAVIGATION_PREFIX = (
    "Online boodschappen",
    "Online bestellen",
    "Data\nAlles",
    "Promos, online shopping",
    "Onze promoties",
    "Data\nAlles",
    "Carrefour Bonus Card en andere diensten",
    "Onze diensten",
    "Data\nAlles",
    "Recepten, Nieuws",
    "Beter eten",
    "Data\nAlles",
    "Online bestellen",
    "Data",
    "Onze promoties",
    "Data",
    "Onze diensten",
    "Data",
    "Beter eten",
    "Data",
    "Shop online",
)


NORMALIZATION_RULE = "category_structure_v1"


PATH_SCHEMA = {
    "source_dataset": pl.String,
    "daltix_id": pl.String,
    "shop": pl.String,
    "country": pl.String,
    "language": pl.String,
    "category_source_dataset": pl.String,
    "category_path_position": pl.UInt32,
    "category_path": pl.List(pl.String),
    "category_path_normalized": pl.List(pl.String),
    "category_path_depth": pl.UInt32,
    "dq_duplicate_category_path": pl.Boolean,
    "dq_repeated_category_label": pl.Boolean,
    "is_prefix_of_another_path": pl.Boolean,
    "product_maximal_path_count": pl.UInt32,
    "dq_navigation_contamination": pl.Boolean,
    "path_review_status": pl.String,
    "source_categories_sha256": pl.String,
    "normalization_rule": pl.String,
}


def parse_category_paths(value: str | None) -> list[list[str]]:
    """Accept nested nonempty string paths; never guess a malformed hierarchy."""
    if value is None:
        return []
    paths = json.loads(value)
    if not isinstance(paths, list) or not paths:
        raise ValueError("Categories must be a nonempty array of paths or NULL.")
    for path in paths:
        if not isinstance(path, list) or not path:
            raise ValueError("Each category path must be a nonempty array.")
        if any(not isinstance(label, str) or not label.strip() for label in path):
            raise ValueError("Category labels must be nonblank strings.")
    return paths


PATH_DATASETS = ("products", "weekly_prices_products")


def _is_prefix_of_another(labels: tuple, others) -> bool:
    """True when ``labels`` is a shorter path that opens a longer one."""
    return any(
        len(labels) < len(other) and other[: len(labels)] == labels for other in others
    )


def _is_carrefour_navigation(row: dict, labels: tuple) -> bool:
    """True for the site-navigation signature of the Belgian Dutch Carrefour paths."""
    # Canonical shop; the Raw source code is "frc".
    context = (row["shop"], row["country"], row["language"])
    return (
        context == ("crf", "be", "nl")
        and labels[: len(FRC_NAVIGATION_PREFIX)] == FRC_NAVIGATION_PREFIX
    )


def _path_review_status(
    *, navigation: bool, repeated: bool, duplicate: bool, prefix: bool, branches: int
) -> str:
    """Why a path still needs a semantic review before it can become a category."""
    if navigation:
        return "navigation_review_required"
    if repeated:
        return "repeated_label_review_required"
    if duplicate:
        return "duplicate_path_occurrence"
    if prefix:
        return "ancestor_path"
    if branches > 1:
        return "multiple_branches_review_required"
    return "unmapped"


def _product_path_records(dataset: str, row: dict, counts: Counter) -> list[dict]:
    """Every path occurrence of one product, with its structural flags.

    Also adds the product to the running ``counts`` of its dataset.
    """
    context = {k: row[k] for k in ("daltix_id", "shop", "country", "language")}
    if any(v is None for v in context.values()):
        raise ValueError("Category parent context must be complete.")
    paths = parse_category_paths(row["categories"])
    counts["products"] += 1
    counts["missing_categories"] += not paths
    counts["products_with_multiple_paths"] += len(paths) > 1
    counts["paths"] += len(paths)

    normalized = [
        tuple(unicodedata.normalize("NFC", label.strip()) for label in p) for p in paths
    ]
    occurrences = Counter(normalized)
    maximal = {
        labels
        for labels in occurrences
        if not _is_prefix_of_another(labels, occurrences)
    }
    counts["products_with_multiple_maximal_paths"] += len(maximal) > 1
    payload_hash = (
        hashlib.sha256(row["categories"].encode("utf-8")).hexdigest() if paths else None
    )

    records = []
    for position, (original, labels) in enumerate(zip(paths, normalized), 1):
        repeated = len(set(labels)) != len(labels)
        prefix = _is_prefix_of_another(labels, normalized)
        duplicate = occurrences[labels] > 1
        navigation = _is_carrefour_navigation(row, labels)
        counts["repeated_label_paths"] += repeated
        counts["duplicate_path_rows"] += duplicate
        counts["normalized_path_rows"] += list(labels) != original
        counts["navigation_path_rows"] += navigation
        counts["prefix_path_rows"] += prefix
        records.append(
            {
                "source_dataset": dataset,
                **context,
                "category_source_dataset": "products"
                if row.get("is_categories_enriched", False)
                else dataset,
                "category_path_position": position,
                "category_path": original,
                "category_path_normalized": list(labels),
                "category_path_depth": len(labels),
                "dq_duplicate_category_path": duplicate,
                "dq_repeated_category_label": repeated,
                "is_prefix_of_another_path": prefix,
                "product_maximal_path_count": len(maximal),
                "dq_navigation_contamination": navigation,
                # Every status still needs semantic review before Gold membership.
                "path_review_status": _path_review_status(
                    navigation=navigation,
                    repeated=repeated,
                    duplicate=duplicate,
                    prefix=prefix,
                    branches=len(maximal),
                ),
                "source_categories_sha256": payload_hash,
                "normalization_rule": NORMALIZATION_RULE,
            }
        )
    return records


def _dataset_path_records(dataset: str, path: Path) -> tuple[list[dict], dict]:
    """Path occurrences of one parent dataset and its ``<dataset>_<count>`` metrics."""
    frame = pl.read_parquet(path)
    require_canonical_shops(frame["shop"].unique().to_list())
    require_unique(frame, ["daltix_id"], dataset)
    counts = Counter()
    records = []
    for row in frame.iter_rows(named=True):
        records.extend(_product_path_records(dataset, row, counts))
    return records, {f"{dataset}_{key}": int(value) for key, value in counts.items()}


def _validate_category_paths(result: pl.DataFrame, metrics: dict) -> None:
    """Every path is accounted for, has a consistent depth and complete lineage."""
    require_unique(
        result,
        ["source_dataset", "daltix_id", "category_path_position"],
        "Category path occurrences",
    )
    lineage_columns = (
        "source_dataset",
        "daltix_id",
        "shop",
        "country",
        "language",
        "category_source_dataset",
        "source_categories_sha256",
        "normalization_rule",
        "path_review_status",
    )
    fail_if_false(
        {
            "all_paths_accounted_for": result.height
            == sum(metrics[f"{d}_paths"] for d in PATH_DATASETS),
            "path_depth_consistent": result.filter(
                pl.col("category_path_depth") != pl.col("category_path").list.len()
            ).height
            == 0,
            "lineage_complete": not any(
                result[c].null_count() for c in lineage_columns
            ),
        },
        "Category paths",
    )


def run_category_paths_pipeline(
    products_path: Path, weekly_products_path: Path, silver_path: Path
) -> dict[str, int]:
    """Emit every path occurrence, scoped by source and original product context.

    Position is a one-based JSON array ordinal, not a primary-category ranking.
    Raw parent JSON remains unchanged. Normalized labels use NFC and edge trim
    only; case, language, repeated labels and alternative paths are preserved.
    """
    if silver_path.resolve() in {
        products_path.resolve(),
        weekly_products_path.resolve(),
    }:
        raise ValueError("Category output must not overwrite its parent inputs.")
    records = []
    metrics = {}
    for dataset, path in zip(PATH_DATASETS, (products_path, weekly_products_path)):
        dataset_records, dataset_metrics = _dataset_path_records(dataset, path)
        records.extend(dataset_records)
        metrics.update(dataset_metrics)

    result = pl.DataFrame(records, schema=PATH_SCHEMA).sort(
        "source_dataset", "daltix_id", "category_path_position"
    )
    _validate_category_paths(result, metrics)

    silver_path.parent.mkdir(parents=True, exist_ok=True)
    result = add_shop_names(result)
    result.write_parquet(silver_path, compression="zstd")
    if not result.equals(pl.read_parquet(silver_path)):
        raise RuntimeError("Category paths changed during materialization.")
    return {"rows": result.height, **metrics}


# --- Primary FMCG category -----------------------------------------------------


IDENTITY = ["daltix_id", "shop", "country"]


REPORT_FIELDS = [
    "primary_fmcg_category_code",
    "primary_fmcg_category_en",
    "fmcg_classification_status",
    "fmcg_mapping_version",
]


STATUSES = {
    "classified",
    "conflicting_evidence",
    "missing_evidence",
    "no_supported_match",
    "excluded_evidence",
    "review_required",
}


SCHEMA = {
    **{field: pl.String for field in IDENTITY + REPORT_FIELDS},
    "candidate_category_codes": pl.List(pl.String),
    "matched_rule_ids": pl.List(pl.String),
    "evidence_source_datasets": pl.List(pl.String),
    "evidence_path_references": pl.List(pl.String),
    "source_path_count": pl.UInt32,
    "excluded_path_count": pl.UInt32,
}


def classify_product_paths(
    paths: list[dict], name: str | None = None, language: str | None = None
) -> dict:
    """Decide one category from the retailer paths, with the product name as backup.

    The paths decide first and must agree: no vote, no first path. The name is
    consulted only when the paths cannot decide, that is when they give no category
    (missing, unmapped or unusable paths) or several. Exactly one category named by
    the words of the name then decides; for conflicting paths it must be one of the
    conflicting candidates. A name never overrides a single agreeing path.
    """
    candidates, rules, sources, references = set(), set(), set(), set()
    excluded = 0
    for row in paths:
        # Match the existing Gold structural exclusions, without altering children.
        if any(
            row.get(flag, False)
            for flag in (
                "dq_duplicate_category_path",
                "is_prefix_of_another_path",
                "dq_navigation_contamination",
                "dq_repeated_category_label",
            )
        ):
            excluded += 1
            continue
        codes, matched_rules, reason = classify_path(
            row["category_path_normalized"], row["language"], row["shop"]
        )
        if reason == "excluded":
            excluded += 1
        candidates.update(codes)
        rules.update(matched_rules)
        if codes:
            sources.add(row["source_dataset"])
            # This points to an existing child-row key, not a new ranking.
            references.add(f"{row['source_dataset']}:{row['category_path_position']}")

    name_codes, name_rules = classify_name(name, language)
    if len(candidates) == 1:
        code, status = next(iter(candidates)), "classified"
    elif len(name_codes) == 1 and (not candidates or name_codes <= candidates):
        # No path evidence, or a name that settles a conflict between the paths.
        code, status = next(iter(name_codes)), "classified"
        rules |= name_rules | ({"name_breaks_path_conflict"} if candidates else set())
        sources.add("product_name")
        candidates = candidates or name_codes
    else:
        code = "unclassified"
        status = (
            "conflicting_evidence"
            if candidates
            else "missing_evidence"
            if not paths
            else "excluded_evidence"
            if excluded == len(paths)
            else "no_supported_match"
        )
    return {
        "primary_fmcg_category_code": code,
        "primary_fmcg_category_en": CATEGORY_LABELS[code],
        "fmcg_classification_status": status,
        "fmcg_mapping_version": MAPPING_VERSION,
        "candidate_category_codes": sorted(candidates),
        "matched_rule_ids": sorted(rules),
        "evidence_source_datasets": sorted(sources),
        "evidence_path_references": sorted(references),
        "source_path_count": len(paths),
        "excluded_path_count": excluded,
    }


def validate_primary_categories(frame: pl.DataFrame) -> None:
    """Validate identity, category domain, explicit abstention and evidence."""
    require_unique(frame, IDENTITY, "Primary FMCG category")
    for field in IDENTITY + REPORT_FIELDS:
        if frame[field].null_count():
            raise ValueError(f"Primary FMCG category has missing {field}.")
    expected_names = frame["primary_fmcg_category_code"].replace_strict(CATEGORY_LABELS)
    if not expected_names.equals(frame["primary_fmcg_category_en"]):
        raise ValueError("Primary FMCG code and English name disagree.")
    if set(frame["fmcg_classification_status"]) - STATUSES:
        raise ValueError("Unexpected FMCG classification status.")
    if frame.filter(pl.col("fmcg_mapping_version") != MAPPING_VERSION).height:
        raise ValueError("Stale FMCG mapping version.")
    classified = pl.col("fmcg_classification_status") == "classified"
    code = pl.col("primary_fmcg_category_code")
    candidates = pl.col("candidate_category_codes")
    rules = pl.col("matched_rule_ids")
    decided_by_name = pl.col("evidence_source_datasets").list.contains("product_name")
    invalid = frame.filter(
        (classified != (code != "unclassified"))
        | (
            classified
            & (
                (rules.list.len() == 0)
                | ~candidates.list.contains(code)
                # A decision rests on a category path, on the product name, or both.
                | (
                    (pl.col("evidence_path_references").list.len() == 0)
                    & ~decided_by_name
                )
                # Several candidates are acceptable only when the name settled them.
                | (
                    (candidates.list.len() > 1)
                    & ~rules.list.contains("name_breaks_path_conflict")
                )
            )
        )
        | (
            (pl.col("fmcg_classification_status") == "conflicting_evidence")
            & (candidates.list.len() < 2)
        )
    )
    if invalid.height:
        raise ValueError("Primary FMCG classification contradicts its evidence.")


def _product_names(
    con: duckdb.DuckDBPyConnection, products_path: Path, weekly_products_path: Path
) -> dict[tuple, tuple[str | None, str | None]]:
    """Name and language of each product; the weekly value wins, as in Gold."""
    rows = con.sql(
        f"""
        SELECT daltix_id, shop, country, name, language
        FROM (
            SELECT daltix_id, shop, country, name, language, 1 AS priority
            FROM read_parquet({quote_literal(weekly_products_path.as_posix())})
            UNION ALL
            SELECT daltix_id, shop, country, name, language, 2 AS priority
            FROM read_parquet({quote_literal(products_path.as_posix())})
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY daltix_id, shop, country ORDER BY priority
        ) = 1
        """
    ).fetchall()
    return {
        (daltix_id, shop, country): (name, language)
        for daltix_id, shop, country, name, language in rows
    }


def run_primary_categories_pipeline(
    products_path: Path,
    weekly_products_path: Path,
    nutritionals_path: Path,
    category_paths_path: Path,
    silver_path: Path,
) -> dict[str, int]:
    """Build one decision per known source identity, including absent path evidence."""
    inputs = [
        products_path,
        weekly_products_path,
        nutritionals_path,
        category_paths_path,
    ]
    if silver_path.resolve() in {p.resolve() for p in inputs}:
        raise ValueError("Classification must not overwrite its source inputs.")
    with duckdb.connect() as con:
        # Read only identity columns from the large nutritional dataset.
        union = " UNION ".join(
            "SELECT DISTINCT daltix_id, shop, country "
            f"FROM read_parquet({quote_literal(p.as_posix())})"
            for p in inputs[:3]
        )
        identities = con.sql(union + " ORDER BY shop, country, daltix_id").pl()
        names = _product_names(con, products_path, weekly_products_path)
    paths = pl.read_parquet(category_paths_path)
    groups = defaultdict(list)
    for row in paths.iter_rows(named=True):
        groups[tuple(row[field] for field in IDENTITY)].append(row)
    records = []
    for row in identities.iter_rows(named=True):
        key = tuple(row[f] for f in IDENTITY)
        evidence = groups.pop(key, [])
        decision = classify_product_paths(evidence, *names.get(key, (None, None)))
        exclusion = REVIEW_EXCLUSIONS.get(key)
        if exclusion:
            current_hashes = {path["source_categories_sha256"] for path in evidence}
            if current_hashes != set(exclusion["source_payload_hashes"]):
                raise ValueError(
                    "Reviewed category evidence changed; review the exception before rebuilding."
                )
            decision.update(
                primary_fmcg_category_code="unclassified",
                primary_fmcg_category_en=CATEGORY_LABELS["unclassified"],
                fmcg_classification_status="review_required",
            )
            decision["matched_rule_ids"].append("reviewed_source_contradiction")
        records.append({**row, **decision})
    if groups:
        raise ValueError("Category evidence has no contextual parent identity.")
    result = add_shop_names(pl.DataFrame(records, schema=SCHEMA))
    validate_primary_categories(result)
    if result.height != identities.height:
        raise ValueError("Classification changed the source identity population.")
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    result.write_parquet(silver_path, compression="zstd")
    if not result.equals(pl.read_parquet(silver_path)):
        raise RuntimeError("Classification changed during materialization.")
    return {
        "rows": result.height,
        **{
            status: result.filter(pl.col("fmcg_classification_status") == status).height
            for status in sorted(STATUSES)
        },
    }
