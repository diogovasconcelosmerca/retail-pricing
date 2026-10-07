"""Silver builders on small business examples, plus the publication protocol."""

import hashlib
import json
import os
from copy import deepcopy
from datetime import date

import polars as pl
import pytest

from retail_pricing import pipeline
from retail_pricing.fmcg_taxonomy import CATEGORY_LABELS
from retail_pricing.quality import (
    normalize_text,
    protect_inputs,
    require_columns,
    require_date,
    require_finite,
)
from retail_pricing.raw import sha256_file, validate_local_snapshot
from retail_pricing.reference import (
    add_shop_names,
    canonicalize_shop,
    get_nutrient_mapping,
    validate_nutrient_mapping,
)
from retail_pricing.silver_locations import (
    run_locations_pipeline,
    run_weekly_locations_pipeline,
)
from retail_pricing.silver_nutrition import run_nutritionals_pipeline
from retail_pricing.silver_prices import run_prices_pipeline, run_weekly_prices_pipeline
from retail_pricing.silver_products import (
    FRC_NAVIGATION_PREFIX,
    SCHEMA,
    classify_product_paths,
    parse_category_paths,
    run_category_paths_pipeline,
    run_weekly_products_pipeline,
    validate_primary_categories,
)


def test_placeholders_are_missing_only_in_their_own_column_and_only_as_whole_values():
    raw = pl.DataFrame(
        {
            "name": ["Not Provided", "Lego", "x"],
            "brand": ["no brand", "*", "Nivea"],
            "description": ["x", "*mag in vaatwasser", " * "],
        }
    )
    clean = raw.with_columns(
        [normalize_text(column) for column in ("name", "brand", "description")]
    )
    assert clean["name"].to_list() == [None, "Lego", "x"]
    assert clean["brand"].to_list() == [None, None, "Nivea"]
    assert clean["description"].to_list() == [None, "*mag in vaatwasser", None]


def test_missing_tokens_preserve_real_brand_and_codes():
    raw = pl.DataFrame({"brand": [" NAN ", "na", "#N/A", "  ", None, "N/A", "Brand"]})
    assert raw.select(normalize_text("brand"))["brand"].to_list() == [
        "NAN",
        "na",
        None,
        None,
        None,
        None,
        "Brand",
    ]
    assert raw.select(normalize_text("brand", null_tokens=("",)))["brand"][2] == "#N/A"


def test_contracts_reject_nonfinite_schema_drift_and_date_truncation(tmp_path):
    for value in [float("nan"), float("inf"), -float("inf")]:
        with pytest.raises(ValueError, match="finite"):
            require_finite(pl.DataFrame({"price": [value]}), ["price"])
    require_finite(pl.DataFrame({"price": [None, 0.009]}), ["price"])
    with pytest.raises(ValueError, match="unexpected"):
        require_columns(pl.DataFrame({"id": [1], "new": [2]}), ["id"])
    with pytest.raises(ValueError, match="Date"):
        require_date(
            pl.DataFrame({"day": [date(2020, 1, 1)]}).with_columns(
                pl.col("day").cast(pl.Datetime)
            ),
            "day",
        )
    with pytest.raises(ValueError, match="protected"):
        protect_inputs(tmp_path / "raw/a.parquet", tmp_path / "raw/b.parquet")


def test_weekly_price_decoding_preserves_grains_duplicates_metrics_and_raw(tmp_path):
    # Two distinct observations and one true duplicate remain the same business case.
    raw = tmp_path / "raw/source.parquet"
    raw.parent.mkdir()
    output = tmp_path / "encoded-result.parquet"
    source = pl.DataFrame(
        {
            "daltix_id": ["p"] * 3,
            "shop": ["idla"] * 3,
            "location": ["store"] * 3,
            "week": [date(2020, 1, 6)] * 3,
            "price": [2.0, 2.0, 3.0],
            "price_promo": [1.0, 1.0, 3.0],
        }
    )
    source.write_parquet(raw)
    raw_bytes = raw.read_bytes()
    metrics = run_weekly_prices_pipeline(raw, output)
    clean = pl.read_parquet(output).sort("price")
    assert raw.read_bytes() == raw_bytes
    assert clean["shop"].to_list() == ["aldi", "aldi"]
    assert metrics["rows"] == 2 and metrics["exact_duplicates_removed"] == 1
    assert (
        metrics["multiple_price_version_grains"] == 1
        and clean["has_multiple_price_versions"].all()
    )
    canonical_input = tmp_path / "raw/already-canonical.parquet"
    canonicalize_shop(source).write_parquet(canonical_input)
    repeated_output = tmp_path / "canonical-result.parquet"
    assert run_weekly_prices_pipeline(canonical_input, repeated_output) == metrics
    assert pl.read_parquet(repeated_output).sort("price").equals(clean)


def write_raw(tmp_path, frame, name="source"):
    path = tmp_path / "raw" / f"{name}.parquet"
    path.parent.mkdir(exist_ok=True)
    frame.write_parquet(path)
    return path


def test_weekly_prices_preserves_alternatives_and_collapses_only_exact_rows(tmp_path):
    # An apostrophe in the path also exercises safe SQL literal handling.
    root = tmp_path / "reviewer's case"
    root.mkdir()
    data = pl.DataFrame(
        {
            "daltix_id": ["p"] * 3,
            "shop": ["aldi"] * 3,
            "location": ["l"] * 3,
            "week": [date(2020, 1, 6)] * 3,
            "price": [1.0, 1.0, 2.0],
            "price_promo": [1.0, 1.0, 1.5],
        }
    )
    raw = write_raw(root, data)
    out = root / "clean/silver_weekly_prices.parquet"
    metrics = run_weekly_prices_pipeline(raw, out)
    assert (
        metrics["rows"],
        metrics["exact_duplicates_removed"],
        metrics["multiple_price_version_grains"],
    ) == (2, 1, 1)
    result = pl.read_parquet(out)
    assert result["has_multiple_price_versions"].all()
    assert (result["price_version_count"] == 2).all()
    assert metrics["multiple_price_version_grains"] == 1
    assert result["price_observation_id"].n_unique() == 2
    for invalid in [
        pl.lit(" ").alias("daltix_id"),
        pl.lit(float("nan")).alias("price"),
    ]:
        bad = write_raw(root, data.with_columns(invalid), "bad")
        with pytest.raises(RuntimeError, match="contract failed"):
            run_weekly_prices_pipeline(bad, root / "clean/bad.parquet")


def test_invalid_promo_does_not_become_no_promotion(tmp_path):
    frame = pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["aldi"],
            "country": ["be"],
            "location": ["l"],
            "downloaded_on": [date(2020, 1, 1)],
            "price": [1.0],
            "promo_price": ["invalid"],
            "unit_std": ["su"],
            "currency": ["eur"],
        }
    )
    with pytest.raises(pl.exceptions.InvalidOperationError):
        run_prices_pipeline(
            write_raw(tmp_path, frame), tmp_path / "clean/silver_prices.parquet"
        )
    raw = write_raw(
        tmp_path,
        frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("promo_price")),
    )
    out = tmp_path / "clean/silver_prices.parquet"
    run_prices_pipeline(raw, out)
    result = pl.read_parquet(out)
    assert result["promo_price"][0] is None and result["is_promotion"][0] is False


def test_product_enrichment_primary_wins_and_context_blocks_fallback(tmp_path):
    primary = pl.DataFrame(
        {
            "daltix_id": ["fill", "keep", "mismatch"],
            "shop": ["aldi"] * 3,
            "country": ["be"] * 3,
            "language": ["nl"] * 3,
            "name": ["A", "B", "C"],
            "brand": [None, "primary", None],
            "description": [None] * 3,
            "categories": [None] * 3,
        }
    )
    fallback = primary.with_columns(
        pl.lit("fallback").alias("brand"),
        pl.when(pl.col("daltix_id") == "mismatch")
        .then(pl.lit("fr"))
        .otherwise(pl.col("country"))
        .alias("country"),
    )
    raw = write_raw(tmp_path, primary)
    fall = write_raw(tmp_path, fallback, "fallback")
    out = tmp_path / "clean/silver_weekly_products.parquet"
    run_weekly_products_pipeline(raw, fall, out)
    result = pl.read_parquet(out).sort("daltix_id")
    assert dict(result.select("daltix_id", "brand").iter_rows()) == {
        "fill": "fallback",
        "keep": "primary",
        "mismatch": None,
    }
    assert result.filter(pl.col("daltix_id") == "mismatch")[
        "dq_enrichment_context_mismatch"
    ][0]
    duplicate = pl.concat([fallback, fallback.head(1)])
    with pytest.raises(pl.exceptions.ComputeError):
        run_weekly_products_pipeline(
            raw, write_raw(tmp_path, duplicate, "duplicate"), out
        )


def test_location_context_conflict_blocks_filling_and_nullable_name_is_allowed(
    tmp_path,
):
    primary = pl.DataFrame(
        {
            "shop": ["aldi", "aldi", "aldi"],
            "location": ["conflict", "safe", "ambiguous"],
            "location_name": [None] * 3,
            "shop_type": [None] * 3,
            "geolocation_latitude": [50.0, None, None],
            "geolocation_longitude": [4.0, None, None],
            "locality": [None] * 3,
            "postcode": [None] * 3,
            "state": [None] * 3,
        }
    )
    fallback = pl.DataFrame(
        {
            "shop": ["aldi"] * 4,
            "id": ["conflict", "safe", "ambiguous", "ambiguous"],
            "country_code": ["se", "be", "be", "be"],
            "postcode": ["9999", "1000", "1", "2"],
            "geolocation_latitude": [59.0, 50.0, 1.0, 2.0],
            "geolocation_longitude": [14.0, 4.0, 1.0, 2.0],
            "dq_business_key_collision": [False, False, True, True],
        }
    )
    out = tmp_path / "clean/silver_weekly_locations.parquet"
    run_weekly_locations_pipeline(
        write_raw(tmp_path, primary), write_raw(tmp_path, fallback, "fallback"), out
    )
    rows = {r["location"]: r for r in pl.read_parquet(out).to_dicts()}
    assert (
        rows["conflict"]["postcode"] is None
        and rows["conflict"]["country_code"] is None
    )
    assert rows["conflict"]["dq_enrichment_context_mismatch"]
    assert (
        rows["safe"]["postcode"] == "1000" and rows["safe"]["is_coordinates_enriched"]
    )
    assert rows["ambiguous"]["postcode"] is None


def test_nutrition_selection_preserves_portion_and_exposes_hash_ties(tmp_path):
    def payload(nutrients, unit="g"):
        return json.dumps(
            {"nutrients": nutrients, "portion": {"value": 100, "unit": unit}}
        )

    fat = lambda v: {"fats": {"value": v, "unit": "g"}}
    richer = {**fat(2), "salt": {"value": 1, "unit": "g"}}
    data = pl.DataFrame(
        {
            "daltix_id": ["tie", "tie", "rich", "rich", "odd"],
            "shop": ["aldi"] * 5,
            "country": ["be"] * 5,
            "language": ["nl"] * 5,
            "download_date": [date(2020, 1, 1)] * 5,
            "nutritional_values_std": [
                payload(fat(1), "ml"),
                payload(fat(2), "ml"),
                payload(fat(1)),
                payload(richer),
                payload({"energy": {"value": 1, "unit": "g"}}),
            ],
        }
    )
    out = tmp_path / "clean/silver_nutritionals.parquet"
    metrics = run_nutritionals_pipeline(write_raw(tmp_path, data), out)
    result = pl.read_parquet(out).sort("daltix_id", "nutrient_name")
    assert (
        metrics["hash_tiebreak_grains"] == 1 and metrics["unconvertible_unit_rows"] == 1
    )
    assert result.filter(pl.col("daltix_id") == "tie")["portion_unit"][0] == "ml"
    assert result.filter(pl.col("daltix_id") == "rich").height == 2
    run_nutritionals_pipeline(write_raw(tmp_path, data.reverse(), "reversed"), out)
    assert result.equals(pl.read_parquet(out).sort("daltix_id", "nutrient_name"))
    for value in [
        "not JSON",
        '{"nutrients": {}}',
        '{"nutrients": null}',
        '{"nutrients": []}',
    ]:
        bad = data.head(1).with_columns(pl.lit(value).alias("nutritional_values_std"))
        with pytest.raises(RuntimeError, match="contract failed"):
            run_nutritionals_pipeline(write_raw(tmp_path, bad, "invalid"), out)


def inputs(tmp_path, values):
    raw = tmp_path / "raw/weekly_prices_locations.parquet"
    raw.parent.mkdir()
    frame = pl.DataFrame(
        {
            "shop": ["idla"] * len(values),
            "location": [str(i) for i in range(len(values))],
            "location_name": ["klem"] * len(values),
            "shop_type": values,
            "geolocation_latitude": [None] * len(values),
            "geolocation_longitude": [None] * len(values),
            "locality": ["Bruxelles"] * len(values),
            "postcode": ["1000"] * len(values),
            "state": ["Vlaanderen"] * len(values),
        }
    )
    frame.write_parquet(raw)
    fallback = tmp_path / "fallback.parquet"
    pl.DataFrame(
        schema={
            "shop": pl.String,
            "id": pl.String,
            "country_code": pl.String,
            "postcode": pl.String,
            "geolocation_latitude": pl.Float64,
            "geolocation_longitude": pl.Float64,
            "dq_business_key_collision": pl.Boolean,
        }
    ).write_parquet(fallback)
    return raw, fallback, tmp_path / "clean/silver_weekly_locations.parquet"


def test_all_reviewed_labels_preserve_lineage_and_do_not_reverse_other_fields(tmp_path):
    observed = [
        "da",
        "hgadtsem",
        "og & pohs",
        "reileta hserf",
        "sserpxe",
        "tekram",
        "tekramrepus",
        "tkramrepyh",
        "yxorp",
    ]
    expected = [
        "ad",
        "mestdagh",
        "shop & go",
        "fresh atelier",
        "express",
        "market",
        "supermarket",
        "hypermarkt",
        "proxy",
    ]
    raw, fallback, out = inputs(tmp_path, observed)
    before = raw.read_bytes()
    metrics = run_weekly_locations_pipeline(raw, fallback, out)
    result = pl.read_parquet(out).sort("location")
    assert result["shop_type"].to_list() == expected
    assert result["shop_type_raw"].to_list() == observed
    assert result["is_shop_type_reversed"].all()
    assert metrics["shop_types_reversed"] == 9 and metrics["missing_shop_type"] == 0
    assert result["shop"].unique().to_list() == ["aldi"]
    assert result["location_name"].unique().to_list() == ["klem"]
    assert result["locality"].unique().to_list() == ["Bruxelles"]
    assert raw.read_bytes() == before
    assert run_weekly_locations_pipeline(raw, fallback, out) == metrics
    assert result.equals(pl.read_parquet(out).sort("location"))


def test_readable_labels_nulls_case_and_whitespace_are_safe(tmp_path):
    values = [
        "market",
        "ad",
        "proxy",
        "  TEKRAM  ",
        "Supermarket",
        None,
        "",
        "  ",
        "NULL",
        "None",
        "n/a",
        "#N/A",
    ]
    raw, fallback, out = inputs(tmp_path, values)
    metrics = run_weekly_locations_pipeline(raw, fallback, out)
    rows = {int(r["location"]): r for r in pl.read_parquet(out).to_dicts()}
    expected = ["market", "ad", "proxy", "market", "supermarket"] + [None] * 7
    assert [rows[i]["shop_type"] for i in range(len(values))] == expected
    assert [rows[i]["shop_type_raw"] for i in range(len(values))] == values
    assert [rows[i]["is_shop_type_reversed"] for i in range(len(values))] == [
        False,
        False,
        False,
        True,
        False,
    ] + [False] * 7
    assert (
        metrics["shop_types_reversed"] == 1
        and metrics["shop_types_already_readable"] == 4
    )
    assert metrics["missing_shop_type"] == 7


@pytest.mark.parametrize("value", ["unknown", "tekram-unknown", "NAN"])
def test_unreviewed_labels_fail_before_overwriting_a_completed_file(tmp_path, value):
    raw, fallback, out = inputs(tmp_path, [value])
    out.parent.mkdir()
    out.write_bytes(b"previous complete output")
    with pytest.raises(ValueError, match="unknown shop_type values require review"):
        run_weekly_locations_pipeline(raw, fallback, out)
    assert out.read_bytes() == b"previous complete output"


def build(tmp_path, payloads):
    raw = tmp_path / "raw/source.parquet"
    raw.parent.mkdir()
    source = pl.DataFrame(
        {
            "daltix_id": [str(i) for i in range(len(payloads))],
            "shop": ["aldi"] * len(payloads),
            "country": ["be"] * len(payloads),
            "language": ["nl"] * len(payloads),
            "download_date": [date(2020, 1, 1)] * len(payloads),
            "nutritional_values_std": payloads,
        }
    )
    source.write_parquet(raw)
    out = tmp_path / "silver.parquet"
    return raw, out


def payload(nutrients, portion_unit="g"):
    return json.dumps(
        {"nutrients": nutrients, "portion": {"value": 100, "unit": portion_unit}}
    )


def test_all_observed_pairs_keep_lineage_and_only_incompatible_values_become_null(
    tmp_path,
):
    mapping = get_nutrient_mapping()
    payloads = [
        payload({r["raw_nutrient_name"]: {"value": 12.5, "unit": r["raw_unit"]}})
        for r in mapping.to_dicts()
    ]
    raw, out = build(tmp_path, payloads)
    raw_before = raw.read_bytes()
    metrics = run_nutritionals_pipeline(raw, out)
    result = pl.read_parquet(out)
    assert result.height == 20 and metrics["missing_numeric_values"] == 6
    assert metrics["non_numeric_values"] == metrics["converted_unit_rows"] == 0
    assert metrics["canonical_unit_mismatch_rows"] == 0
    assert metrics["identity_unit_rows"] == 14
    for row in result.to_dicts():
        index = int(row["daltix_id"])
        reference = mapping.row(index, named=True)
        assert (
            row["source_payload_hash"]
            == hashlib.md5(payloads[index].encode()).hexdigest()
        )
        assert row["nutrient_name_raw"] == reference["raw_nutrient_name"]
        assert row["nutrient_unit_raw"] == reference["raw_unit"]
        assert row["nutrient_value_raw"] == "12.5"
        assert row["nutrient_name"] == reference["canonical_nutrient_name"]
        assert row["nutrient_unit"] == reference["canonical_unit"]
        incompatible = reference["mapping_status"] == "incompatible_unit"
        assert row["dq_incompatible_nutrient_unit"] == incompatible
        assert row["nutrient_value"] == (None if incompatible else 12.5)
        assert row["unit_conversion_factor"] == (None if incompatible else 1.0)
    assert raw.read_bytes() == raw_before


def test_unknown_pair_in_discarded_version_fails_before_overwriting(tmp_path):
    raw, out = build(
        tmp_path,
        [
            payload({"proteins": {"value": 1, "unit": "oz"}}),
            payload(
                {
                    "proteins": {"value": 1, "unit": "g"},
                    "fats": {"value": 2, "unit": "g"},
                }
            ),
        ],
    )
    frame = pl.read_parquet(raw).with_columns(pl.lit("p").alias("daltix_id"))
    frame.write_parquet(raw)
    out.write_bytes(b"last complete output")
    with pytest.raises(RuntimeError, match="Unmapped.*proteins.*oz"):
        run_nutritionals_pipeline(raw, out)
    assert out.read_bytes() == b"last complete output"


@pytest.mark.parametrize("value", [None, "invalid", "NaN", "Infinity"])
def test_invalid_source_values_cannot_be_hidden_as_unit_exceptions(tmp_path, value):
    raw, out = build(tmp_path, [payload({"energy": {"value": value, "unit": "g"}})])
    with pytest.raises(RuntimeError, match="source_values_numeric_and_finite"):
        run_nutritionals_pipeline(raw, out)
    assert not out.exists()


def test_mass_plausibility_preserves_values_and_does_not_assume_liquid_density(
    tmp_path,
):
    raw, out = build(
        tmp_path,
        [
            payload({"fats": {"value": 120, "unit": "g"}}),
            payload({"fats": {"value": 120, "unit": "g"}}, "ml"),
        ],
    )
    metrics = run_nutritionals_pipeline(raw, out)
    result = pl.read_parquet(out).sort("daltix_id")
    assert result["nutrient_value"].to_list() == [120.0, 120.0]
    assert result["dq_mass_exceeds_portion"].to_list() == [True, False]
    assert metrics["mass_exceeds_portion_rows"] == 1


def test_future_alias_mapping_cannot_collapse_two_nutrients_to_one_grain(
    tmp_path, monkeypatch
):
    from retail_pricing import silver_nutrition as nutritionals

    mapping = get_nutrient_mapping().with_columns(
        pl.when(pl.col("canonical_nutrient_name") == "proteins")
        .then(pl.lit("fats"))
        .otherwise(pl.col("canonical_nutrient_name"))
        .alias("canonical_nutrient_name")
    )
    validate_nutrient_mapping(mapping)
    monkeypatch.setattr(nutritionals, "get_nutrient_mapping", lambda: mapping)
    raw, out = build(
        tmp_path,
        [
            payload(
                {
                    "fats": {"value": 1, "unit": "g"},
                    "proteins": {"value": 2, "unit": "g"},
                }
            )
        ],
    )
    with pytest.raises(RuntimeError, match="normalized_grain_unique"):
        run_nutritionals_pipeline(raw, out)


@pytest.mark.parametrize(
    "kj,kcal,unit,comparable,mismatch",
    [
        (418.4, 100, "kJ", True, False),
        (426.768, 100, "kJ", True, False),
        (427, 100, "kJ", True, True),
        (0, 0, "kJ", True, False),
        (1, 0, "kJ", True, False),
        (1.01, 0, "kJ", True, True),
        (-1, 100, "kJ", False, False),
        (418.4, 100, "g", False, False),
    ],
)
def test_energy_check_uses_same_payload_units_and_documented_tolerance(
    tmp_path, kj, kcal, unit, comparable, mismatch
):
    raw, out = build(
        tmp_path,
        [
            payload(
                {
                    "energy": {"value": kj, "unit": unit},
                    "kilocalories": {"value": kcal, "unit": "kcal"},
                    "fats": {"value": -2, "unit": "g"},
                },
                "ml",
            )
        ],
    )
    before = raw.read_bytes()
    metrics = run_nutritionals_pipeline(raw, out)
    frame = pl.read_parquet(out)
    assert frame.height == 3 and raw.read_bytes() == before
    assert (
        frame["energy_pair_difference_kj"].is_not_null().to_list() == [comparable] * 3
    )
    assert frame["dq_energy_kcal_mismatch"].to_list() == [mismatch] * 3
    assert metrics["energy_mismatch_observations"] == int(mismatch)
    fat = frame.filter(pl.col("nutrient_name") == "fats").row(0, named=True)
    assert fat["nutrient_value"] == -2 and fat["dq_negative_nutrient_value"]
    assert frame["portion_unit"].to_list() == ["ml"] * 3


def test_energy_values_in_different_observations_are_not_paired(tmp_path):
    raw, out = build(
        tmp_path,
        [
            payload({"energy": {"value": 400, "unit": "kJ"}}),
            payload({"kilocalories": {"value": 100, "unit": "kcal"}}),
        ],
    )
    result = run_nutritionals_pipeline(raw, out)
    assert result["comparable_energy_observations"] == 0
    frame = pl.read_parquet(out)
    assert frame["energy_pair_difference_kj"].null_count() == 2


def parent(ids, values):
    return pl.DataFrame(
        {
            "daltix_id": ids,
            "shop": ["aldi"] * len(ids),
            "country": ["be"] * len(ids),
            "language": ["nl"] * len(ids),
            "categories": values,
        }
    )


def test_paths_preserve_multiplicity_context_source_order_and_unicode(tmp_path):
    p, w, out = (
        tmp_path / name for name in ("p.parquet", "w.parquet", "paths.parquet")
    )
    value = json.dumps(
        [[" Food ", "Cafe\u0301"], ["Food"], [" Food ", "Cafe\u0301"], ["A", "A"]]
    )
    parent(["p", "missing"], [value, None]).write_parquet(p)
    parent(["p"], [json.dumps([["Other"]])]).with_columns(
        pl.lit(True).alias("is_categories_enriched")
    ).write_parquet(w)
    before = (p.read_bytes(), w.read_bytes())
    metrics = run_category_paths_pipeline(p, w, out)
    frame = pl.read_parquet(out)
    primary = frame.filter(pl.col("source_dataset") == "products")
    assert primary["category_path"].to_list() == json.loads(value)
    assert primary["category_path_normalized"][0].to_list() == ["Food", "Caf\u00e9"]
    assert primary["category_path_position"].to_list() == [1, 2, 3, 4]
    assert primary["dq_duplicate_category_path"].to_list() == [True, False, True, False]
    assert primary["is_prefix_of_another_path"].to_list() == [False, True, False, False]
    assert primary["dq_repeated_category_label"].to_list() == [
        False,
        False,
        False,
        True,
    ]
    assert (
        frame.filter(pl.col("source_dataset") == "weekly_prices_products")[
            "category_source_dataset"
        ][0]
        == "products"
    )
    assert metrics["rows"] == 5 and metrics["products_missing_categories"] == 1
    assert (p.read_bytes(), w.read_bytes()) == before
    assert run_category_paths_pipeline(p, w, out) == metrics
    assert frame.equals(pl.read_parquet(out))


@pytest.mark.parametrize(
    "value", ["{}", "[]", "[[]]", '["A"]', "[[null]]", '[[" "]]', "not JSON"]
)
def test_invalid_categories_fail_without_guessing(value):
    with pytest.raises(ValueError):
        parse_category_paths(value)


def test_all_null_categories_have_typed_empty_output(tmp_path):
    p, w, out = (
        tmp_path / name for name in ("p.parquet", "w.parquet", "paths.parquet")
    )
    parent(["p"], [None]).write_parquet(p)
    parent(["p"], [None]).write_parquet(w)
    assert run_category_paths_pipeline(p, w, out)["rows"] == 0
    assert pl.read_parquet(out).schema["category_path"] == pl.List(pl.String)


def test_navigation_is_scoped_and_never_silently_removed(tmp_path):
    p, w, out = (
        tmp_path / name for name in ("p.parquet", "w.parquet", "paths.parquet")
    )
    value = json.dumps([list(FRC_NAVIGATION_PREFIX) + ["Food", "Coffee"]])
    parent(["p"], [value]).with_columns(pl.lit("crf").alias("shop")).write_parquet(p)
    parent(["p"], [value]).write_parquet(w)
    run_category_paths_pipeline(p, w, out)
    frame = pl.read_parquet(out)
    assert frame["dq_navigation_contamination"].to_list() == [True, False]
    assert frame["category_path_normalized"].to_list() == json.loads(value) * 2
    assert frame["path_review_status"].to_list() == [
        "navigation_review_required",
        "repeated_label_review_required",
    ]
    assert all(len(v) == 64 for v in frame["source_categories_sha256"])


def test_malformed_parent_fails_before_overwriting_previous_child(tmp_path):
    p, w, out = (
        tmp_path / name for name in ("p.parquet", "w.parquet", "paths.parquet")
    )
    parent(["p"], ['[["Food"]]']).write_parquet(p)
    parent(["p"], ['["not a path"]']).write_parquet(w)
    out.write_bytes(b"previous complete output")
    with pytest.raises(ValueError, match="nonempty array"):
        run_category_paths_pipeline(p, w, out)
    assert out.read_bytes() == b"previous complete output"


def evidence(path, position=1, **changes):
    return {
        "category_path_normalized": path,
        "language": "nl",
        "shop": "ah",
        "source_dataset": "weekly_prices_products",
        "category_path_position": position,
        **changes,
    }


def test_multiple_paths_require_agreement_and_never_use_order_or_votes():
    paths = [
        evidence(["Babyvoeding", "6 maanden"]),
        evidence(["Babyvoeding", "12 maanden"], 2),
    ]
    before = deepcopy(paths)
    result = classify_product_paths(paths)
    assert result["primary_fmcg_category_code"] == "baby_care"
    assert result == classify_product_paths(list(reversed(paths)))
    paths.append(evidence(["Dierenvoeding"], 3))
    conflict = classify_product_paths(paths)
    assert conflict["primary_fmcg_category_code"] == "unclassified"
    assert conflict["fmcg_classification_status"] == "conflicting_evidence"
    assert paths[:2] == before
    # A promotional-only path is not a competing merchandise category.
    paths = [evidence(["Wijn"]), evidence(["2+1 op Snoep"], 2)]
    assert (
        classify_product_paths(paths)["primary_fmcg_category_code"]
        == "alcoholic_beverages"
    )


def test_labels_added_since_v2_classify_and_broad_departments_still_abstain():
    honey = classify_product_paths([evidence(["Voeding", "Honing"])])
    assert honey["primary_fmcg_category_code"] == "pantry_cooking"
    assert honey["fmcg_classification_status"] == "classified"
    assert honey["fmcg_mapping_version"] == "fmcg_v3"
    sweets = classify_product_paths([evidence(["Snoep en kauwgom"])])
    assert sweets["primary_fmcg_category_code"] == "confectionery_snacks"
    # A department that mixes categories must not decide on its own.
    department = classify_product_paths([evidence(["Kruidenierswaren"])])
    assert department["fmcg_classification_status"] != "classified"


def test_v3_path_rules_settle_eggs_plant_drinks_and_toothpaste():
    eggs = classify_product_paths(
        [evidence(["Assortiment", "Bakken en koken", "Verse eieren"])]
    )
    assert eggs["primary_fmcg_category_code"] == "dairy_eggs"
    plant = classify_product_paths(
        [evidence(["Melk/Melkdrank", "Plantaardig alternatief"])]
    )
    assert plant["primary_fmcg_category_code"] == "plant_based_alternatives"
    toothpaste = classify_product_paths([evidence(["Mondhygiëne", "Tandpasta"])])
    assert toothpaste["primary_fmcg_category_code"] == "personal_care"


def test_the_name_decides_only_when_the_paths_cannot():
    # No path at all: one category named by the words of the name decides.
    by_name = classify_product_paths([], "Colgate tandpasta white 100ml", "nl")
    assert by_name["primary_fmcg_category_code"] == "personal_care"
    assert by_name["fmcg_classification_status"] == "classified"
    assert by_name["evidence_source_datasets"] == ["product_name"]
    assert by_name["evidence_path_references"] == []
    assert by_name["matched_rule_ids"] == ["name:tandpasta"]

    # Paths that agree are never overridden by a different name.
    agreeing = classify_product_paths([evidence(["Wijn"])], "Colgate tandpasta", "nl")
    assert agreeing["primary_fmcg_category_code"] == "alcoholic_beverages"
    assert "product_name" not in agreeing["evidence_source_datasets"]

    # Conflicting paths: the name breaks the tie only in favour of a candidate.
    conflict = [evidence(["Wijn"]), evidence(["Dierenvoeding"], 2)]
    assert classify_product_paths(conflict)["fmcg_classification_status"] == (
        "conflicting_evidence"
    )
    broken = classify_product_paths(conflict, "Pedigree brokken", "nl")
    assert broken["primary_fmcg_category_code"] == "pet_care"
    assert "name_breaks_path_conflict" in broken["matched_rule_ids"]
    assert broken["candidate_category_codes"] == ["alcoholic_beverages", "pet_care"]
    unrelated = classify_product_paths(conflict, "Colgate tandpasta", "nl")
    assert unrelated["fmcg_classification_status"] == "conflicting_evidence"

    # Two categories among the words, another language or no name: no evidence.
    mixed = classify_product_paths([], "tandpasta voor honden pedigree", "nl")
    assert mixed["fmcg_classification_status"] == "missing_evidence"
    assert classify_product_paths([], "Colgate tandpasta", "fr")[
        "fmcg_classification_status"
    ] == ("missing_evidence")
    assert classify_product_paths([], None, "nl")["fmcg_classification_status"] == (
        "missing_evidence"
    )


def test_name_only_decisions_pass_validation_and_broken_ones_do_not():
    def frame(paths, name):
        decision = classify_product_paths(paths, name, "nl")
        return add_shop_names(
            pl.DataFrame(
                [{"daltix_id": "p", "shop": "ah", "country": "be", **decision}],
                schema=SCHEMA,
            )
        )

    validate_primary_categories(frame([], "Lego duplo"))
    validate_primary_categories(
        frame([evidence(["Wijn"]), evidence(["Dierenvoeding"], 2)], "Pedigree")
    )
    broken = frame([], "Lego duplo").with_columns(
        evidence_source_datasets=pl.lit([], dtype=pl.List(pl.String))
    )
    with pytest.raises(ValueError, match="contradicts its evidence"):
        validate_primary_categories(broken)
    no_tiebreak = frame(
        [evidence(["Wijn"]), evidence(["Dierenvoeding"], 2)], "Pedigree"
    ).with_columns(matched_rule_ids=pl.lit(["nl_label_x"]))
    with pytest.raises(ValueError, match="contradicts its evidence"):
        validate_primary_categories(no_tiebreak)


def test_the_committed_name_vocabulary_is_consistent():
    from retail_pricing.fmcg_name_rules import (
        NAME_KEYWORDS_NL,
        NAME_TOKEN,
        STOP_TOKENS,
        TOKEN_CODE,
    )

    assert set(NAME_KEYWORDS_NL) <= set(CATEGORY_LABELS)
    assert all(NAME_TOKEN.fullmatch(token) for token in TOKEN_CODE)
    assert not set(TOKEN_CODE) & STOP_TOKENS
    assert sum(map(len, NAME_KEYWORDS_NL.values())) == len(TOKEN_CODE)


def test_mining_keeps_only_frequent_and_pure_words():
    from retail_pricing.fmcg_name_rules import mine_name_keywords

    decided = (
        [("Fancy tandpasta", "personal_care")] * 25
        + [("Fancy taart", "bakery")] * 3
        + [("Gemengd product", "bakery")] * 15
        + [("Gemengd product", "pantry_cooking")] * 15
        + [("Colruyt product", "meat_poultry_seafood")] * 30
    )
    vocabulary = mine_name_keywords(decided, min_support=20, min_purity=0.98)
    assert set(vocabulary) == {"tandpasta"}
    assert vocabulary["tandpasta"] == ("personal_care", 25, 1.0)


def test_structural_exclusions_and_missing_evidence_remain_explicit():
    assert (
        classify_product_paths([])["fmcg_classification_status"] == "missing_evidence"
    )
    result = classify_product_paths(
        [evidence(["Koffie"], dq_navigation_contamination=True)]
    )
    assert result["fmcg_classification_status"] == "excluded_evidence"


def test_reviewed_source_contradiction_abstains_and_changed_payload_requires_review(
    tmp_path, monkeypatch
):
    from retail_pricing import silver_products as module

    key = ("p", "ah", "be")
    parent = pl.DataFrame({"daltix_id": ["p"], "shop": ["ah"], "country": ["be"]})
    described = parent.with_columns(name=pl.lit("Item"), language=pl.lit("nl"))
    for name in ["products", "weekly"]:
        described.write_parquet(tmp_path / f"{name}.parquet")
    parent.write_parquet(tmp_path / "nutrition.parquet")
    child = pl.DataFrame(
        [
            {
                **parent.row(0, named=True),
                **evidence(["Koffie"]),
                "source_categories_sha256": "reviewed_hash",
            }
        ]
    )
    child.write_parquet(tmp_path / "paths.parquet")
    monkeypatch.setitem(
        module.REVIEW_EXCLUSIONS,
        key,
        {
            "source_payload_hashes": ["reviewed_hash"],
            "reason": "Reviewed contradiction fixture",
        },
    )
    args = [
        tmp_path / f"{n}.parquet"
        for n in ["products", "weekly", "nutrition", "paths", "output"]
    ]
    metrics = module.run_primary_categories_pipeline(*args)
    assert metrics["review_required"] == 1
    assert pl.read_parquet(args[-1])["primary_fmcg_category_code"][0] == "unclassified"
    child.with_columns(
        pl.lit("new_hash").alias("source_categories_sha256")
    ).write_parquet(args[-2])
    with pytest.raises(ValueError, match="evidence changed"):
        module.run_primary_categories_pipeline(*args)


def snapshot(raw):
    raw.mkdir()
    product = pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["idla"],
            "name": ["NAN milk"],
            "brand": ["nan"],
            "country": ["be"],
            "language": ["nl"],
            "description": [None],
            "categories": ['[["Food"], ["Food", "Dairy"]]'],
        }
    )
    product.write_parquet(raw / "products.parquet")
    product.write_parquet(raw / "weekly_prices_products.parquet")
    pl.DataFrame(
        {
            "shop": ["idla"],
            "id": ["l"],
            "country_code": ["be"],
            "type": [None],
            "postcode": ["1000"],
            "geolocation_latitude": [50.0],
            "geolocation_longitude": [4.0],
            "sources": ['["online"]'],
        }
    ).write_parquet(raw / "locations.parquet")
    pl.DataFrame(
        {
            "shop": ["idla"],
            "location": ["l"],
            "location_name": [None],
            "shop_type": ["tekram"],
            "postcode": [None],
            "locality": [None],
            "state": [None],
            "geolocation_latitude": [None],
            "geolocation_longitude": [None],
        }
    ).write_parquet(raw / "weekly_prices_locations.parquet")
    pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["idla"],
            "country": ["be"],
            "location": ["l"],
            "price": [1.0],
            "promo_price": [None],
            "downloaded_on": [date(2020, 1, 6)],
            "unit_std": ["su"],
            "currency": ["eur"],
        }
    ).write_parquet(raw / "prices.parquet")
    pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["idla"],
            "location": ["l"],
            "week": [date(2020, 1, 6)],
            "price": [1.0],
            "price_promo": [1.0],
        }
    ).write_parquet(raw / "weekly_prices.parquet")
    pl.DataFrame(
        {
            "daltix_id": ["p"],
            "shop": ["idla"],
            "country": ["be"],
            "language": ["nl"],
            "download_date": [date(2020, 1, 6)],
            "nutritional_values_std": [
                json.dumps(
                    {
                        "nutrients": {"fats": {"value": 1, "unit": "g"}},
                        "portion": {"value": 100, "unit": "ml"},
                    }
                )
            ],
        }
    ).write_parquet(raw / "nutritionals.parquet")
    return validate_local_snapshot(raw)


def test_full_pipeline_and_failed_rebuild_preserve_last_complete_checkpoint(
    tmp_path, monkeypatch
):
    raw, clean = tmp_path / "raw", tmp_path / "clean"
    original = snapshot(raw)
    result = pipeline.run_silver_pipeline(raw, clean)
    assert result["weekly_locations"]["postcodes_enriched"] == 1
    assert result["products"]["missing_brand"] == 0
    assert result["weekly_locations"]["shop_types_reversed"] == 1
    assert (
        pl.read_parquet(clean / "silver_weekly_locations.parquet")["shop_type"][0]
        == "market"
    )
    assert result["product_category_paths"]["rows"] == 4
    # Rebuilding the same snapshot must preserve every table and metric.
    first = {
        name: pl.read_parquet(clean / filename)
        for name, filename in pipeline.SILVER_FILES.items()
    }
    # Every related table receives the same canonical value, without losing rows.
    expected_rows = {name: 1 for name in first} | {"product_category_paths": 4}
    for name, frame in first.items():
        assert frame["shop"].unique().to_list() == ["aldi"]
        assert frame["shop_name"].unique().to_list() == ["Aldi"]
        assert frame.height == expected_rows[name]
    assert result["cross_table"]["weekly_location_unmatched_rows"] == 0
    assert result["cross_table"]["weekly_product_context_mismatches"] == 0
    assert result["cross_table"]["nutrition_product_context_mismatches"] == 0
    assert pipeline.run_silver_pipeline(raw, clean) == result
    for name, filename in pipeline.SILVER_FILES.items():
        assert first[name].equals(pl.read_parquet(clean / filename))
    # A stale source-oriented shop in any Silver table blocks publication.
    for filename in pipeline.SILVER_FILES.values():
        table = clean / filename
        saved = table.read_bytes()
        try:
            pl.read_parquet(table).with_columns(
                pl.lit("idla").alias("shop")
            ).write_parquet(table)
            with pytest.raises(ValueError, match="must be canonical"):
                pipeline.run_cross_table_contracts(clean)
        finally:
            table.write_bytes(saved)
    # Incorrect reporting names must fail across every published Silver table.
    for filename in pipeline.SILVER_FILES.values():
        table = clean / filename
        saved = table.read_bytes()
        try:
            pl.read_parquet(table).with_columns(
                pl.lit("Wrong retailer").alias("shop_name")
            ).write_parquet(table)
            with pytest.raises(ValueError, match="Shop names differ"):
                pipeline.run_cross_table_contracts(clean)
        finally:
            table.write_bytes(saved)
    manifest = pipeline.load_silver_manifest(clean)
    assert manifest["metrics"] == result and len(manifest["files"]) == len(
        pipeline.SILVER_FILES
    )
    assert {p.name for p in clean.glob("*.parquet")} == set(
        pipeline.SILVER_FILES.values()
    )
    assert all(f["file"] == f"silver_{f['dataset']}.parquet" for f in manifest["files"])
    before = {p.name: sha256_file(p) for p in clean.iterdir() if p.is_file()}

    def fail(**kwargs):
        raise RuntimeError("Injected final-table failure")

    monkeypatch.setattr(pipeline, "run_weekly_prices_pipeline", fail)
    with pytest.raises(RuntimeError, match="Injected"):
        pipeline.run_silver_pipeline(raw, clean)
    assert before == {p.name: sha256_file(p) for p in clean.iterdir() if p.is_file()}
    assert original == validate_local_snapshot(raw)
    assert pipeline.load_silver_manifest(clean) == manifest
    assert not list(clean.glob(".silver-build-*"))


def test_interrupted_publication_never_looks_complete(tmp_path, monkeypatch):
    raw, clean = tmp_path / "raw", tmp_path / "clean"
    snapshot(raw)
    pipeline.run_silver_pipeline(raw, clean)
    real_replace, moves = os.replace, []

    def crash_on_third_move(src, dst):
        moves.append(dst)
        if len(moves) == 3:
            raise OSError("Injected publication failure")
        return real_replace(src, dst)

    monkeypatch.setattr(pipeline.os, "replace", crash_on_third_move)
    with pytest.raises(OSError, match="Injected"):
        pipeline.run_silver_pipeline(raw, clean)
    # The manifest is removed first and written last: a half-published set has none.
    assert not (clean / "manifest.json").exists()
    with pytest.raises(FileNotFoundError, match="No validated Silver checkpoint"):
        pipeline.load_silver_manifest(clean)
    assert not list(clean.glob(".silver-build-*"))
    monkeypatch.undo()
    pipeline.run_silver_pipeline(raw, clean)
    assert pipeline.load_silver_manifest(clean)["status"] == "complete"


def test_readers_refuse_edited_files_and_other_source_code(tmp_path, monkeypatch):
    raw, clean = tmp_path / "raw", tmp_path / "clean"
    snapshot(raw)
    pipeline.run_silver_pipeline(raw, clean)
    pipeline.load_silver_manifest(clean)
    monkeypatch.setattr(pipeline, "code_fingerprint", lambda: "another implementation")
    with pytest.raises(RuntimeError, match="different source code"):
        pipeline.load_silver_manifest(clean)
    monkeypatch.undo()
    table = clean / pipeline.SILVER_FILES["prices"]
    pl.read_parquet(table).with_columns(pl.lit(9.99).alias("price")).write_parquet(
        table
    )
    with pytest.raises(RuntimeError, match="differ from the completed manifest"):
        pipeline.load_silver_manifest(clean)


def test_locations_flag_coordinates_outside_the_benelux_without_dropping_them(tmp_path):
    frame = pl.DataFrame(
        {
            "shop": ["aldi", "aldi"],
            "country_code": ["be", "be"],
            "id": ["brussels", "filipstad"],
            "type": [None, None],
            "geolocation_latitude": [50.85, 59.71],
            "geolocation_longitude": [4.35, 14.17],
            "postcode": ["1000", "68234"],
            "sources": ['["a"]', '["a"]'],
        },
        schema_overrides={"type": pl.String},
    )
    out = tmp_path / "clean/silver_locations.parquet"
    metrics = run_locations_pipeline(write_raw(tmp_path, frame), out)
    result = pl.read_parquet(out).sort("id")
    assert result["dq_outside_benelux"].to_list() == [False, True]
    assert result.height == 2 and metrics["outside_benelux"] == 1


def test_weekly_locations_get_type_region_and_far_coordinate_flag(tmp_path):
    primary = pl.DataFrame(
        {
            "shop": ["crf", "crf", "aldi", "clp"],
            "location": ["base", "ixelles", "filipstad", "beveren"],
            "location_name": ["Default", "Ixelles", "Filipstad", "Beveren"],
            "shop_type": [None] * 4,
            "geolocation_latitude": [None, 50.82, 59.71, 51.21],
            "geolocation_longitude": [None, 4.39, 14.17, 4.25],
            "locality": [None] * 4,
            "postcode": [None, "1050", "68234", None],
            "state": [None] * 4,
        },
        schema_overrides={"shop_type": pl.String, "locality": pl.String},
    )
    fallback = pl.DataFrame(
        schema={
            "shop": pl.String,
            "id": pl.String,
            "country_code": pl.String,
            "postcode": pl.String,
            "geolocation_latitude": pl.Float64,
            "geolocation_longitude": pl.Float64,
            "dq_business_key_collision": pl.Boolean,
        }
    )
    out = tmp_path / "clean/silver_weekly_locations.parquet"
    metrics = run_weekly_locations_pipeline(
        write_raw(tmp_path, primary),
        write_raw(tmp_path, fallback, "fallback"),
        out,
    )
    rows = {r["location"]: r for r in pl.read_parquet(out).to_dicts()}
    assert rows["base"]["location_type"] == "National price (online)"
    assert rows["ixelles"]["location_type"] == "Store"
    # A region needs a four-digit Belgian postcode: never guessed from anything else.
    assert rows["ixelles"]["region"] == "Brussels"
    assert rows["filipstad"]["region"] is None and rows["beveren"]["region"] is None
    assert rows["filipstad"]["dq_outside_benelux"]
    assert not rows["ixelles"]["dq_outside_benelux"]
    assert metrics["outside_benelux"] == 1
    assert metrics["national_price_contexts"] == 1 and metrics["regions_assigned"] == 1
