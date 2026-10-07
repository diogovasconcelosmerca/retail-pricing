"""Reviewed reference data: shop codes and names, nutrient units, FMCG rules."""

import duckdb
import polars as pl
import pytest

from retail_pricing.fmcg_taxonomy import CATEGORY_LABELS, classify_path
from retail_pricing.reference import (
    CANONICAL_SHOPS,
    ENCODED_SHOPS,
    add_shop_names,
    belgian_region,
    canonical_shop_sql,
    canonicalize_shop,
    get_nutrient_mapping,
    location_type,
    outside_benelux,
    require_canonical_shops,
    require_shop_names,
    shop_name_sql,
    validate_nutrient_mapping,
)


def test_complete_reviewed_domain_nulls_and_idempotence_agree_between_engines():
    expected = ["crf", "cg", "ah", "aldi", "lidl", "dll", "jumbo", "clp", "spar"]
    assert list(CANONICAL_SHOPS) == expected
    assert len(set(expected)) == len(ENCODED_SHOPS)
    assert not set(ENCODED_SHOPS) & set(expected)
    values = (
        list(ENCODED_SHOPS)
        + expected
        + [
            None,
            "",
            "  ",
            " NULL ",
            "None",
            "N/A",
            "#N/A",
            " \tidla\n ",
        ]
    )
    frame = pl.DataFrame({"shop": values, "untouched": ["klem"] * len(values)})
    clean = canonicalize_shop(frame)
    assert clean["shop"].to_list() == expected * 2 + [None] * 7 + ["aldi"]
    assert clean.height == frame.height
    assert clean["untouched"].equals(frame["untouched"])
    assert canonicalize_shop(clean).equals(clean)
    with duckdb.connect() as con:
        con.register("source", frame)
        first = con.sql(
            f"SELECT {canonical_shop_sql()} AS shop, untouched FROM source"
        ).pl()
        assert first.equals(clean)
        con.register("clean", first)
        assert (
            con.sql(f"SELECT {canonical_shop_sql()} AS shop, untouched FROM clean")
            .pl()
            .equals(clean)
        )


@pytest.mark.parametrize("value", ["unknown", "ALDI", "new'code", "123"])
def test_unreviewed_codes_fail_in_both_engines(value):
    frame = pl.DataFrame({"shop": [value]})
    with pytest.raises(ValueError, match="Unknown shop values require review"):
        canonicalize_shop(frame)
    with duckdb.connect() as con:
        con.register("source", frame)
        with pytest.raises(duckdb.Error, match="Unknown shop values require review"):
            con.sql(f"SELECT {canonical_shop_sql()} FROM source").fetchall()


def test_non_string_input_fails_safely_and_stale_silver_is_rejected():
    with pytest.raises(ValueError, match="Unknown shop values require review"):
        canonicalize_shop(pl.DataFrame({"shop": [123]}))
    assert canonicalize_shop(pl.DataFrame({"shop": [None]}))["shop"].to_list() == [None]
    require_canonical_shops([None, *CANONICAL_SHOPS])
    with pytest.raises(ValueError, match="must be canonical"):
        require_canonical_shops(["aldi", "idla"])


def test_reviewed_names_agree_in_both_engines_without_changing_identity():
    codes = ["aldi", "crf", "clp", "ah", "jumbo", "lidl", "spar", "dll", "cg", None]
    names = [
        "Aldi",
        "Carrefour",
        "Colruyt Lowest Prices",
        "Albert Heijn",
        "Jumbo",
        "Lidl",
        "SPAR",
        "Delhaize",
        "Collect&Go",
        None,
    ]
    raw = pl.DataFrame({"shop": codes, "observation_id": range(len(codes))})
    expected = add_shop_names(raw)
    assert set(codes[:-1]) == set(CANONICAL_SHOPS)
    assert expected["shop_name"].to_list() == names
    assert not set(names).intersection(CANONICAL_SHOPS)
    assert expected.select(raw.columns).equals(raw)
    assert add_shop_names(expected).equals(expected)
    require_shop_names(expected)
    with duckdb.connect() as con:
        con.register("source", raw)
        assert (
            con.sql(f"SELECT *, {shop_name_sql()} AS shop_name FROM source")
            .pl()
            .equals(expected)
        )
    assert add_shop_names(raw.head(0)).schema["shop_name"] == pl.String


@pytest.mark.parametrize("code", ["idla", "unreviewed", "ALDI"])
def test_labels_never_guess_or_redecode_shop_codes(code):
    frame = pl.DataFrame({"shop": [code]})
    with pytest.raises(ValueError, match="must be canonical"):
        add_shop_names(frame)
    with duckdb.connect() as con:
        con.register("source", frame)
        with pytest.raises(duckdb.Error, match="Unknown canonical shop"):
            con.sql(f"SELECT {shop_name_sql()} FROM source").fetchall()


def test_missing_or_wrong_names_fail_instead_of_being_repaired_downstream():
    with pytest.raises(ValueError, match="Missing Silver shop_name"):
        require_shop_names(pl.DataFrame({"shop": ["aldi"]}))
    with pytest.raises(ValueError, match="Shop names differ"):
        require_shop_names(pl.DataFrame({"shop": ["aldi"], "shop_name": ["Carrefour"]}))


def test_mapping_rejects_duplicate_keys_bad_factors_and_inconsistent_units():
    good = get_nutrient_mapping()
    with pytest.raises(ValueError, match="Duplicate"):
        validate_nutrient_mapping(pl.concat([good, good.head(1)]))
    for factor in [0.0, -1.0, float("nan"), float("inf")]:
        bad = good.with_columns(
            pl.when(pl.col("mapping_status") == "identity")
            .then(pl.lit(factor))
            .otherwise(pl.col("conversion_factor"))
            .alias("conversion_factor")
        )
        with pytest.raises(ValueError, match="finite and positive"):
            validate_nutrient_mapping(bad)
    bad = good.with_columns(
        pl.when((pl.col("raw_nutrient_name") == "energy") & (pl.col("raw_unit") == "g"))
        .then(pl.lit("kcal"))
        .otherwise(pl.col("canonical_unit"))
        .alias("canonical_unit")
    )
    with pytest.raises(ValueError, match="Inconsistent"):
        validate_nutrient_mapping(bad)
    with pytest.raises(ValueError, match="NULL factor"):
        validate_nutrient_mapping(
            good.with_columns(pl.lit(1.0).alias("conversion_factor"))
        )


@pytest.mark.parametrize(
    "path,expected",
    [
        (["Babyvoeding", "6 maanden"], "baby_care"),
        (["Diepvries", "Vis"], "frozen"),
        (["Huishouden", "Diepvrieszakjes"], "household_supplies"),
        (["Dierenvoeding", "Kip"], "pet_care"),
        (["Zuivel", "Plantaardige dranken"], "plant_based_alternatives"),
        (["Wijn, bier, sterke drank", "Alcoholvrij"], "non_alcoholic_beverages"),
        (["Groenten en fruit", "Vruchtenconserven"], "pantry_cooking"),
        (["Groenten en fruit", "Gedroogd fruit"], "confectionery_snacks"),
        (["Ontbijtgranen", "Ontbijtrepen"], "confectionery_snacks"),
        (["Verzorging & hygiÃƒÂ«ne", "Make-up"], "beauty_cosmetics"),
        (["Onderhoud", "Toiletpapier"], "household_supplies"),
        (
            [
                "Frisdrank, sappen, koffie, thee",
                "Koffie",
                "Koffieverrijkers",
                "Zoetstof",
            ],
            "pantry_cooking",
        ),
        (["\t KOFFIE  "], "coffee_tea"),
        (["(Slow)Juicers (Fruitpers)"], "non_fmcg"),
    ],
)
def test_reviewed_merchandise_boundaries(path, expected):
    codes, rules, _ = classify_path(path, "nl", "ah")
    assert codes == {expected} and rules


@pytest.mark.parametrize(
    "path,language",
    [
        (["2+1 op Snoep", "Chocolade"], "nl"),
        (["6 maanden"], "nl"),
        (["Dranken en alcohol"], "nl"),
        (["Baby"], "nl"),
        (["Niet-voeding"], "nl"),
        (["Koffie"], "fr"),
        (["Afslankingsproducten", "Chocolade"], "nl"),
    ],
)
def test_unreviewed_or_non_merchandise_evidence_abstains(path, language):
    assert classify_path(path, language, "ah")[0] == set()


def test_all_requested_categories_are_present_with_unique_english_labels():
    assert len(CATEGORY_LABELS) == 23
    assert len(set(CATEGORY_LABELS.values())) == 23
    assert CATEGORY_LABELS["unclassified"] == "Other / Unclassified"


def test_belgian_region_follows_postcode_ranges_and_ignores_other_shapes():
    postcodes = ["1000", "1299", "1300", "1499", "1500", "3999", "4000", "7999"]
    postcodes += ["8000", "9999", "0999", "5657AH", "68234", "7130 BINCHE", None]
    expected = ["Brussels", "Brussels", "Wallonia", "Wallonia", "Flanders"]
    expected += ["Flanders", "Wallonia", "Wallonia", "Flanders", "Flanders"]
    expected += [None] * 5
    frame = pl.DataFrame({"postcode": postcodes}, schema={"postcode": pl.String})
    assert frame.select(belgian_region().alias("r"))["r"].to_list() == expected


def test_far_coordinates_are_flagged_and_missing_ones_are_not():
    frame = pl.DataFrame(
        {
            "geolocation_latitude": [50.85, 52.37, 49.6, 59.71, 50.0, None],
            "geolocation_longitude": [4.35, 4.90, 6.13, 14.17, 1.0, None],
        }
    )
    flags = frame.select(outside_benelux().alias("flag"))["flag"].to_list()
    # Brussels, Amsterdam and Luxembourg pass; Filipstad and an English point do not.
    assert flags == [False, False, False, True, True, False]


def test_default_location_is_a_national_price_and_others_are_stores():
    names = ["Default", " default ", "DEFAULT", "Ixelles", None]
    frame = pl.DataFrame({"location_name": names}, schema={"location_name": pl.String})
    assert frame.select(location_type().alias("t"))["t"].to_list() == [
        "National price (online)",
        "National price (online)",
        "National price (online)",
        "Store",
        "Store",
    ]
