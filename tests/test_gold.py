"""Gold builders on tiny Silver fixtures, the Gold contracts and the parity comparison."""

import json
import math
from copy import deepcopy
from datetime import date

import duckdb
import polars as pl
import pytest

from retail_pricing.gold import (
    add_price_comparison_columns,
    attach_primary_categories,
    build_category_model,
    build_dim_date,
    build_dim_location,
    build_dim_nutrient,
    build_dim_product,
    build_dim_shop,
    create_fact_product_nutrition,
    create_fact_weekly_prices,
)
from retail_pricing.gold_validation import validate_gold_model
from retail_pricing.parity import compare
from retail_pricing.reference import add_shop_names
from retail_pricing.silver_nutrition import run_nutritionals_pipeline
from retail_pricing.silver_products import (
    SCHEMA,
    classify_product_paths,
    run_category_paths_pipeline,
)

RATIO_COLUMNS = [
    "log_paid_yoy_ratio",
    "log_shelf_yoy_ratio",
    "log_store_vs_national_ratio",
]


def write(tmp_path, name, frame):
    path = tmp_path / f"{name}.parquet"
    frame.write_parquet(path)
    return path


def product_rows(*rows):
    """Build a products-style frame from (daltix_id, shop, country, name, brand)."""
    return pl.DataFrame(
        {
            "daltix_id": [r[0] for r in rows],
            "shop": [r[1] for r in rows],
            "country": [r[2] for r in rows],
            "language": ["nl"] * len(rows),
            "name": [r[3] for r in rows],
            "brand": [r[4] for r in rows],
            "description": [None] * len(rows),
            "categories": [None] * len(rows),
        },
        schema_overrides={"description": pl.String, "categories": pl.String},
    )


def test_dim_product_holds_only_mart_members_and_infers_unmatched_prices(tmp_path):
    weekly = write(
        tmp_path,
        "weekly",
        product_rows(
            ("w1", "aldi", "be", "Weekly name", None),
            ("w2", "aldi", "be", "Not priced, not in nutrition", "B2"),
        ),
    )
    nutrition = write(
        tmp_path,
        "nutrition",
        pl.DataFrame(
            {
                "daltix_id": ["n-be", "n-nl", "w1"],
                "shop": ["ah", "ah", "aldi"],
                "country": ["be", "nl", "be"],
                "language": ["nl", "nl", "nl"],
            }
        ),
    )
    products = write(
        tmp_path,
        "products",
        product_rows(
            ("w1", "aldi", "be", "Old name", "Reference brand"),
            ("r1", "aldi", "be", "Recovered", "Br"),
            ("amb", "aldi", "be", "Ambiguous", "Br"),
            ("amb", "aldi", "fr", "Ambiguous", "Br"),
        ),
    )
    prices = write(
        tmp_path,
        "prices",
        add_shop_names(
            pl.DataFrame(
                {
                    "daltix_id": ["w1", "r1", "amb", "ghost"],
                    "shop": ["aldi"] * 4,
                }
            )
        ),
    )
    dim = build_dim_product(weekly, nutrition, products, prices)
    rows = {r["daltix_id"]: r for r in dim.to_dicts()}
    # Keys follow the sorted natural identity; -1 is the single Unknown member.
    assert dim["product_key"].to_list() == [-1, 1, 2, 3, 4, 5]
    assert rows[None]["name"] == "Unknown / Unmatched Product"
    assert rows[None]["descriptive_source"] == "unknown"
    assert not rows[None]["is_inferred_product"]
    # Weekly values win; the products reference only fills the missing brand.
    assert rows["w1"]["name"] == "Weekly name"
    assert rows["w1"]["brand"] == "Reference brand"
    assert rows["w1"]["descriptive_source"] == "weekly_products"
    # Belgian nutrition-only products stay; Dutch ones and unreferenced ones do not.
    assert rows["n-be"]["descriptive_source"] == "identity_only"
    assert "n-nl" not in rows and "w2" not in rows
    # A priced identity is recovered only when the reference supports one country.
    assert rows["r1"]["country"] == "be"
    assert rows["r1"]["descriptive_source"] == "products"
    # Every other price identity gets its own member instead of sharing Unknown.
    assert "amb" in rows and rows["amb"]["is_inferred_product"]
    ghost = rows["ghost"]
    assert ghost["is_inferred_product"] and ghost["country"] == "be"
    assert ghost["name"] == "Unmatched product (Aldi ghost)"
    assert ghost["brand"] is None
    assert ghost["descriptive_source"] == "inferred_from_prices"
    assert not rows["w1"]["is_inferred_product"]


def test_dim_date_covers_whole_years_with_iso_weeks(tmp_path):
    prices = write(
        tmp_path,
        "prices",
        pl.DataFrame({"week": [date(2020, 1, 6), date(2020, 12, 28)]}),
    )
    nutrition = write(
        tmp_path,
        "nutrition",
        pl.DataFrame({"download_date": [date(2020, 11, 27)]}),
    )
    dim = build_dim_date(prices, nutrition)
    assert dim["date"].min() == date(2020, 1, 1) and dim["date"].max() == date(
        2020, 12, 31
    )
    assert dim["date_key"].n_unique() == dim.height == 366
    monday = dim.filter(pl.col("date") == date(2020, 12, 28)).row(0, named=True)
    assert monday["date_key"] == 20201228 and monday["year_week"] == "2020-W53"
    assert monday["week_start_date"] == date(2020, 12, 28)
    assert monday["week_end_date"] == date(2021, 1, 3)
    assert monday["quarter_label"] == "2020-Q4"
    # Sort keys let Power BI order quarters and months chronologically.
    assert monday["year_quarter_key"] == 20204 and monday["year_month_key"] == 202012


def test_dim_shop_consumes_silver_names_and_keeps_code_sorted_keys(tmp_path):
    weekly = write(
        tmp_path,
        "weekly",
        add_shop_names(pl.DataFrame({"shop": ["clp", "aldi", "clp"]})),
    )
    nutrition = write(
        tmp_path,
        "nutrition",
        add_shop_names(
            pl.DataFrame(
                {
                    "shop": ["cg", "ah", "aldi", "jumbo"],
                    "country": ["be", "be", "be", "nl"],
                }
            )
        ),
    )
    # jumbo has only Dutch nutrition, so it is outside the Belgian mart.
    assert build_dim_shop(weekly, nutrition).to_dicts() == [
        {"shop_key": 1, "shop": "ah", "shop_name": "Albert Heijn"},
        {"shop_key": 2, "shop": "aldi", "shop_name": "Aldi"},
        {"shop_key": 3, "shop": "cg", "shop_name": "Collect&Go"},
        {"shop_key": 4, "shop": "clp", "shop_name": "Colruyt Lowest Prices"},
    ]
    conflicting = add_shop_names(
        pl.DataFrame({"shop": ["aldi"], "country": ["be"]})
    ).with_columns(pl.lit("Different retailer").alias("shop_name"))
    conflicting.write_parquet(nutrition)
    with pytest.raises(ValueError, match="Shop names differ"):
        build_dim_shop(weekly, nutrition)


def test_dim_location_keeps_only_price_contexts_with_type_and_region(tmp_path):
    def location(shop, code, name, postcode):
        return {
            "shop": shop,
            "location": code,
            "location_name": name,
            "shop_type": None,
            "geolocation_latitude": None,
            "geolocation_longitude": None,
            "locality": None,
            "postcode": postcode,
            "state": None,
            "location_type": "National price (online)"
            if name == "Default"
            else "Store",
            "region": {"1050": "Brussels", "4430": "Wallonia"}.get(postcode),
            "dq_postcode_conflict": False,
            "dq_latitude_conflict": False,
            "dq_longitude_conflict": False,
            "dq_enrichment_context_mismatch": False,
            "is_postcode_enriched": False,
            "is_coordinates_enriched": False,
            "dq_invalid_latitude": False,
            "dq_invalid_longitude": False,
            "dq_incomplete_coordinates": False,
            "dq_outside_benelux": False,
        }

    weekly_locations = write(
        tmp_path,
        "weekly_locations",
        pl.DataFrame(
            [
                location("crf", "base", "Default", None),
                location("crf", "ixelles", "Ixelles", "1050"),
                location("crf", "unpriced", "Unpriced store", "4430"),
                location("clp", "ans", "Ans", "4430"),
            ],
            schema_overrides={
                "shop_type": pl.String,
                "geolocation_latitude": pl.Float64,
                "geolocation_longitude": pl.Float64,
                "locality": pl.String,
                "state": pl.String,
            },
        ),
    )
    prices = write(
        tmp_path,
        "prices",
        pl.DataFrame(
            {
                "shop": ["crf", "crf", "clp"],
                "location": ["base", "ixelles", "ans"],
            }
        ),
    )
    dim = build_dim_location(weekly_locations, prices)
    assert dim["location"].to_list() == ["ans", "base", "ixelles"]
    assert dim["location_key"].to_list() == [1, 2, 3]
    rows = {r["location"]: r for r in dim.to_dicts()}
    assert rows["base"]["location_type"] == "National price (online)"
    assert rows["base"]["region"] is None
    assert rows["ixelles"]["location_type"] == "Store"
    assert rows["ixelles"]["region"] == "Brussels"
    assert rows["ans"]["region"] == "Wallonia"


def test_dim_nutrient_lists_only_nutrients_measured_in_the_mart_market(tmp_path):
    nutrition = write(
        tmp_path,
        "nutrition",
        pl.DataFrame(
            {
                "nutrient_name": ["fats", "salt", "energy"],
                "country": ["be", "be", "nl"],
            }
        ),
    )
    assert build_dim_nutrient(nutrition)["nutrient_name"].to_list() == ["fats", "salt"]


def test_weekly_price_fact_keeps_variation_and_unmatched_products(tmp_path):
    week = [date(2020, 1, 6), date(2020, 1, 13), date(2020, 1, 13), date(2020, 1, 20)]
    prices = write(
        tmp_path,
        "prices",
        pl.DataFrame(
            {
                "daltix_id": ["w1"] * 4 + ["ghost"],
                "shop": ["aldi"] * 5,
                "location": ["l1"] * 5,
                "week": week + [date(2020, 1, 6)],
                "price": [2.0, 2.0, 2.2, 3.0, 1.5],
                "price_promo": [2.0, 2.0, 2.2, 2.0, 1.5],
                "is_promotion": [False, False, False, True, False],
                "has_multiple_price_versions": [False, True, True, False, False],
            }
        ),
    )
    dim_product = pl.DataFrame(
        {"product_key": [-1, 1], "daltix_id": [None, "w1"], "shop": [None, "aldi"]},
        schema_overrides={"daltix_id": pl.String, "shop": pl.String},
    )
    dim_shop = pl.DataFrame({"shop_key": [1], "shop": ["aldi"]})
    dim_location = pl.DataFrame(
        {"location_key": [1], "shop": ["aldi"], "location": ["l1"]}
    )
    dim_date = pl.DataFrame(
        {
            "date_key": [20200106, 20200113, 20200120],
            "date": [date(2020, 1, 6), date(2020, 1, 13), date(2020, 1, 20)],
        }
    )
    with duckdb.connect() as con:
        create_fact_weekly_prices(
            con, prices, dim_product, dim_shop, dim_location, dim_date
        )
        fact = con.sql("SELECT * FROM fact_weekly_prices").pl()
    rows = {(r["source_daltix_id"], r["date_key"]): r for r in fact.to_dicts()}
    assert fact.height == 4  # one row per product, location and week
    ordinary = rows[("w1", 20200106)]
    assert ordinary["product_key"] == 1 and ordinary["effective_price"] == 2.0
    # Two different prices in one week: no scalar price, but the range is kept.
    varied = rows[("w1", 20200113)]
    assert varied["regular_price"] is None and varied["effective_price"] is None
    assert (varied["regular_price_min"], varied["regular_price_max"]) == (2.0, 2.2)
    assert (
        varied["has_intraweek_price_variation"]
        and varied["price_observation_versions"] == 2
    )
    promotion = rows[("w1", 20200120)]
    assert promotion["is_promotion"] and promotion["promo_price"] == 2.0
    assert promotion["discount_amount"] == 1.0
    assert promotion["discount_pct"] == pytest.approx(1 / 3)
    # An identity without a Product member maps to -1 and stays traceable.
    unmatched = rows[("ghost", 20200106)]
    assert unmatched["product_key"] == -1 and unmatched["effective_price"] == 1.5


def test_nutrition_fact_keeps_basis_and_unusable_values(tmp_path):
    def payload(unit):
        return json.dumps(
            {
                "nutrients": {
                    "fats": {"value": 3, "unit": "g"},
                    "salt": {"value": 1, "unit": "ml"},
                },
                "portion": {"value": 100, "unit": unit},
            }
        )

    (tmp_path / "raw").mkdir()
    raw = write(
        tmp_path / "raw",
        "source",
        pl.DataFrame(
            {
                "daltix_id": ["p1", "p2", "p1"],
                "shop": ["aldi", "aldi", "aldi"],
                "country": ["be", "be", "be"],
                "language": ["nl", "nl", "nl"],
                "download_date": [
                    date(2020, 11, 27),
                    date(2020, 11, 27),
                    date(2020, 12, 15),
                ],
                "nutritional_values_std": [payload("g"), payload("ml"), payload("g")],
            }
        ),
    )
    silver = tmp_path / "silver_nutritionals.parquet"
    run_nutritionals_pipeline(raw, silver)
    dim_product = pl.DataFrame(
        {
            "product_key": [1, 2],
            "daltix_id": ["p1", "p2"],
            "shop": ["aldi", "aldi"],
            "country": ["be", "be"],
        }
    )
    dim_shop = pl.DataFrame({"shop_key": [1], "shop": ["aldi"]})
    dim_date = pl.DataFrame(
        {
            "date_key": [20201127, 20201215],
            "date": [date(2020, 11, 27), date(2020, 12, 15)],
        }
    )
    dim_nutrient = pl.DataFrame(
        {"nutrient_key": [1, 2], "nutrient_name": ["fats", "salt"]}
    )
    with duckdb.connect() as con:
        create_fact_product_nutrition(
            con, silver, dim_product, dim_shop, dim_date, dim_nutrient
        )
        fact = con.sql("SELECT * FROM fact_product_nutrition").pl()
    assert fact.height == 6
    bases = dict(zip(fact["product_key"], fact["nutrition_basis"], strict=False))
    assert bases == {1: "per_100g", 2: "per_100ml"}
    # Salt in ml is an incompatible unit: the row stays, its value is unusable.
    salt = fact.filter(pl.col("nutrient_key") == 2)
    assert salt["is_value_usable"].to_list() == [False] * 3
    assert salt["dq_incompatible_nutrient_unit"].all()
    assert fact.filter(pl.col("nutrient_key") == 1)["is_value_usable"].all()
    # Benchmarks weigh a product once: only its most recent label is flagged.
    latest = {
        (row["product_key"], row["date_key"]): row["is_latest_observation"]
        for row in fact.iter_rows(named=True)
    }
    assert latest == {
        (1, 20201127): False,
        (1, 20201215): True,
        (2, 20201127): True,
    }


def test_category_model_keeps_eligible_paths_once_per_product(tmp_path):
    def parent(rows):
        return pl.DataFrame(
            {
                "daltix_id": [r[0] for r in rows],
                "shop": ["aldi"] * len(rows),
                "country": ["be"] * len(rows),
                "language": ["nl"] * len(rows),
                "categories": [r[1] for r in rows],
            }
        )

    products = write(
        tmp_path,
        "products",
        parent(
            [
                ("w1", json.dumps([["Food", "Dairy"], ["Food"]])),
                ("w2", json.dumps([["Food", "Dairy"]])),
                ("outside", json.dumps([["Other"]])),
            ]
        ),
    )
    weekly = write(tmp_path, "weekly", parent([("w1", None)]))
    paths = tmp_path / "paths.parquet"
    run_category_paths_pipeline(products, weekly, paths)
    dim_product = pl.DataFrame(
        {
            "product_key": [1, 2],
            "daltix_id": ["w1", "w2"],
            "shop": ["aldi", "aldi"],
            "country": ["be", "be"],
        }
    )
    dim_category, bridge = build_category_model(paths, dim_product)
    # The "Food" path is a prefix of "Food > Dairy" and the outside product has no member.
    assert dim_category["category_path_text"].to_list() == ["Food > Dairy"]
    assert dim_category["category_leaf"].to_list() == ["Dairy"]
    assert bridge.sort("product_key").rows() == [(1, 1), (2, 1)]


def test_primary_category_join_preserves_rows_keys_and_context(tmp_path):
    def decision(path, country):
        evidence = {
            "category_path_normalized": path,
            "language": "nl",
            "shop": "ah",
            "source_dataset": "weekly_prices_products",
            "category_path_position": 1,
        }
        return {
            "daltix_id": "p",
            "shop": "ah",
            "country": country,
            **classify_product_paths([evidence]),
        }

    lookup = add_shop_names(
        pl.DataFrame(
            [decision(["Koffie"], "be"), decision(["Wijn"], "nl")], schema=SCHEMA
        )
    )
    path = write(tmp_path, "classification", lookup)
    products = pl.DataFrame(
        {
            "product_key": [-1, 17, 42, 43],
            "daltix_id": [None, "p", "p", "unmatched"],
            "shop": [None, "ah", "ah", "ah"],
            "country": [None, "be", "nl", "be"],
            "is_inferred_product": [False, False, False, True],
        }
    )
    result = attach_primary_categories(products, path)
    assert result.select(products.columns).equals(products)
    assert result["primary_fmcg_category_code"].to_list() == [
        "unclassified",
        "coffee_tea",
        "alcoholic_beverages",
        "unclassified",
    ]
    assert result["fmcg_classification_status"][3] == "missing_evidence"
    assert result["fmcg_classification_status"][0] == "unknown_product"
    pl.concat([lookup, lookup.head(1)]).write_parquet(path)
    with pytest.raises((ValueError, RuntimeError)):
        attach_primary_categories(products, path)
    lookup.head(1).write_parquet(path)
    with pytest.raises(ValueError, match="missing its Silver classification"):
        attach_primary_categories(products, path)
    wrong = lookup.with_columns(
        pl.lit("Coffee & Tea").alias("primary_fmcg_category_en")
    )
    wrong.write_parquet(path)
    with pytest.raises(ValueError, match="disagree"):
        attach_primary_categories(products, path)


BASELINE = {
    "raw": {"products": 3},
    "silver": {
        "silver_products": {
            "rows": 3,
            "columns": ["daltix_id", "shop"],
            "fingerprint": "3:a:b",
        }
    },
    "gold": {"dim_shop": {"rows": 1, "columns": ["shop_key"], "fingerprint": "1:c:d"}},
    "metrics": {"fact_weekly_prices.unknown_product_rows": 476695},
}
ACTUAL = {
    **deepcopy(BASELINE),
    "invariants": {"dim_shop: shop_key unique": 0},
}


def failed(actual):
    results = compare(actual, BASELINE)
    return [r.check for r in results if not r.passed]


def test_parity_passes_only_when_every_baseline_value_matches():
    assert failed(ACTUAL) == []
    for change, expected in [
        (lambda a: a["raw"].update(products=4), ["products rows"]),
        (
            lambda a: a["silver"]["silver_products"].update(rows=2),
            ["silver_products rows"],
        ),
        (
            lambda a: a["silver"]["silver_products"].update(columns=["daltix_id"]),
            ["silver_products columns"],
        ),
        (
            lambda a: a["gold"]["dim_shop"].update(fingerprint="1:c:e"),
            ["dim_shop content fingerprint"],
        ),
        (
            lambda a: a["metrics"].update(
                {"fact_weekly_prices.unknown_product_rows": 0}
            ),
            ["fact_weekly_prices.unknown_product_rows"],
        ),
        (
            lambda a: a["invariants"].update({"dim_shop: shop_key unique": 2}),
            ["dim_shop: shop_key unique"],
        ),
        (lambda a: a["gold"].update(dim_shop=None), ["dim_shop exists"]),
    ]:
        changed = deepcopy(ACTUAL)
        change(changed)
        assert failed(changed) == expected


# --- Gold contracts ---------------------------------------------------------------


def mini_mart():
    """A consistent one-market mart: dimensions and the two facts, all tiny."""
    dimensions = {
        "dim_date": pl.DataFrame({"date_key": [20200106], "date": [date(2020, 1, 6)]}),
        "dim_shop": pl.DataFrame(
            {"shop_key": [1], "shop": ["aldi"], "shop_name": ["Aldi"]}
        ),
        "dim_product": pl.DataFrame(
            {
                "product_key": [-1, 1, 2],
                "daltix_id": [None, "p1", "p2"],
                "shop": [None, "aldi", "aldi"],
                "country": [None, "be", "be"],
                "is_inferred_product": [False, False, True],
                "fmcg_classification_status": [
                    "unknown_product",
                    "classified",
                    "missing_evidence",
                ],
            }
        ),
        "dim_category": pl.DataFrame(
            {
                "category_key": [1],
                "shop": ["aldi"],
                "country": ["be"],
                "category_path_normalized": [["Food"]],
            }
        ),
        "bridge_product_category": pl.DataFrame(
            {"product_key": [1], "category_key": [1]}
        ),
        "dim_location": pl.DataFrame(
            {
                "location_key": [1],
                "shop": ["aldi"],
                "location": ["base"],
                "location_type": ["National price (online)"],
            }
        ),
        "dim_nutrient": pl.DataFrame({"nutrient_key": [1], "nutrient_name": ["fats"]}),
    }
    facts = {
        "fact_weekly_prices": pl.DataFrame(
            {
                "product_key": [1, 2],
                "shop_key": [1, 1],
                "location_key": [1, 1],
                "date_key": [20200106, 20200106],
                "source_daltix_id": ["p1", "p2"],
            }
        ),
        "fact_product_nutrition": pl.DataFrame(
            {
                "product_key": [1],
                "shop_key": [1],
                "date_key": [20200106],
                "nutrient_key": [1],
                "nutrition_basis": ["per_100g"],
                "is_latest_observation": [True],
            }
        ),
    }
    return dimensions, facts


def run_contracts(dimensions, facts):
    with duckdb.connect() as con:
        for name, frame in facts.items():
            con.register(name, frame)
        validate_gold_model(con, **dimensions)


def test_contracts_accept_a_consistent_mart():
    run_contracts(*mini_mart())


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (
            lambda d, f: d.update(
                dim_shop=pl.concat([d["dim_shop"], d["dim_shop"]]).with_columns(
                    shop_key=pl.Series([1, 2])
                )
            ),
            "dim_shop natural key is not unique",
        ),
        (
            lambda d, f: d.update(
                dim_product=d["dim_product"].with_columns(
                    product_key=pl.Series([-1, 1, 1])
                )
            ),
            "dim_product product_key is not unique",
        ),
        (
            lambda d, f: f.update(
                fact_weekly_prices=pl.concat(
                    [f["fact_weekly_prices"], f["fact_weekly_prices"].head(1)]
                )
            ),
            "fact_weekly_prices grain is not unique",
        ),
        (
            lambda d, f: f.update(
                fact_weekly_prices=f["fact_weekly_prices"].with_columns(
                    location_key=pl.Series([1, 99])
                )
            ),
            "fact_weekly_prices contains invalid foreign keys",
        ),
        (
            lambda d, f: d.update(
                bridge_product_category=pl.concat(
                    [d["bridge_product_category"], d["bridge_product_category"]]
                )
            ),
            "bridge contains duplicate memberships",
        ),
        (
            lambda d, f: d.update(
                dim_location=pl.concat(
                    [
                        d["dim_location"],
                        d["dim_location"].with_columns(
                            location_key=pl.lit(2, dtype=pl.Int64),
                            location=pl.lit("never-priced"),
                        ),
                    ]
                )
            ),
            "dim_location holds unpriced locations",
        ),
        (
            lambda d, f: f.update(
                fact_product_nutrition=pl.concat(
                    [
                        f["fact_product_nutrition"],
                        f["fact_product_nutrition"].with_columns(
                            date_key=pl.lit(20200113, dtype=pl.Int64)
                        ),
                    ]
                )
            ),
            "a nutrient series has two latest dates",
        ),
    ],
)
def test_contracts_reject_broken_keys_grains_and_unused_members(change, message):
    dimensions, facts = mini_mart()
    if message == "a nutrient series has two latest dates":
        dimensions["dim_date"] = pl.DataFrame(
            {
                "date_key": [20200106, 20200113],
                "date": [date(2020, 1, 6), date(2020, 1, 13)],
            }
        )
    change(dimensions, facts)
    with pytest.raises(ValueError, match=message):
        run_contracts(dimensions, facts)


def test_price_comparison_columns_compare_a_year_apart_and_store_vs_national():
    dim_date = pl.DataFrame(
        {
            "date_key": [20190107, 20200106],
            "date": [date(2019, 1, 7), date(2020, 1, 6)],  # 364 days apart
        }
    )
    dim_location = pl.DataFrame(
        {"location_key": [1, 2], "location_type": ["National price (online)", "Store"]}
    )
    # product, location, week, source id, effective price, regular price
    rows = [
        (1, 1, 20190107, "a", 2.0, 2.0),
        (1, 1, 20200106, "a", 2.2, 2.0),  # paid +10%, shelf unchanged
        (1, 2, 20200106, "a", 2.42, 2.42),  # store 10% above the national price
        (2, 1, 20200106, "b", 5.0, 5.0),  # no week a year earlier
        (3, 1, 20190107, "c", None, 4.0),  # missing price: only the shelf pair
        (3, 1, 20200106, "c", 3.0, 4.4),
        (4, 1, 20190107, "d", 0.0, 0.0),  # a price of zero is never a pair
        (4, 1, 20200106, "d", 3.0, 3.0),
        (-1, 1, 20190107, "x", 1.0, 1.0),  # the Unknown product is never compared
        (-1, 1, 20200106, "x", 2.0, 2.0),
    ]
    fact = pl.DataFrame(
        rows,
        schema={
            "product_key": pl.Int64,
            "location_key": pl.Int64,
            "date_key": pl.Int32,
            "source_daltix_id": pl.String,
            "effective_price": pl.Float64,
            "regular_price": pl.Float64,
        },
        orient="row",
    ).with_columns(shop_key=pl.lit(1, dtype=pl.Int64))
    with duckdb.connect() as con:
        con.register("dim_date", dim_date)
        con.register("dim_location", dim_location)
        con.register("weekly_prices_input", fact)
        con.execute(
            "CREATE TEMP TABLE fact_weekly_prices AS SELECT * FROM weekly_prices_input"
        )
        add_price_comparison_columns(con)
        result = con.sql("SELECT * FROM fact_weekly_prices").pl()

    # Same rows, same columns, three ratios added at the end; the order is the grain.
    assert result.height == fact.height
    assert result.columns == [*fact.columns, *RATIO_COLUMNS]
    assert [result[c].dtype for c in RATIO_COLUMNS] == [pl.Float64] * 3
    assert result["date_key"].to_list() == sorted(result["date_key"].to_list())

    def ratios(product, location, week):
        row = result.filter(
            (pl.col("product_key") == product)
            & (pl.col("location_key") == location)
            & (pl.col("date_key") == week)
        ).row(0, named=True)
        return [row[c] for c in RATIO_COLUMNS]

    # A week a year earlier gives paid and shelf ratios; the first year has none.
    paid, shelf, store = ratios(1, 1, 20200106)
    assert paid == pytest.approx(math.log(1.1)) and shelf == pytest.approx(0.0)
    assert store is None  # the national price itself is not compared
    assert ratios(1, 1, 20190107) == [None, None, None]
    # A store price is compared with the national price of the same week.
    assert ratios(1, 2, 20200106) == [None, None, pytest.approx(math.log(1.1))]
    # No earlier week, a missing price, a zero price and Unknown give NULL.
    assert ratios(2, 1, 20200106) == [None, None, None]
    paid, shelf, _ = ratios(3, 1, 20200106)
    assert paid is None and shelf == pytest.approx(math.log(1.1))
    assert ratios(4, 1, 20200106) == [None, None, None]
    assert ratios(-1, 1, 20200106) == [None, None, None]
