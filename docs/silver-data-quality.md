# Silver data quality

Silver turns the seven raw source tables into datasets a category or pricing analyst
can trust, without deciding things the data cannot support. The rules are few:

- **Keep evidence, flag exceptions.** A suspect value gets a `dq_*` flag; it is not
  deleted or silently repaired. Raw stays untouched and remains the audit trail.
- **Missing is not wrong.** A product without a brand or a store without coordinates
  stays in the data with a NULL.
- **Context is part of identity.** A product is `(daltix_id, shop, country)`, a
  location is `(shop, location)`. The same id in another retailer is another product.
- **Unknown values stop the build.** New shop codes, shop types or nutrient units
  fail with a clear message instead of being guessed.
- **Every market stays.** Silver never filters by country: Belgium, the Netherlands,
  Luxembourg and Germany are all kept. The Belgian client scope is applied in Gold,
  where it can be explained and reversed.

Every rule below lives in one place in [`src/retail_pricing/`](../src/retail_pricing/) and the
notebooks only display its results ([02 · Silver pipeline](../notebooks/02_silver_pipeline.ipynb)).

## Published datasets

| Dataset | Rows | Grain | What Silver does |
| --- | ---: | --- | --- |
| `silver_products` | 32,826 | one product reference | Trim text, turn assessed placeholders into NULL, keep the `NAN` brand. |
| `silver_locations` | 1,638 | one location record | Validate coordinates, flag the one duplicated key and coordinates outside Benelux, drop the all-NULL `type`. |
| `silver_prices` | 1,198,547 | one daily price | Keep NULL promo prices; flag a promotion when `promo_price < price`. |
| `silver_nutritionals` | 9,467,978 | product × date × nutrient | Pick one version per grain, standardise units, keep the portion basis. |
| `silver_weekly_products` | 114,517 | one weekly product | Primary values win; fill gaps from `products` only when shop, country and language agree. |
| `silver_weekly_locations` | 1,230 | one weekly location | Decode shop types; add location type and Belgian region; never overwrite primary geography. |
| `silver_weekly_prices` | 19,075,099 | price observation in a product × store × week | Remove 142,972 exact duplicates only; keep every different price. |
| `silver_product_category_paths` | 154,760 | one category path of one product | Parse the JSON paths, flag structural problems. |
| `silver_product_primary_categories` | 167,077 | one product identity | One English FMCG category decision, with the reason. |

Every table carries `shop` (the stable code) and `shop_name` (the reporting label).

## Retailer codes and names

All nine `shop` codes in Raw are written backwards. Decoding is a reviewed reversal,
applied once at the Raw-to-Silver boundary by
[`reference.py`](../src/retail_pricing/reference.py); unknown codes stop the build, and
already-decoded codes pass through unchanged. Retailer names are a separate reviewed
attribute so codes stay stable keys.

| Raw code | Silver `shop` | `shop_name` | Basis for the name |
| --- | --- | --- | --- |
| `idla` | `aldi` | Aldi | decoded code |
| `frc` | `crf` | Carrefour | Carrefour product text and navigation signature |
| `lld` | `dll` | Delhaize | source text names Delhaize; AD / Proxy store formats |
| `plc` | `clp` | Colruyt Lowest Prices | location and assortment evidence |
| `gc` | `cg` | Collect&Go | **reviewed attribution**, not confirmed by a code book |
| `ha` | `ah` | Albert Heijn | decoded code |
| `obmuj` | `jumbo` | Jumbo | decoded code |
| `ldil` | `lidl` | Lidl | decoded code |
| `raps` | `spar` | SPAR | decoded code |

Every value of every Raw source was grouped, not sampled, so the nine codes are the
complete domain. `cg` is the one soft point: Collect&Go descriptions appear under
`clp`, but Colruyt assortment and store overlap support the reviewed label.

Country and retailer coverage differ by table, which matters when comparing
retailers: weekly prices are Belgian only (`aldi`, `clp`, `crf`, `dll`); nutrition
covers `ah` (Belgium and the Netherlands), `crf`, `clp`, `cg`, `dll` and `jumbo`
(Netherlands); daily prices cover `clp`, `crf`, `cg`, `dll` and `jumbo`.

## Products and locations

- **Placeholders.** `""`, `null`, `none`, `n/a` and `#N/A` become NULL in every text
  column. Three descriptive columns also have placeholders of their own, found by
  counting whole-value matches in Raw: `name` (`not provided`, 161 weekly products),
  `brand` (`not provided`, `no brand`, `unknown`, `*`, `.`, `geen`; 736) and
  `description` (`x`, `*`, `0`, `-`, `.`; 4,772). They become NULL only when they
  are the entire value, so a description such as `*mag in vaatwasser` is untouched.
  `NAN` and `NA` are real brands and codes and are kept.
- **Product references.** Weekly products are primary. The products reference fills a
  missing name, brand, description or category only when shop, country and language
  match, and never overwrites a value. In this snapshot 9,406 products match safely
  and nothing needed filling.
- **Locations.** Two records share one `(shop, id)` key; both stay, both are flagged
  and neither is used to fill a weekly location. 40 postcodes and 18 coordinate pairs
  are missing. Four records carry postcodes that do not fit their stated country (for
  example `68234` under `be`, a Swedish Filipstad location); they are kept and
  documented, not corrected, because the source gives no better answer.
- **Weekly locations.** Nine reversed shop-type labels (794 rows) are decoded with the
  raw label and a `is_shop_type_reversed` flag kept; 436 rows have no type. Geography
  is filled from the locations reference only when compatible, which never happened
  here. `country_code` stays NULL for all 1,230 rows: no country is inferred from the
  retailer.
- **Geography is flagged, never filtered.** `dq_outside_benelux` marks coordinates
  outside the Belgium / Netherlands / Luxembourg bounding box; the Swedish Filipstad
  record is the only one, in both location tables, and it stays in Silver.
- **Location type and region.** `location_type` is *National price (online)* for a
  `Default` / base context that has no address or coordinates (6 of 1,230; a reviewed
  reading, not a source fact) and *Store* otherwise. `region` comes from Belgian
  postcode ranges only: 1000–1299 Brussels, 1300–1499 Wallonia, 1500–3999 Flanders,
  4000–7999 Wallonia, 8000–9999 Flanders (Brussels 111, Flanders 716, Wallonia 376).
  The 21 stores without a Belgian four-digit postcode keep a NULL region.

## Prices

- **Daily prices** (`prices`) keep NULL promo prices under the source convention that
  NULL means no active promotion; 6,764 rows are promotions. Currency (`eur`) and unit
  (`su`) exist only here. `su` does not define a pack size, so no unit price (€/kg or
  €/l) can be derived.
- **Weekly prices** collapse only rows that are identical in every field. Different
  prices for the same product, store and week are all kept: 751,824 rows in 375,912
  weekly grains carry more than one version. The source has no timestamp within the
  week, so Silver cannot say which price was the shelf price. 10,916 rows have a promo
  price above the regular price and are flagged.
- Weekly prices carry no currency, unit or country of their own.

## Nutrition

- **One version per grain.** 100 product-date grains have conflicting labels. The
  version with most numeric nutrients, then most nutrients, then most units is kept; 91
  remain tied and are broken by a payload hash. That is reproducible, not proven true;
  every version stays in Raw.
- **Units.** 14 nutrients, 20 source name/unit pairs. Fourteen already use the intended
  unit (`g`, `kJ`, `kcal`); six are incompatible (for example energy in `g`, salt in
  `ml`). Those 70 rows keep their source value, and their canonical value is NULL.
  Nothing is converted, because no valid conversion exists in the data.
- **Basis.** Per-100 g and per-100 ml stay distinct (8,988,998 and 478,980 rows).
- **Plausibility.** 225 rows across ten products report more nutrient mass than a 100 g
  portion; they are flagged. Energy in kJ is compared with kcal × 4.184 in the same
  label with a 2% / 1 kJ tolerance; 1,081,872 pairs were comparable and none mismatch.
  No negative values occur.

## Categories and the English FMCG category

Retailer category paths are structured evidence, not a taxonomy: 6,926 products have
several paths, many are only prefixes of each other, and 3,183 Carrefour
paths start with site-navigation labels. The child table keeps every path and flags duplicates,
prefixes, repeated labels and navigation.

The FMCG category is decided once, in Silver, and consumed by Gold (`fmcg_v3`):

1. **Category paths decide first.** Normalised Dutch merchandise labels are matched
   against a versioned rule set
   ([`fmcg_taxonomy.py`](../src/retail_pricing/fmcg_taxonomy.py)); other languages abstain.
   Structural noise (navigation, ancestors, duplicates, promotions) is skipped, and
   documented boundaries are resolved inside a path (pet or baby use over ingredients,
   frozen over departments, alcohol-free beverages, canned versus fresh fruit, eggs over
   the baking department). Paths that agree give one category; paths that disagree give
   *Other / Unclassified* with `conflicting_evidence`. No first-path or majority rule.
2. **The product name is the backup.** When the paths cannot decide (none, unmapped or
   unusable) or disagree, the words of the Dutch name can:
   [`fmcg_name_rules.py`](../src/retail_pricing/fmcg_name_rules.py) lists 501 words such
   as *tandpasta*, *lego* or *pedigree*. Exactly one category must be named, and for
   disagreeing paths it must be one of the competing candidates. A name never overrides
   a single agreeing path.
3. **Otherwise the product abstains** with a recorded reason. No model, translation or
   fuzzy matching runs in the pipeline.

**Where the name vocabulary comes from.** It is mined once, offline, from the products
whose category the paths already decided: a word is kept when at least 20 of them
carry it and at least 98% agree on one category, minus a short reviewed list of generic
or retailer words. The result is committed as an explicit list, so the Silver build
stays deterministic and every decision names the word that caused it (`name:<word>`).
It was validated on data it had not seen: on a random 50/50 split the words fire on
23.5% of the products with 98.7% agreement with the path decision, and leaving one
retailer out at a time agreement stays between 92% and 99% (see notebook 02, section 9).
Agreement is an estimate, not a verification.

The taxonomy has 20 requested reporting groups plus Non-FMCG / General Merchandise,
Tobacco and Plant-Based Alternatives. Outcomes for the 167,077 identities:

| Status | Identities | Meaning |
| --- | ---: | --- |
| `classified` | 97,116 | 87,535 by category path, 9,450 by name only, 131 paths tied and settled by the name |
| `missing_evidence` | 46,237 | no category path and no name a rule understands |
| `no_supported_match` | 14,760 | paths exist, no rule applies |
| `excluded_evidence` | 6,792 | only unusable paths (navigation, prefixes) |
| `conflicting_evidence` | 2,120 | paths point to different categories |
| `review_required` | 52 | known source contradiction withheld (for example cotton swabs under make-up) |

`fmcg_v2` classified 87,470 (52.4%); `fmcg_v3` classifies 97,116 (58.1%). Of the earlier
decisions, 37 plant-based drinks that Colruyt files under dairy now follow the other
retailers into *Plant-Based Alternatives*, and one Aldi egg product that was filed under
*Bakery* now abstains; every other decision is unchanged.

Path rules are added one exact label at a time, each tested alone on the real paths:
it must classify more products and change none that were already classified.
`fmcg_v2` added 13 labels (for example *Honing*, *Confituur*, *Snoep en kauwgom*) and
`fmcg_v3` added *Verse eieren*, *Plantaardig alternatief* and *Tandpasta*. Broad
department labels (*Kruidenierswaren*, *Dranken*, *Ontbijt*, *Wereldkeuken*,
*Conserven*, *Warme dranken*) were rejected on purpose: they mix categories and would
have turned thousands of correct decisions into conflicts. Bare labels that only look
pure because of how one retailer files its wines (*Frankrijk*, *Online exclusiviteiten*)
are also left to a person: notebook 02 lists them with their support.

**Known boundary cases.** Aldi files diapers under *Papierproducten* (household paper),
so five of its diapers sit in *Household Paper & Supplies* rather than *Baby Care*, and
retailers split the same personal-care brand between *Personal Care* and *Beauty &
Cosmetics*. These are documented, not corrected.

Coverage is not accuracy: it depends on how much category evidence each retailer's
website exposes, not on retailer quality. `classified` means rule-supported, not
verified per product. [Improving the FMCG category](#improving-the-fmcg-category) lists
the next steps for the remaining gap.

## Improving the FMCG category

Today 79.0% of price rows (68.7% of described products) carry a category. Category paths
decide first; when they cannot, the product name does (about 9,500 products). The rest has
no path and no usable name (6.6% of rows, including the inferred products), no rule that
applies (11.6%), or only unusable or conflicting paths (2.9%). What worked, and what to
do next:

1. **Keep the decision in Silver, deterministic and versioned.** New rules go into
   [`fmcg_taxonomy.py`](../src/retail_pricing/fmcg_taxonomy.py) or
   [`fmcg_name_rules.py`](../src/retail_pricing/fmcg_name_rules.py) and change `MAPPING_VERSION`.
2. **Add path rules one exact label at a time.** Test each on the real category paths and
   keep it only if it classifies more products and changes none of the classified ones.
   Department labels such as *Kruidenierswaren* mix categories and are rejected.
3. **Use the product name as a second, independent signal.** The vocabulary is mined from
   the products the paths already classified (no hand labelling), kept only when frequent
   and pure, and checked on unseen data: a random hold-out and leave-one-retailer-out,
   92-99% agreement with the paths. It only speaks when the paths cannot.
4. **Next: a person reviews the unmapped labels** that notebook 02 lists (bare labels
   such as *Frankrijk* look pure only because of one retailer's filing), and Aldi, where
   only 23% of products are classified, gets brand and name rules.
5. **Measure real precision** with a small hand-labelled sample per retailer (about 300
   products). Agreement with the paths is only a proxy.
6. **Use external taxonomies (GS1 GPC, Open Food Facts) and classification models only to
   propose candidates.** A person reviews them and turns the good ones into explicit
   rules; nothing is assigned automatically.
7. **Report coverage and precision together, per retailer**, and keep the reason for
   every abstention. Coverage is not accuracy.

## Publication and checks

- **Stage, validate, move, manifest last.** All nine files are built in a staging
  directory and pass their own contracts and the cross-table contracts (canonical shop
  domain and names, no row multiplication, complete location reference, consistent
  product context). Files are then moved into `data/clean/` and `manifest.json` is
  written last. Readers trust only a complete manifest whose `code_sha256` equals the
  fingerprint of the current source code, so stale Silver is refused rather than read.
- **Regression.** `parity` compares row counts, columns, a content fingerprint and key
  metrics of every layer with a reviewed baseline; see the [README](../README.md).
- Single-user workflow: concurrent builds are not excluded, and an interrupted
  publication leaves no manifest, so the next run rebuilds.

The Open Food Facts taxonomy and two published classification models were evaluated as
possible dictionaries or review candidates; none is used in the pipeline. Translating
product names and fuzzy product matching are deferred: same SKU is not same family, and
brand, variant and pack size would have to be validated first.

## Limits to remember

Weekly prices have no unit, currency or pack size; observed prices are not sales;
category memberships and descriptions are snapshot references without validity dates;
valid coordinates do not prove a correct store location; and price observations
alone cannot show promotion effectiveness.
