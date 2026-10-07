# Handoff for Claude Code — retail-pricing

Working notes for Claude Code, kept in the repository.
Written 2026-09-29 after an audit done from a remote session that could not run
Silver/Gold builds (3 GB RAM). You run on the user's Windows machine: you CAN run
the full pipeline. Always verify by running it.

## Context
- Business case (Data & Insights Engineer – Customer Solutions), kept as a
  portfolio project, so the structure must look clean.
- Flow: PostgreSQL → Raw (data/raw) → Silver (data/clean) → Gold (data/gold)
  → Power BI PBIP/TMDL (semantic_model/) → client report (Belgium scope).
- Run: `uv run python -m retail_pricing.pipelines.full_pipeline` (Raw reused).
  Close Power BI first: the pipeline deletes data/clean and data/gold.
- The user writes in European Portuguese; answer in PT-PT, code/docs in English.

## Verified state before the handoff (2026-09-29)
- 98 tests pass, ruff clean, parity 141/141 PASS, notebooks 02/03 run clean.
- Silver changes that are really in code: shop codes decoded centrally in
  `reference/shops.py` (idla→aldi, frc→crf, plc→clp, lld→dll, obmuj→jumbo,
  ha→ah, gc→cg, ldil→lidl, raps→spar); `shop_name` added in every Silver
  table and carried into dim_shop; new `silver_product_primary_categories`
  (one FMCG decision per daltix_id+shop+country) attached to dim_product.
- Cleanup already done: line endings normalised + `.gitattributes`; three
  CR-only files rebuilt; `code_fingerprint` ignores CRLF/LF; stray audit txt →
  docs/evidence/; notebook 03 got section 12 (in-memory Gold == published Gold,
  NOT yet executed — first run happens on Windows); README counts updated.
- Current sizes: fact_weekly_prices 18,699,187; fact_product_nutrition
  9,467,978; dim_product 153,528; dim_category 8,671; bridge 104,185;
  dim_location 1,230; dim_shop 7.

## Step 0 (always first)
1. Run the full pipeline, then Restart+Run All notebooks 02 and 03.
2. Commit this checkpoint before any refactor.
3. Record sha256 of every Parquet in data/clean and data/gold → baseline.
   Any refactor must reproduce IDENTICAL files (or explain every difference).

## DECIDED PLAN (user approved 2026-09-29) — execute in this order

Work in phases; after each phase: full pipeline, tests, ruff, notebooks, and show
the user a short summary. Do NOT commit during phases A–C. Commit only in Phase D, after the
user approves the final state.

### Phase A — cosmetic / lean (outputs must stay IDENTICAL)
Record sha256 of every Parquet in data/clean and data/gold BEFORE starting.
A1. Flat package, ~13 modules (option B), move code without rewriting logic:
    `__init__.py` (paths once) · `raw.py` · `reference.py` · `fmcg_taxonomy.py`
    · `quality.py` · `silver_products.py` (products, weekly_products, category
    paths, primary categories) · `silver_locations.py` · `silver_prices.py` ·
    `silver_nutrition.py` · `gold.py` (dims, category model, facts) ·
    `gold_validation.py` · `pipeline.py` (one CLI:
    `python -m retail_pricing.pipeline {raw,silver,gold,parity,all}`) · `parity.py`.
A2. Parity: shrink parity_check.py (1,345 lines of hand-written expected
    values) to a small module that compares row counts + file hashes + key
    metrics against `parity_baseline.json`. Must detect the same failures;
    accept only if results are identical.
A3. Silver publication: replace lock + rollback + PublicationRecoveryError with
    the simpler robust pattern: build into a staging dir, validate, move files,
    write manifest LAST (readers trust only a complete manifest whose
    code_sha256 matches). Keep the code-fingerprint guard.
A4. Notebooks = show the logic, lightest clean form: markdown (what + why,
    FMCG-framed) → call the src function → one evidence cell → conclusion.
    Remove notebook 03's re-implementation of Gold; call src builders step by
    step; keep section 12 parity only if still meaningful (it becomes trivial
    once the notebook calls src — then drop it). 02: explicit sections for shop
    code decoding (Raw→Silver table), shop names, FMCG classification.
    01: move scripts/profile_category_structure.py content into it; delete
    scripts/. NEVER delete saved outputs: re-execute so outputs are fresh.
A5. Docs: README ~150 lines (what, architecture, how to run, core decisions);
    docs/ reduced to 3 files: silver-data-quality.md, gold-model.md,
    semantic-model.md. Every decision framed in FMCG/retail terms. Merge the
    content of the 9 current review docs + docs/evidence into these; remove
    the rest.
A6. Tests 11 files → ~4 (test_raw, test_reference, test_silver, test_gold).

### Phase B — engineering (outputs change; update baselines deliberately)
B1. Inferred product members (Gold): every unmatched (shop, source_daltix_id)
    in weekly prices gets its own deterministic product_key, name
    "Unmatched product (<shop> <id>)", country 'be' (from the Belgian
    weekly-pricing process), enrichment NULL, `is_inferred_product = true`,
    FMCG status `missing_evidence`. Keep -1 only for truly missing ids (expect
    0). 476,695 rows / 11,011 ids today. Validate id stability: one id never
    crosses shops (verified 0).
B2. Belgium client mart (Gold): Silver keeps ALL markets (BE/NL/LU/DE).
    Gold publishes the Belgian mart: fact_weekly_prices (already 100% BE),
    fact_product_nutrition filtered to product country = 'be' (drops 5.13M NL
    rows: AH-NL + Jumbo), dimensions reduced to members referenced by the facts
    (+ unknown). Consequence: dim_location drops from 1,230 to the 16 locations
    actually priced; Filipstad disappears naturally.
B3. Geography flag (Silver, not a report filter): add `dq_outside_benelux`
    (coordinates outside BE/NL/LU bounding box) to silver_locations and
    silver_weekly_locations; flag only, never delete.
B3b. Geography model for the Belgian mart. The 16 priced locations are:
    3 national "Default/base" price contexts without coordinates (aldi, crf,
    dll) + 13 physical stores (clp 4: Halle, Ans, Anderlecht, Beveren; crf 9:
    Mont-St-Jean, Drogenbos, Waterloo, Berchem-Ste-Agathe, Ixelles, Wilrijk,
    Froissart, Gerpinnes, Wemmel). Add deterministic attributes (Silver
    standardisation from a small reference, carried to dim_location):
    `location_type` = 'National price (online)' | 'Store', and `region` =
    Brussels | Flanders | Wallonia from Belgian postcode ranges (1000–1299
    Brussels; 1300–1499 Wallonia; 1500–3999 Flanders; 4000–7999 Wallonia;
    8000–9999 Flanders). Map = 13 stores only; never present it as national
    coverage. Regional page = Carrefour national price vs its 9 stores, and
    dispersion across the 4 Colruyt stores.
B4. FMCG category to Power BI: classification already lives in Silver
    (silver_product_primary_categories) and is on Gold dim_product. Missing:
    columns in Product.tmdl (Primary FMCG Category, Classification Status).
    Then make the FMCG category the report's category slicer and stop loading
    Category + Product Category (bridge) into Power BI → removes the
    bidirectional relationship. Keep bridge/dim_category in Gold as evidence.
    Improve coverage in `fmcg_taxonomy.py` rules: 14.6% of weekly rows are
    `no_supported_match`; Aldi only 43.7% classified.
B5. DAX: assortment measures are weekly (CurrentWeek-7). Add true monthly
    versions or keep weekly visuals. Regional pricing: only clp (4 locations)
    and crf (10) have >1 priced location; aldi and dll are 1 location each.

### Phase C — DAX review (68 measures: 41 Weekly Prices, 27 Product Nutrition)
Evidence: weekly rows per retailer-week vary a lot (crf 77.6k–139k, aldi
1.3k–2.9k) → observed churn partly reflects scraping coverage. Nutrition has
~12.5 observation dates per product-nutrient (174.8 rows/product).
C1. Remove from the client model (redundant/misleading): Minimum/Maximum
    Effective Price, Effective Price Range, 4W/13W Rolling Average Price,
    13W Weekly Average Price Volatility %, Average Discount Amount,
    Same-Product Location Price Spread (keep Dispersion %), Assortment WoW %,
    Non-Numeric Nutrition Rate, Negative Nutrient Value Rate, Energy Mismatch
    Rate, Average Absolute Energy Difference kJ (all 0 or mix-driven).
C2. Move to a hidden "99 Data Quality" folder: Promotion Conflict Rate,
    Intraweek Variation Rate, Effective Price Variation Rate, remaining
    nutrition DQ rates.
C3. Fix: Earliest/Latest Price Observation Date return a DATE (not FORMAT
    text); Matched YoY description says "same ISO week" but code uses a
    364-day (52-week) lag — fix the description; harmonise WoW/YoY match
    pattern (both NATURALINNERJOIN, both >0); WoW Price Change Frequency uses
    NOT ISBLANK while Matched WoW uses >0 — align.
C4. Monthly semantics: assortment measures and Promotion Rate WoW Change are
    weekly (anchor week − 7). Add month versions (products observed in month M
    vs M−1) or keep weekly visuals. Matched YoY on a month axis uses only the
    last week of the month — add a version that pools all matched pairs of all
    weeks in the period.
C5. Nutrition averages are observation-weighted. Add a row-level Gold flag
    `is_latest_observation` (latest download_date per product × nutrient) and
    compute benchmarks on it, equal weight per product.
C6. Nutrition Coverage % breaks under a Date filter (pricing 2019–2020 vs
    nutrition 2020-11-27→2021-02-24): ignore Date on the nutrition side.
C7. Category benchmark measures use the retailer taxonomy
    (HASONEVALUE(Category[Category Key])) and REMOVEFILTERS('Product'); with
    the FMCG category on Product this would also remove the category filter →
    rewrite with REMOVEFILTERS on product identity columns only / ALLEXCEPT
    on Primary FMCG Category.

C8. Power BI safety: the user keeps Power BI Desktop CLOSED while you edit
    TMDL/report files (it overwrites them on save). Before removing any
    measure, column or table (Category, Product Category bridge), grep
    semantic_model/retailpbi.Report/definition/pages/**/visual.json for
    references and list which visuals would break; replace them with the new
    field (e.g. Primary FMCG Category) instead of leaving broken visuals.
    Keep GoldPath unchanged. After the phase, the user opens the .pbip,
    refreshes, and checks cards against Gold: Price Observations
    = fact_weekly_prices rows, Observed Shops = 4, Observed Locations = 16.

### Phase D — final README and commit (after user approval of A–C)
D1. Rewrite README.md from scratch against the FINAL code (≤ ~150 lines),
    written for the reviewer: 1) what the project answers for a
    Belgian FMCG client; 2) architecture diagram Raw → Silver → Gold → Power
    BI; 3) how to run (uv sync, .env, one pipeline command); 4) key numbers
    (rows per Gold table, retailers, weeks, locations); 5) core engineering
    decisions, each framed in FMCG terms (shop decoding, Belgium mart, inferred
    products, FMCG category in Silver, weekly grain, matched like-for-like
    pricing); 6) data limitations / claims we do not make (no sales, no
    €/kg, observed ≠ launch/delisting, 16 price contexts); 7) repo map; 8)
    links to the 3 docs. Every number must come from the final run.
D2. Final gate (pipeline, tests, ruff, notebooks), then `git status` review:
    nothing under data/, no .env, no CLAUDE.md, no caches.
D3. Commit in logical commits, one per phase (A refactor, B data model,
    C semantic model, D docs), clear imperative messages. Show `git log
    --stat` to the user. Do not push unless asked.
