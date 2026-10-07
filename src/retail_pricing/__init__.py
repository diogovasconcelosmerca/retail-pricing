"""Reproducible Raw -> Silver -> Gold pipelines for a retail pricing case."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_DIR = PROJECT_ROOT / "data" / "clean"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

# Published Silver file names, one per dataset. Order is the manifest order.
SILVER_FILES = {
    name: f"silver_{name}.parquet"
    for name in (
        "products",
        "locations",
        "prices",
        "nutritionals",
        "weekly_products",
        "weekly_locations",
        "weekly_prices",
        "product_category_paths",
        "product_primary_categories",
    )
}
