"""
ingest.py
---------
Loads raw sales CSV data and performs basic structural validation.
"""

import pandas as pd
from pathlib import Path


REQUIRED_COLUMNS = [
    "sale_id", "date", "month", "quarter", "territory", "province",
    "city", "clinic_id", "clinic_name", "sales_rep", "product_name",
    "product_category", "animal_segment", "unit_price", "quantity",
    "discount_pct", "revenue", "quota", "quota_attainment",
]


def load_sales_data(filepath: str | Path) -> pd.DataFrame:
    """
    Load sales data from a CSV file.

    Parameters
    ----------
    filepath : str or Path
        Path to the raw sales CSV file.

    Returns
    -------
    pd.DataFrame
        Raw sales data as a DataFrame.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If required columns are missing.
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"Data file not found: {filepath}")

    df = pd.read_csv(filepath, parse_dates=["date"])

    # Validate required columns
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    print(f"[ingest] Loaded {len(df):,} rows from '{filepath.name}'")
    print(f"[ingest] Date range: {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"[ingest] Columns: {list(df.columns)}\n")

    return df


if __name__ == "__main__":
    # Run from project root: python -m python.etl.ingest
    data_path = Path("data/sample_sales_data.csv")
    df = load_sales_data(data_path)
    print(df.head())
