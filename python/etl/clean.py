"""
clean.py
--------
Cleans and standardizes the raw sales DataFrame produced by ingest.py.
Handles nulls, type casting, duplicates, and basic outlier flagging.
"""

import pandas as pd


# Revenue values beyond this multiplier of the IQR fence are flagged
OUTLIER_REVENUE_ZSCORE = 3.0


def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize raw sales data.

    Steps
    -----
    1. Drop exact duplicate rows.
    2. Enforce correct dtypes.
    3. Standardize string columns (strip whitespace, title case).
    4. Validate numeric ranges (price, quantity, discount, attainment).
    5. Flag statistical outliers in revenue.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from ingest.load_sales_data().

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with an added 'revenue_outlier' boolean column.
    """
    original_len = len(df)
    df = df.copy()

    # 1. Drop exact duplicates
    df = df.drop_duplicates()
    dropped = original_len - len(df)
    if dropped:
        print(f"[clean] Dropped {dropped} duplicate rows.")

    # 2. Enforce dtypes
    df["date"] = pd.to_datetime(df["date"])
    df["unit_price"]    = pd.to_numeric(df["unit_price"],    errors="coerce")
    df["quantity"]      = pd.to_numeric(df["quantity"],      errors="coerce").astype("Int64")
    df["discount_pct"]  = pd.to_numeric(df["discount_pct"],  errors="coerce")
    df["revenue"]       = pd.to_numeric(df["revenue"],       errors="coerce")
    df["quota"]         = pd.to_numeric(df["quota"],         errors="coerce")
    df["quota_attainment"] = pd.to_numeric(df["quota_attainment"], errors="coerce")

    # 3. Standardize string columns
    str_cols = ["territory", "province", "city", "clinic_name",
                "sales_rep", "product_name", "product_category", "animal_segment"]
    for col in str_cols:
        df[col] = df[col].str.strip().str.title()

    df["sale_id"]   = df["sale_id"].str.strip().str.upper()
    df["clinic_id"] = df["clinic_id"].str.strip().str.upper()

    # 4. Validate numeric ranges
    invalid_price    = df["unit_price"] <= 0
    invalid_qty      = df["quantity"] <= 0
    invalid_discount = (df["discount_pct"] < 0) | (df["discount_pct"] > 1)
    invalid_revenue  = df["revenue"] <= 0

    for label, mask in [
        ("unit_price <= 0",      invalid_price),
        ("quantity <= 0",        invalid_qty),
        ("discount_pct out of [0,1]", invalid_discount),
        ("revenue <= 0",         invalid_revenue),
    ]:
        count = mask.sum()
        if count:
            print(f"[clean] WARNING: {count} rows with {label} — setting to NaN.")
            df.loc[mask, label.split()[0]] = pd.NA

    # 5. Flag revenue outliers (z-score method)
    mean_rev = df["revenue"].mean()
    std_rev  = df["revenue"].std()
    df["revenue_outlier"] = (
        (df["revenue"] - mean_rev).abs() > OUTLIER_REVENUE_ZSCORE * std_rev
    )
    outlier_count = df["revenue_outlier"].sum()
    if outlier_count:
        print(f"[clean] Flagged {outlier_count} revenue outliers (z > {OUTLIER_REVENUE_ZSCORE}).")

    # Drop rows where critical fields are null after cleaning
    before = len(df)
    df = df.dropna(subset=["revenue", "quota", "sales_rep", "territory"])
    removed = before - len(df)
    if removed:
        print(f"[clean] Removed {removed} rows with null critical fields.")

    print(f"[clean] Clean complete: {len(df):,} rows remaining.\n")
    return df


if __name__ == "__main__":
    from pathlib import Path
    from ingest import load_sales_data

    df_raw   = load_sales_data(Path("data/sample_sales_data.csv"))
    df_clean = clean_sales_data(df_raw)
    print(df_clean.dtypes)
    print(df_clean.head())
