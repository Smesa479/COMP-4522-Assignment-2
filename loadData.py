"""
Data loading and ETL script (Pandas-only route).
Reads the four supplied CSV files, standardizes keys to prevent join failures,
performs data quality checks, and transforms the data into an aggregated dataset.
"""
# Added by: Francis Ang (201736684)
# This file has three functions:
# load_tables - reads the four CSV files as text (etl/etl_pipeline.py uses this one).
# find_data_folder - finds the folder with the CSV files.
# load_and_clean_data - our group version (load, clean and join). It gives the same
# 19,604 valid sales as etl/etl_pipeline.py, so we use it to double check.
import warnings
from pathlib import Path

import pandas as pd

# Suppress the specific Pandas4Warning if it still appears based on pandas version
warnings.simplefilter(action='ignore', category=Warning)

TABLES = ["store", "employee", "product", "sales_performance"]


# Added by: Francis Ang (201736684)
# find_data_folder: this finds where the four CSV files are.
# Before, the script only looked in the folder we ran it from, so it crashed
# when we ran it from somewhere else. Now it looks next to this file.
# It checks data/raw first, then the same folder as this file.
def find_data_folder():
    here = Path(__file__).resolve().parent
    for folder in (here / "data" / "raw", here):
        if all((folder / f"{t}.csv").exists() for t in TABLES):
            return folder
    raise FileNotFoundError(
        f"Could not find store.csv, employee.csv, product.csv and sales_performance.csv in "
        f"{here / 'data' / 'raw'} or {here}. Put the four CSV files in one of those folders."
    )


# Added by: Francis Ang (201736684)
# load_tables: this only loads the four CSV files and gives them back as text.
# We read everything as text (dtype=str) so the bad values like 'a', 'A69'
# and 'C3PO' are not lost. The checks in etl/etl_pipeline.py need to see them.
def load_tables(folder=None):
    folder = Path(folder) if folder else find_data_folder()
    tables = {}
    for table in TABLES:
        path = folder / f"{table}.csv"
        if not path.exists():
            raise FileNotFoundError(f"Required CSV file not found: {path.resolve()}")
        tables[table] = pd.read_csv(path, dtype=str)
    return tables


# Changed by: Francis Ang (201736684)
# folder=None now (it was folder="."). If no folder is given, we use
# find_data_folder() so the CSV files are found automatically.
def load_and_clean_data(folder=None):
    folder = Path(folder) if folder else find_data_folder()
    tables = {}

    # 1. Load Data & Clean Formatting
    for table in TABLES:
        path = folder / f"{table}.csv"
        if not path.exists():
            raise FileNotFoundError(f"Required CSV file not found: {path.resolve()}")

        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()

        # Updated to include 'str' explicitly to resolve Pandas4Warning
        for col in df.select_dtypes(include=['object', 'str']).columns:
            df[col] = df[col].astype(str).str.strip()

        tables[table] = df

    store_df = tables["store"]
    employee_df = tables["employee"]
    product_df = tables["product"]
    sales_df = tables["sales_performance"]

    # Force all Keys to strings
    store_df['Store_ID'] = store_df['Store_ID'].astype(str)
    employee_df['Employee_ID'] = employee_df['Employee_ID'].astype(str)
    employee_df['Store_ID'] = employee_df['Store_ID'].astype(str)
    product_df['Product_ID'] = product_df['Product_ID'].astype(str)
    sales_df['Employee_ID'] = sales_df['Employee_ID'].astype(str)
    sales_df['Product_ID'] = sales_df['Product_ID'].astype(str)

    # --- 2. Data Inspection & Quality Checks ---

    # Changed by: Francis Ang (201736684)
    # initial_sales_count: we now count the sales here, before any filter.
    # Before, we counted after the foreign key filter, so the report said only
    # 3 were discarded. The real number is 396 (393 more were dropped earlier).
    initial_sales_count = len(sales_df)

    # Added by: Francis Ang (201736684)
    # Primary keys must be unique: keep the first occurrence of each key.
    store_df = store_df.drop_duplicates(subset=['Store_ID'])
    employee_df = employee_df.drop_duplicates(subset=['Employee_ID'])
    product_df = product_df.drop_duplicates(subset=['Product_ID'])

    # Added by: Francis Ang (201736684)
    # Foreign keys must exist: an employee needs a valid store; a sale needs a valid employee and product.
    # Changed by: Francis Ang (201736684)
    # Added .copy() at the end of the sales filter. Without it pandas can give a
    # SettingWithCopy warning when we change the columns below.
    employee_df = employee_df[employee_df['Store_ID'].isin(store_df['Store_ID'])]
    sales_df = sales_df[
        sales_df['Employee_ID'].isin(employee_df['Employee_ID']) &
        sales_df['Product_ID'].isin(product_df['Product_ID'])
    ].copy()
    # Added by: Francis Ang (201736684)
    # after_fk: number of sales left after the foreign key filter.
    # We use it in the report line below to split the discards into two groups.
    after_fk = len(sales_df)

    # Added by: Francis Ang (201736684)
    # Convert the measures to numbers; anything that is not a number becomes NaN.
    sales_df['Revenue'] = pd.to_numeric(sales_df['Revenue'], errors='coerce')
    sales_df['Units_Sold'] = pd.to_numeric(sales_df['Units_Sold'], errors='coerce')
    sales_df['Effort_Hours'] = pd.to_numeric(sales_df['Effort_Hours'], errors='coerce')

    # Added by: Francis Ang (201736684)
    # Business rules from the assignment: Revenue >= 0, Units_Sold >= 1, Effort_Hours >= 0.
    sales_df = sales_df[
        (sales_df['Revenue'] >= 0) &
        (sales_df['Units_Sold'] >= 1) &
        (sales_df['Effort_Hours'] >= 0)
    ]
    sales_df = sales_df.dropna(subset=['Revenue', 'Units_Sold', 'Effort_Hours'])

    # Changed by: Francis Ang (201736684)
    # The report line now shows the full 396 and splits it in two:
    # 393 sales with an unknown employee/product, and 3 with a bad
    # Revenue / Units_Sold / Effort_Hours value.
    discarded_sales = initial_sales_count - len(sales_df)
    print(f"Data Quality Report: Discarded {discarded_sales} invalid sales records "
          f"({initial_sales_count - after_fk} with an unknown employee/product, "
          f"{after_fk - len(sales_df)} with an invalid Revenue/Units_Sold/Effort_Hours value).")

    # --- 3. ETL and Data Transformation ---

    merged_df = sales_df.merge(employee_df, on='Employee_ID', how='inner')
    merged_df = merged_df.merge(product_df, on='Product_ID', how='inner')
    merged_df = merged_df.merge(store_df, on='Store_ID', how='inner')

    return merged_df


# Changed by: Francis Ang (201736684)
# Main block: it also prints how many rows each CSV file has, so we can see
# the files were loaded (store 11, employee 100, product 42, sales 20000).
# The result is now saved in the output folder, not in the folder we ran
# the script from. For the full ETL run python etl/etl_pipeline.py.
if __name__ == "__main__":
    for name, df in load_tables().items():
        print(f"{name}: {df.shape[0]} rows, {df.shape[1]} columns")
    final_dataset = load_and_clean_data()
    print(f"Joined dataset: {final_dataset.shape[0]} rows, {final_dataset.shape[1]} columns")
    out_dir = Path(__file__).resolve().parent / "output"
    out_dir.mkdir(exist_ok=True)
    final_dataset.to_csv(out_dir / "transformed_mining_data.csv", index=False)
    print("Exported to 'output/transformed_mining_data.csv'.")
