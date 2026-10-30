"""
Data loading script (Pandas-only route).
Reads the four supplied CSV files as strings so malformed values can be inspected
before numeric/date conversion.
"""
import pandas as pd
from pathlib import Path

TABLES = ["store", "employee", "product", "sales_performance"]


def load_tables(folder="."):
    folder = Path(folder)
    tables = {}
    for table in TABLES:
        path = folder / f"{table}.csv"
        if not path.exists():
            raise FileNotFoundError(
                f"Required CSV file not found: {path.resolve()}"
            )
        tables[table] = pd.read_csv(path, dtype=str)
    return tables


# if __name__ == "__main__":
#     for name, df in load_tables().items():
#         print(f"{name}: {df.shape[0]} rows, {df.shape[1]} columns")
