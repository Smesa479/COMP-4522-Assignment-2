"""
Data loading and ETL script (Pandas-only route).
Reads the four supplied CSV files, standardizes keys to prevent join failures,
performs data quality checks, and transforms the data into an aggregated dataset.
"""
import pandas as pd
from pathlib import Path
import warnings

# Suppress the specific Pandas4Warning if it still appears based on pandas version
warnings.simplefilter(action='ignore', category=Warning)

TABLES = ["store", "employee", "product", "sales_performance"]

def load_and_clean_data(folder="."):
    folder = Path(folder)
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
    
    store_df = store_df.drop_duplicates(subset=['Store_ID'])
    employee_df = employee_df.drop_duplicates(subset=['Employee_ID'])
    product_df = product_df.drop_duplicates(subset=['Product_ID'])

    employee_df = employee_df[employee_df['Store_ID'].isin(store_df['Store_ID'])]
    sales_df = sales_df[
        sales_df['Employee_ID'].isin(employee_df['Employee_ID']) & 
        sales_df['Product_ID'].isin(product_df['Product_ID'])
    ]

    sales_df['Revenue'] = pd.to_numeric(sales_df['Revenue'], errors='coerce')
    sales_df['Units_Sold'] = pd.to_numeric(sales_df['Units_Sold'], errors='coerce')
    sales_df['Effort_Hours'] = pd.to_numeric(sales_df['Effort_Hours'], errors='coerce')

    initial_sales_count = len(sales_df)
    sales_df = sales_df[
        (sales_df['Revenue'] >= 0) & 
        (sales_df['Units_Sold'] >= 1) & 
        (sales_df['Effort_Hours'] >= 0)
    ]
    sales_df = sales_df.dropna(subset=['Revenue', 'Units_Sold', 'Effort_Hours'])
    
    discarded_sales = initial_sales_count - len(sales_df)
    print(f"Data Quality Report: Discarded {discarded_sales} invalid sales records.")

    # --- 3. ETL and Data Transformation ---
    
    merged_df = sales_df.merge(employee_df, on='Employee_ID', how='inner')
    merged_df = merged_df.merge(product_df, on='Product_ID', how='inner')
    merged_df = merged_df.merge(store_df, on='Store_ID', how='inner')

    return merged_df

if __name__ == "__main__":
    final_dataset = load_and_clean_data()
    print(f"Aggregated Dataset: {final_dataset.shape[0]} rows, {final_dataset.shape[1]} columns")
    final_dataset.to_csv("transformed_mining_data.csv", index=False)
    print("Exported to 'transformed_mining_data.csv'.")