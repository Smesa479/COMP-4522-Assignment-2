#!/usr/bin/env python3
"""
COMP4522 Project - ETL, data quality checks and transformation (pandas only).

What this script does, in plain words
  1. EXTRACT   : loadData.load_tables() reads the 4 CSV files (every value as text).
  2. INSPECT   : runs every required data quality check and counts the problems.
  3. CLEAN     : removes or repairs bad records and writes every one of them to a log.
  4. TRANSFORM : joins Sales -> Employee -> Store and Sales -> Product, adds computed
                 columns and builds aggregated summary tables (all in pandas).
  5. LOAD      : saves the mining dataset, the summaries and the logs to the output/ folder.

Why the data is read as text first
  The files contain deliberately bad values (for example Store_ID = 'a' or Employee_ID = 'C3PO').
  If pandas converted them to numbers immediately it would fail or hide them, so we keep
  everything as text, check it, and only convert after the checks.

Usage (from the project root)
    python etl/etl_pipeline.py
"""
import json
import sys
from pathlib import Path

import pandas as pd

 
# Settings and paths
 
# ROOT = the project folder (this file lives in <ROOT>/etl/, so go up two levels).
ROOT = Path(__file__).resolve().parent.parent
RAW = None                       # folder with the 4 supplied CSV files (found automatically in main)
OUT = ROOT / "output"            # where all results are written

# Make the project folder importable so we can reuse the group's loading script (loadData.py).
sys.path.insert(0, str(ROOT))
from loadData import load_tables, find_data_folder  # noqa: E402

# The four real product categories. Anything else (e.g. 'Other') is treated as invalid.
VALID_CATEGORIES = {"Home", "Food", "Clothing", "Electronics"}
# Used to spot impossible store opening years (a store cannot open in the future).
CURRENT_YEAR = pd.Timestamp.today().year



# Helper tools used by the cleaning code
 
class Log:
    """Notebook of everything we find. Two lists become two CSV files at the end:
       - checks   -> output/quality_checks.csv  (one row per check: how many records were affected)
       - discards -> output/discard_log.csv     (one row per record that was removed or repaired)
       The assignment requires every discarded record to be documented; this is that record."""

    def __init__(self):
        self.checks = []    # one entry per quality check
        self.discards = []  # one entry per discarded / repaired record

    def check(self, table, name, n_bad, action):
        """Record the result of one check: which table, what was checked, how many records
        failed, and what we did about them."""
        self.checks.append(
            {"table": table, "check": name, "records_affected": int(n_bad), "action": action}
        )

    def discard(self, table, keys, reason, action="discarded"):
        """Record each removed/repaired record. `keys` identifies the records (e.g. the IDs).
        `reason` is either one text for all of them, or a Series with one text per record."""
        keys = list(keys)
        reasons = list(reason) if isinstance(reason, pd.Series) else [reason] * len(keys)
        for k, r in zip(keys, reasons):
            self.discards.append(
                {"table": table, "record": str(k), "reason": r, "action": action}
            )


def is_int_text(s):
    """True for every value that is a whole number written as text, e.g. '1023'.
    False for 'a', 'C3PO', '' or '12.5'. (fillna/astype make sure we always get plain True/False.)"""
    return s.str.fullmatch(r"\d+").fillna(False).astype(bool)


def is_num_text(s):
    """True for every value that is a number written as text, e.g. '19.41' or '-259.00'."""
    return s.str.fullmatch(r"-?\d+(\.\d+)?").fillna(False).astype(bool)


def normalise(df):
    """Turn every column into clean text: remove spaces around values, and turn any missing
    value (blank cell, NaN, 'NA', ...) into an empty string ''.
    Reason: the loader may convert blanks to NaN; after this step 'missing' always looks the
    same ('') so our missing-value check cannot miss anything."""
    return df.apply(lambda c: c.fillna("").astype(str).str.strip())


def act(n, text):
    """Wording for the 'action' column: the given text if n > 0 problems, otherwise 'none found'."""
    return text if n else "none found"


 
# 2 + 3. INSPECT and CLEAN
 
def inspect_and_clean(raw):
    """Run all quality checks, remove/repair bad records, log everything.
    Input : dict of the four raw tables (text).
    Output: (dict of cleaned tables with proper types, the Log, number of raw sales rows)."""
    log = Log()
    store, emp, prod, sales = (normalise(raw[k]) for k in ("store", "employee", "product", "sales"))
    # Give every sales row its CSV row number so a discarded sale can be traced back to the file.
    sales.insert(0, "row_id", range(1, len(sales) + 1))
    n0 = len(sales)  # how many sales rows we started with

    # ================= STORE =================
    # Check 1: Store_ID must be present and numeric (it is the primary key).
    bad = ~is_int_text(store.Store_ID)
    log.check("store", "Missing / non-numeric Store_ID", bad.sum(), act(bad.sum(), "discarded"))
    log.discard("store", store.loc[bad, "Store_ID"], "Missing or non-numeric Store_ID")
    # Check 2: primary key uniqueness - a second row with an already-seen Store_ID is a duplicate.
    dup = store.Store_ID.duplicated() & ~bad
    log.check("store", "Duplicate Store_ID (primary key)", dup.sum(), act(dup.sum(), "later duplicates discarded"))
    log.discard("store", store.loc[dup, "Store_ID"], "Duplicate Store_ID (first occurrence kept)")
    store = store[~bad & ~dup].copy()  # keep only the good rows
    # Check 3: Opening_Year must be a believable year (Store_11 says 1492, which is impossible).
    # We do NOT delete the store (it is otherwise a valid row); we only blank the bad year.
    yr = pd.to_numeric(store.Opening_Year, errors="coerce")
    bad_yr = yr.isna() | (yr < 1800) | (yr > CURRENT_YEAR)
    log.check("store", "Implausible Opening_Year (<1800 or >current year)", bad_yr.sum(),
              act(bad_yr.sum(), "Opening_Year set to NULL, store kept"))
    log.discard("store", store.loc[bad_yr, "Store_ID"] + " (" + store.loc[bad_yr, "Store_Name"] + ")",
                "Opening_Year " + store.loc[bad_yr, "Opening_Year"] + " is implausible", "repaired (set to NULL)")
    # Convert to proper numeric types now that the values are validated.
    store["Store_ID"] = store.Store_ID.astype(int)
    store["Opening_Year"] = yr.where(~bad_yr).astype("Int64")  # Int64 allows missing values

    # ================= PRODUCT =================
    # Check 1: valid ID, one of the 4 real categories, and a numeric price.
    # This catches the two junk rows: 'bc31' (Other) and '999' (Other 2).
    bad = ~is_int_text(prod.Product_ID) | ~prod.Category.isin(VALID_CATEGORIES) | ~is_num_text(prod.Base_Price)
    log.check("product", "Non-numeric Product_ID, unknown Category (e.g. 'Other') or invalid Base_Price",
              bad.sum(), act(bad.sum(), "discarded (no sale references them)"))
    log.discard("product", prod.loc[bad, "Product_ID"],
                "Invalid product row (ID/category/price): category '" + prod.loc[bad, "Category"] + "'")
    # Check 2: primary key uniqueness.
    dup = prod.Product_ID.duplicated() & ~bad
    log.check("product", "Duplicate Product_ID (primary key)", dup.sum(), act(dup.sum(), "later duplicates discarded"))
    log.discard("product", prod.loc[dup, "Product_ID"], "Duplicate Product_ID (first occurrence kept)")
    prod = prod[~bad & ~dup].copy()
    prod["Product_ID"] = prod.Product_ID.astype(int)
    prod["Base_Price"] = prod.Base_Price.astype(float)
    # Check 3: a price of zero or less makes no sense.
    nonpos = prod.Base_Price <= 0
    log.check("product", "Base_Price <= 0", nonpos.sum(), act(nonpos.sum(), "discarded"))
    log.discard("product", prod.loc[nonpos, "Product_ID"], "Base_Price <= 0")
    prod = prod[~nonpos].copy()

    # ================= EMPLOYEE =================
    # Remember which Employee_IDs exist in the RAW file. Later we use this to tell apart
    # "this employee never existed" (e.g. 1048) from "this employee was removed by us" (1023).
    raw_emp_nums = set(pd.to_numeric(emp.Employee_ID, errors="coerce").dropna().astype(int))
    # Check 1: DOB and DOJ should be dates. In this file they are corrupted fragments such as
    # '58:57.4', so every row fails. We keep them as text and do not use them in the analysis.
    fmt = "%Y-%m-%d"
    bad_dates = pd.to_datetime(emp.DOB, format=fmt, errors="coerce").isna() | \
        pd.to_datetime(emp.DOJ, format=fmt, errors="coerce").isna()
    log.check("employee", "DOB / DOJ are not valid YYYY-MM-DD dates (corrupted 'mm:ss.f' fragments)",
              bad_dates.sum(), act(bad_dates.sum(), "kept as text, excluded from analysis"))
    # Check 2: Employee_ID must be numeric (catches 'A69').
    bad_emp_id = ~is_int_text(emp.Employee_ID)
    log.check("employee", "Missing / non-numeric Employee_ID", bad_emp_id.sum(), act(bad_emp_id.sum(), "discarded"))
    log.discard("employee", emp.loc[bad_emp_id, "Employee_ID"], "Non-numeric Employee_ID")
    # Check 3: primary key uniqueness.
    dup = emp.Employee_ID.duplicated() & ~bad_emp_id
    log.check("employee", "Duplicate Employee_ID (primary key)", dup.sum(), act(dup.sum(), "later duplicates discarded"))
    log.discard("employee", emp.loc[dup, "Employee_ID"], "Duplicate Employee_ID (first occurrence kept)")
    # Check 4: foreign key - Employee.Store_ID must point to a store that exists (catches Store_ID = 'a').
    store_ids = set(store.Store_ID)
    store_ok = is_int_text(emp.Store_ID) & pd.to_numeric(emp.Store_ID, errors="coerce").isin(store_ids)
    bad_emp_fk = ~bad_emp_id & ~dup & ~store_ok
    log.check("employee", "Store_ID not found in Store (FK)", bad_emp_fk.sum(), act(bad_emp_fk.sum(), "discarded"))
    log.discard("employee", emp.loc[bad_emp_fk, "Employee_ID"],
                "Store_ID '" + emp.loc[bad_emp_fk, "Store_ID"] + "' is not a valid Store (FK violation)")
    emp = emp[~bad_emp_id & ~dup & store_ok].copy()
    emp["Employee_ID"] = emp.Employee_ID.astype(int)
    emp["Store_ID"] = emp.Store_ID.astype(int)

    # ================= SALES_PERFORMANCE =================
    cols = ["Employee_ID", "Product_ID", "Sale_Date", "Units_Sold", "Revenue", "Effort_Hours"]
    # Check 1: missing values - any of the six columns empty.
    blank = (sales[cols] == "").any(axis=1)
    # Check 2: Sale_Date must be a real date in YYYY-MM-DD format (errors='coerce' turns bad ones into NaT).
    dates = pd.to_datetime(sales.Sale_Date, format="%Y-%m-%d", errors="coerce")
    bad_date = dates.isna() & ~blank
    # Check 3: numbers must really be numbers (Units_Sold must be a whole number).
    non_numeric = (~sales.Units_Sold.str.fullmatch(r"-?\d+").fillna(False)  | ~is_num_text(sales.Revenue) 
                   | ~is_num_text(sales.Effort_Hours)) & ~blank
    log.check("sales_performance", "Missing / blank values", blank.sum(), act(blank.sum(), "discarded"))
    log.check("sales_performance", "Unparseable Sale_Date", bad_date.sum(), act(bad_date.sum(), "discarded"))
    log.check("sales_performance", "Non-numeric Revenue/Effort_Hours or non-integer Units_Sold",
              non_numeric.sum(), act(non_numeric.sum(), "discarded"))
    # Now convert the text columns to real numbers / dates (anything unconvertible becomes NaN).
    sales["Units_Sold"] = pd.to_numeric(sales.Units_Sold, errors="coerce")
    sales["Revenue"] = pd.to_numeric(sales.Revenue, errors="coerce")
    sales["Effort_Hours"] = pd.to_numeric(sales.Effort_Hours, errors="coerce")
    sales["Sale_Date"] = dates
    invalid = blank | bad_date | non_numeric  # rows with a missing or unusable value

    # Check 4: foreign keys. Two different situations:
    #   orphan  = the Employee_ID is not in the raw Employee table at all (1048, C3PO, R2D2)
    #   cascade = the employee exists but we removed it above (1023, bad Store_ID) so its sales have no valid employee
    emp_num = pd.to_numeric(sales.Employee_ID, errors="coerce")
    orphan = ~emp_num.isin(raw_emp_nums) & ~invalid
    cascade = emp_num.isin(raw_emp_nums) & ~emp_num.isin(set(emp.Employee_ID)) & ~invalid
    # Sales.Product_ID must exist in the cleaned Product table.
    prod_bad = ~pd.to_numeric(sales.Product_ID, errors="coerce").isin(set(prod.Product_ID)) & ~invalid
    # Rows that are exact copies of an earlier row.
    dupe = sales.drop(columns="row_id").duplicated()

    # Check 5: the business rules from the assignment.
    neg_rev, low_units, neg_eff = sales.Revenue < 0, sales.Units_Sold < 1, sales.Effort_Hours < 0
    log.check("sales_performance", "Revenue < 0", neg_rev.sum(), act(neg_rev.sum(), "discarded"))
    log.check("sales_performance", "Units_Sold < 1", low_units.sum(), act(low_units.sum(), "discarded"))
    log.check("sales_performance", "Effort_Hours < 0", neg_eff.sum(), act(neg_eff.sum(), "discarded"))
    # Revenue = 0 is allowed by the rule (>= 0), so we keep it but report it.
    log.check("sales_performance", "Revenue = 0 (allowed by rule Revenue >= 0)", (sales.Revenue == 0).sum(),
              "kept, flagged")
    # Report which employee IDs caused the foreign key problems and how many sales each has.
    ids = ", ".join(f"{k} x{v}" for k, v in sales.loc[orphan, "Employee_ID"].value_counts().items())
    log.check("sales_performance", "Employee_ID not in the Employee table (FK)", orphan.sum(),
              act(orphan.sum(), "discarded: " + ids))
    ids = ", ".join(f"{k} x{v}" for k, v in sales.loc[cascade, "Employee_ID"].value_counts().items())
    log.check("sales_performance", "Employee exists but was removed in cleaning (invalid Store_ID)", cascade.sum(),
              act(cascade.sum(), "discarded: " + ids))
    log.check("sales_performance", "Product_ID not in Product (FK)", prod_bad.sum(), act(prod_bad.sum(), "discarded"))
    log.check("sales_performance", "Exact duplicate rows", dupe.sum(), act(dupe.sum(), "later duplicates discarded"))

    # Decide ONE reason per rejected sale. A row can break several rules, so the rules are listed
    # in priority order and the first one that applies is the reason written in the discard log.
    rules = [
        ("Missing or invalid value", invalid),
        ("Revenue < 0", neg_rev),
        ("Units_Sold < 1", low_units),
        ("Effort_Hours < 0", neg_eff),
        ("Employee_ID not in Employee table (FK violation)", orphan),
        ("Employee removed during cleaning (invalid Store_ID), no valid employee", cascade),
        ("Product_ID not in Product table (FK violation)", prod_bad),
        ("Exact duplicate of an earlier row", dupe),
    ]
    reason = pd.Series(pd.NA, index=sales.index, dtype="object")
    for text, mask in rules[::-1]:  # go through the list backwards so earlier (higher priority) rules overwrite later ones
        reason[mask.to_numpy()] = text
    rejected = reason.notna()  # True for every row that has a reason = every row to be removed
    # Write one discard-log entry per rejected sale, e.g. "row 218 (emp 1038)".
    for text, grp in reason[rejected].groupby(reason[rejected]):
        log.discard("sales_performance", "row " + sales.loc[grp.index, "row_id"].astype(str)
                    + " (emp " + sales.loc[grp.index, "Employee_ID"] + ")", text)
    # Keep only the valid rows and give the key columns proper integer types.
    sales = sales[~rejected].copy()
    sales["Employee_ID"] = sales.Employee_ID.astype(int)
    sales["Product_ID"] = sales.Product_ID.astype(int)
    sales["Units_Sold"] = sales.Units_Sold.astype(int)
    sales = sales.drop(columns="row_id")  # only needed for the log
    return {"store": store, "employee": emp, "product": prod, "sales": sales}, log, n0



# 4. TRANSFORM  (pandas joins + aggregation)

def transform(clean):
    """Build the dataset used for data mining from the four cleaned tables.
    Returns: the main table (one row per sale) and three aggregated summary tables."""
    store, emp, prod, sales = clean["store"], clean["employee"], clean["product"], clean["sales"]

    # JOINS: attach to every sale the employee's store, the store's name/city and the product's
    # category/price. validate="m:1" makes pandas stop with an error if a join would ever
    # duplicate sales rows (many sales -> one employee / store / product is expected).
    df = (sales
          .merge(emp[["Employee_ID", "Store_ID"]], on="Employee_ID", how="inner", validate="m:1")
          .merge(store[["Store_ID", "Store_Name", "City"]], on="Store_ID", how="inner", validate="m:1")
          .merge(prod, on="Product_ID", how="inner", validate="m:1"))

    # COMPUTED ATTRIBUTES (new columns derived from existing ones)
    # Revenue earned per hour of effort (left blank instead of dividing by zero effort).
    df["Revenue_per_Hour"] = df.Revenue / df.Effort_Hours.where(df.Effort_Hours > 0)
    # Average revenue per unit sold in this sale.
    df["Revenue_per_Unit"] = df.Revenue / df.Units_Sold
    # Revenue compared with what the list price would give (units x base price).
    df["Price_Realisation"] = df.Revenue / (df.Units_Sold * df.Base_Price)
    df["Sale_Month"] = df.Sale_Date.dt.to_period("M").astype(str)   # e.g. '2024-03'
    df["Sale_Weekday"] = df.Sale_Date.dt.day_name()                 # e.g. 'Monday'
    df = df.sort_values(["Employee_ID", "Sale_Date"]).reset_index(drop=True)

    # AGGREGATED DATASET FOR MINING 
    # one row per Effort_Hours value with the average Revenue (avg_rev) and the number of sales behind it.
    # Done in pandas (groupby), not in a database.
    agg = df.groupby("Effort_Hours").agg(avg_rev=("Revenue", "mean"), n_sales=("Revenue", "size")).reset_index()

    # AGGREGATION: g(by) groups the sales by the given columns and computes the same summary
    # numbers for each group (count of sales, totals, averages).
    g = lambda by: (df.groupby(by).agg(  # noqa: E731
        n_sales=("Revenue", "size"),
        total_units=("Units_Sold", "sum"),
        total_revenue=("Revenue", "sum"),
        total_effort_hours=("Effort_Hours", "sum"),
        avg_revenue_per_sale=("Revenue", "mean"),
        avg_effort_per_sale=("Effort_Hours", "mean"),
    ).reset_index())

    # One row per employee (with their store), per store, and per product category.
    emp_sum = g(["Employee_ID", "Store_ID", "Store_Name", "City"])
    emp_sum["revenue_per_effort_hour"] = emp_sum.total_revenue / emp_sum.total_effort_hours
    store_sum = g(["Store_ID", "Store_Name", "City"])
    # How many different employees have sales in each store.
    store_sum["n_employees"] = store_sum.Store_ID.map(df.groupby("Store_ID").Employee_ID.nunique())
    store_sum["revenue_per_effort_hour"] = store_sum.total_revenue / store_sum.total_effort_hours
    cat_sum = g(["Category"])
    cat_sum["revenue_per_effort_hour"] = cat_sum.total_revenue / cat_sum.total_effort_hours
    return df, emp_sum, store_sum, cat_sum, agg


 
# 5. LOAD (write outputs)  +  main program
 
def main():
    # Create the output folders if they do not exist yet.
    OUT.mkdir(exist_ok=True)
    (OUT / "clean").mkdir(exist_ok=True)

    # 1) EXTRACT: use the group's loader, then give the tables shorter keys.
    raw_tables = load_tables(RAW or find_data_folder())  # dict: store, employee, product, sales_performance (all text)
    raw = {"store": raw_tables["store"], "employee": raw_tables["employee"],
           "product": raw_tables["product"], "sales": raw_tables["sales_performance"]}
    # 2) + 3) INSPECT and CLEAN.
    clean, log, n_raw_sales = inspect_and_clean(raw)

    # 4) TRANSFORM.
    df, emp_sum, store_sum, cat_sum, agg = transform(clean)

    # 5) LOAD: save everything to CSV files in output/.
    for name, d in clean.items():
        d.to_csv(OUT / "clean" / f"{name}_clean.csv", index=False)       # cleaned copies of the 4 tables
    pd.DataFrame(log.checks).to_csv(OUT / "quality_checks.csv", index=False)   # result of every check
    pd.DataFrame(log.discards).to_csv(OUT / "discard_log.csv", index=False)    # every removed/repaired record
    df.to_csv(OUT / "mining_dataset.csv", index=False)                         # main dataset for the notebook
    emp_sum.to_csv(OUT / "employee_summary.csv", index=False)
    store_sum.to_csv(OUT / "store_summary.csv", index=False)
    cat_sum.to_csv(OUT / "category_summary.csv", index=False)
    agg.to_csv(OUT / "effort_aggregated.csv", index=False)

    # Small summary of row counts before/after cleaning (saved and printed).
    summary = {
        "raw_rows": {k: int(len(v)) for k, v in raw.items()},
        "clean_rows": {k: int(len(v)) for k, v in clean.items()},
        "sales_rows_in_mining_dataset": int(len(df)),
        "effort_aggregated_rows": int(len(agg)),
        "discarded_sales_rows": int(n_raw_sales - len(clean["sales"])),
    }
    (OUT / "etl_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    print(pd.DataFrame(log.checks).to_string(index=False))


# Run main() only when this file is executed directly (python etl/etl_pipeline.py),
# not when it is imported by another script.
if __name__ == "__main__":
    main()
