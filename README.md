# COMP-4522-Assignment-2: Retail ETL, data quality and predictive analytics

Pandas route (no database server needed).

## Folder layout
- `loadData.py` - data loading script (`load_tables`) plus the group's compact load/clean/join (`load_and_clean_data`, used as a cross-check)
- `data/raw/` - the four supplied CSV files (the scripts also work if the CSVs sit next to `loadData.py`)
- `etl/etl_pipeline.py` - full ETL: quality checks, cleaning, discard log, joins, computed attributes, aggregation -> `output/`
- `output/` - `mining_dataset.csv`, `*_summary.csv`, `quality_checks.csv`, `discard_log.csv`, `clean/`
- `notebook/analytics.ipynb` - descriptive analytics + linear regression (Effort_Hours -> Revenue)
- `report/COMP4522_Project_Report.docx` - written report

## How to run (from this folder)
```
pip install pandas scikit-learn matplotlib seaborn jupyter
python loadData.py            # loads the CSVs, runs the group's compact cleaning (prints "Discarded 396")
python etl/etl_pipeline.py    # full ETL: creates/updates output/
jupyter notebook notebook/analytics.ipynb   # Run All
```

## Key results
- 20,000 sales rows -> 19,604 valid (396 discarded, all logged in `output/discard_log.csv`)
- Both cleaning scripts give the same 19,604 valid sales
- Revenue = 50.48 + 27.41 x Effort_Hours, R2 about 0.80
- Predicted revenue at 10 hours of effort: about $324.59
