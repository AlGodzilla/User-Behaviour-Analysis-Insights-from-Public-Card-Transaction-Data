# User Behaviour Analysis — Insights from Public Card & Transaction Data

This repo ingests three public datasets (users, cards, transactions), cleans them, saves a local SQLite database, runs a handful of analysis queries, and exports charts and CSVs for quick exploration.

## What’s in here

- \`analysis.py\` — read/clean data, persist to SQLite, run SQL queries, and export charts/CSVs  
- \`results/\` — query outputs as CSV (ready for Excel/BI tools)  
- \`charts/\` — PNG charts suitable for slides and docs  
- \`answer.pptx\` (optional) — presentation wired to these latest charts  
- \`.gitignore\` — excludes raw source files & local DB from version control

> **Note:** Raw source files (\`users_data.csv\`, \`cards_data.csv\`, \`transactions_data.csv\`) and the local DB are **not included** in the repository. Place the CSVs in the repo root (or \`data/\`) before running.

## Requirements

- Python 3.9+
- `pip install pandas matplotlib seaborn openpyxl`  (openpyxl is harmless even though transactions are now read from CSV only)

## How to run

From the repo root:

```bash
python analysis.py
````

This will produce:

* `user_behavior.db` (SQLite) — local analytical store
* `results/` CSVs
* `charts/` PNGs
* `data_preview.txt` (quick peek at shapes/dtypes)

## Outputs (click to open)

**Results (CSV):**

* [age\_distribution.csv](results/age_distribution.csv)
* [avg\_txn\_amount\_by\_brand.csv](results/avg_txn_amount_by_brand.csv)
* [card\_brand\_distribution.csv](results/card_brand_distribution.csv)
* [card\_type\_distribution.csv](results/card_type_distribution.csv)
* [gender\_distribution.csv](results/gender_distribution.csv)
* [top\_mcc.csv](results/top_mcc.csv)
* [transaction\_methods.csv](results/transaction_methods.csv)

**Charts (PNG):**

* [Age distribution](charts/age_distribution.png)
* [Average amount by brand](charts/avg_amount_by_brand.png)
* [Card type distribution](charts/card_type_distribution.png)
* [Gender distribution](charts/gender_distribution.png)
* [Top MCC frequency](charts/top_mcc_frequency.png)
* [Transaction method distribution](charts/transaction_method_distribution.png)

**Dashboard (Looker Studio):**
[Interactive report](https://lookerstudio.google.com/reporting/a5ef513f-e7b5-4c78-9260-32cbbb4e5afe)

## Notes

* Transactions are read **only** from \`transactions\_data.csv\` (no Excel fallback).
* The repository ignores the raw CSVs and the local DB by design; commit the processed outputs and charts only.