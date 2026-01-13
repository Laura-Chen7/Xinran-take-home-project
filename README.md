# Data Import Assignment

## Goal
Convert three exported client spreadsheets (constituents + emails + donation history) into two CSV outputs for client sign-off and import.

## Repository structure
- `main.py`: reads input files and prints schema information (row counts + columns)
- `data/input/`: local-only input files (ignored by git)
- `data/output/`: final output CSVs (will be committed)
- `src/`: transformation and validation modules (to be implemented)

> Note: `data/input/` is intentionally excluded via `.gitignore` to avoid committing client data to the repository.

## How to run (current milestone: input ingestion + schema check)
1. Place the three input CSV files under `data/input/` with the following names:
   - `constituents.csv`
   - `emails.csv`
   - `donations.csv`

2. Run:
```bash
python3 main.py
