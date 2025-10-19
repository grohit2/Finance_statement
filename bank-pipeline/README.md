# bank-pipeline (Spark local) — multi-profile + generic rules + React+D3 CSV dashboard

Profiles: `lokesh`, `rohit`.

## Quick start
1) Put your bank CSVs into:
   - `data/raw_inbox/lokesh/`
   - `data/raw_inbox/rohit/`
2) Create venv & deps: `make setup`
3) Run the full pipeline: `make pipeline`
4) Build share list for the dashboard: `make shares`
5) (Optional) Start the React + D3 dashboard (reads CSV exports):
   - macOS/Linux (symlink once):
     ```
     cd apps/react-d3/public && ln -s ../../../data/exports/csv data && cd ..
     npm run dev
     ```
   - Windows (no symlinks): copy `data/exports/csv` to `apps/react-d3/public/data/`

## Data flow
raw_inbox → bronze (canonical) → silver (clean + ids) → categorize (rules) → gold (facts + aggregates) → CSV exports → React+D3.

## CSV input (minimum)
- Date column (e.g., `Date`, `Posting Date`, `Transaction Date`)
- Description column (e.g., `Description`, `Narration`, `Details`)
- Either `Amount` OR separate `Debit` / `Credit` columns (we normalize to signed `amount`)

Optional:
- `Balance` → `balance`
- `Currency` → `currency`
- `Account Number`/`Account` → `account_id`

## Rules
Edit `conf/filters.yaml`. Rules can match any column using equals/contains/regex/gt/lt/etc. and set fields like `category`, `merchant`, `is_transfer`, etc.
