# USALI Variance Analyzer Pipeline

This repository now includes a ready-to-run ingestion pipeline for monthly USALI workbooks (like `USALI Statement of Accounts - NOV P&L v2.4.xlsx`).

## What the pipeline does

1. **Reads workbook metadata** from the `Start` sheet (period, BU code, ledger, forecast version).
2. **Extracts P&L lines** from all summary and departmental sheets (`For Operators`, `Rooms`, `F&B`, `A&G`, etc.).
3. **Loads account-level facts** from `Master_Extract` for chart-of-accounts and cost-center mapping.
4. **Calculates variance metrics** per line:
   - Actual vs Budget
   - Actual vs Prior Year
   - Percentage variance
5. **Writes all data into SQLite** so you can build trend reports and narrative explanations over time.

## Quick start

```bash
python -m pip install openpyxl
python usali_pipeline.py ingest \
  --input "USALI Statement of Accounts - NOV P&L v2.4.xlsx" \
  --db usali.db

# Print verification summary (counts + sample extracted codes)
python usali_pipeline.py verify --db usali.db

# Export extracted dimensions and facts to CSV for manual review
python usali_pipeline.py export-verification --db usali.db --outdir verification_exports
```

The export command writes:
- `chart_of_accounts.csv`
- `cost_centers.csv`
- `account_cost_center_map.csv`
- `account_facts_latest_import.csv`
- `statement_lines_latest_import.csv`


## Streamlit front-end

Run a front-end to upload workbook + month, trigger backend ingestion, and review extracted outputs before analysis:

```bash
python -m pip install streamlit pandas openpyxl
streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0
```

Then open: `http://localhost:8501`

The app will:
1. Ask for upload month (`YYYY-MM`) and workbook file.
2. Run backend ingestion into SQLite (`usali.db`).
3. Show preview tables for imports, chart of accounts, cost centers, statement lines, and account facts for review.

## Database tables

- `imports`: one row per uploaded monthly workbook.
- `statement_lines`: extracted P&L lines per sheet, with variance columns.
- `account_facts`: account/cost-center level facts from `Master_Extract` blocks.
- `chart_of_accounts`: discovered account codes over time.
- `cost_centers`: discovered cost centers over time.
- `account_cost_center_map`: account-to-cost-center combinations seen in data.
- `v_statement_trend` (view): period-over-period trend surface for BI/reporting.

## Example analysis queries

### Top unfavorable budget variances by department

```sql
SELECT i.period, s.sheet_name, s.line_label, s.actual, s.budget, s.variance_budget
FROM statement_lines s
JOIN imports i ON i.id = s.import_id
WHERE s.variance_budget > 0
ORDER BY s.variance_budget DESC
LIMIT 25;
```

### Expense lines over budget (for narrative generation)

```sql
SELECT i.period,
       s.sheet_name,
       s.section,
       s.line_label,
       s.actual,
       s.budget,
       s.variance_budget,
       ROUND(100.0 * s.variance_budget / NULLIF(s.budget, 0), 2) AS variance_pct
FROM statement_lines s
JOIN imports i ON i.id = s.import_id
WHERE s.section LIKE '%Expenses%'
  AND s.variance_budget > 0
ORDER BY variance_pct DESC;
```

### Monthly trend for a specific KPI line

```sql
SELECT period, sheet_name, line_label, actual, budget, prior_year
FROM v_statement_trend
WHERE line_label = 'Total Departmental Profit'
ORDER BY period;
```

## Monthly workflow

1. Drop each new monthly workbook into the repo or ingestion folder.
2. Run `usali_pipeline.py ingest` for each workbook (same DB path).
3. Query `v_statement_trend` and `statement_lines` for variance and narrative logic.
4. Feed query output to your reporting layer / LLM narrative generator.

This gives you the core data foundation for trend analysis, budget comparison, prior-year analysis, and automated month-end commentary.
