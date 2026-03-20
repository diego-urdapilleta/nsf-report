# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Report

Execute `main_data/run_report.py` to build all views/tables in sequence:

```bash
python main_data/run_report.py
```

This requires Snowflake credentials in `.env` (gitignored):
```
SF_ACCOUNT="..."
SF_USER="..."
```

Authentication uses external browser (SSO). The script sets role `RISK_ROLE`, warehouse `DATASCIENCE_WH`, database `ANALYTICS_PRODUCTION`, and runs the four `main_data/` SQL files in order.

## Architecture

This is a Snowflake SQL reporting project for NSF (Non-Sufficient Funds) analysis. SQL objects are built in `sandbox.durdapilletadelaparra.*`. Downstream consumption is via Sigma dashboards.

### Two Platforms: Ario and TK (Turnkey)

Loans originate on Ario (older) and are being migrated to Turnkey. Most tables carry both `ario_transaction_id` and `tk_transaction_id`. The deduplication rule throughout: **if a loan has any TK transaction, drop Ario-only transactions for that loan**.

### `main_data/` — Core Pipeline (run in order)

| File | Output | Purpose |
|------|--------|---------|
| `01_all_nsfs.sql` | `all_nsfs_1` (view) | Raw NSF events from both Ario and TK ledger sources |
| `02_first_nsf_indicator.sql` | `all_nsfs` (view) | Adds `is_first_nsf` flag; filters by DPD ≤ 2 and account status |
| `03_all_transactions.sql` | `all_transactions` (view) | Unions NSFs + payments; deduplicates Ario/TK rows per loan |
| `04_main_data.sql` | `nsf_report_dt` (dynamic table, 1-day lag) | Final reporting table with full loan/merchant/portfolio attributes |

`run_report.py` also creates a view `nsf_report` over the dynamic table for Sigma.

### `payment_plans/`

Standalone analysis layered on top of `nsf_report_dt`. Joins NSFs with payment plans, categorizes plan types (Deferred, Reduction/skip, Restructured, Other), and produces vintage curves of payment plan adoption rates.

### `plots/` and `resolution_vintage_curves/`

Parameterized queries designed to be run directly in Sigma. They accept selector/filter parameters and output weekly rates or vintage curves. Not part of the `run_report.py` pipeline.

### Key Business Logic

- **Resolution**: `platform_resolved = TRUE AND time_to_cure ≤ 14 days`
- **First NSF**: Minimum posted date per loan, with no prior unresolved NSF, DPD ≤ 2, account not written off / in settlement
- **FICO buckets**: <600, 600–649, 650–699, 700–749, 750+
- **Loan age buckets**: ≤3mo, ≤6mo, ≤12mo, 12mo+
- **Outstanding balance buckets**: ≤$10k, ≤$20k, ≤$50k, $50k+
