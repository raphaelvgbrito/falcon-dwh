#!/usr/bin/env bash
# Clean end-to-end run for the hand-in: empty schema -> all days in order -> reports.
# Usage: scripts/full_run.sh [python]   (run from the repo root)
set -euo pipefail
PY="${1:-python}"
mkdir -p logs reports

"$PY" -m falcon_dwh.reset_schema 2>&1 | tee logs/full_run.log
"$PY" -m falcon_dwh.run 2>&1 | tee -a logs/full_run.log
"$PY" -m falcon_dwh.report counts 2>&1 | tee logs/report_counts.log
"$PY" -m falcon_dwh.report rerun --date 2026-09-15 2>&1 | tee logs/rerun_proof.log
"$PY" -m falcon_dwh.report dq 2>&1 | tee logs/report_dq.log
