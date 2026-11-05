"""Entry point for a Databricks job (spark_python_task) or `spark-submit`.

    run_job.py run    --config config/config.databricks_job.yaml --date 2026-08-24
    run_job.py report --config config/config.databricks_job.yaml rerun --date 2026-09-15

The package lives in src/; the job runs this file from the repo checkout, so src/ is put on the
path here instead of installing a wheel.
"""
import inspect
import sys
from pathlib import Path

# Databricks serverless runs this file with exec(compile(source, path)), so __file__ is not set;
# the code object still knows its path. Plain python / spark-submit set __file__ as usual.
_this_file = globals().get("__file__") or inspect.currentframe().f_code.co_filename
ROOT = Path(_this_file).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from falcon_dwh import report, run  # noqa: E402

if __name__ == "__main__":
    command, args = sys.argv[1], sys.argv[2:]
    if command == "run":
        run.main(args)
    elif command == "report":
        report.main(args)
    else:
        raise SystemExit(f"unknown command {command!r}, use 'run' or 'report'")
