"""Hand-in reports, written to reports/:

    python -m falcon_dwh.report counts                 # row counts of every output table
    python -m falcon_dwh.report rerun --date 2026-09-15  # re-run proof: before / after one day
    python -m falcon_dwh.report dq                     # data quality report + reconciliation

All outputs are small aggregates, so collecting them to the driver is fine.
"""
import argparse
import csv
import datetime as dt
import logging
from pathlib import Path

from pyspark.sql import functions as F

from falcon_dwh.checks import fingerprint
from falcon_dwh.config import load_config, repo_path, table_name
from falcon_dwh.run import run_day
from falcon_dwh.schemas import SOURCES
from falcon_dwh.spark_session import get_spark

REPORTS = Path("reports")  # set from output.reports_dir in main()

# table -> numeric columns whose sums are part of the proof
OUTPUTS = {
    **{f"stg_{n}": [] for n in SOURCES},
    "stg_postbacks": ["revenue"],
    "stg_orders": ["amount"],
    "dim_placement": [],
    "dim_banner": [],
    "dim_date": [],
    "dim_time": [],
    "fact_request": ["order_amount"],
    "fact_impression": ["render_count", "click_count", "conversion_count", "revenue_usd"],
    "fact_postback": ["revenue"],
    "rejected_impressions": [],
    "ml_training": ["order_amount_usd"],
    "ref_fx_rates": ["units_per_usd"],
    "ref_holidays": [],
    "dq_report": ["failed_rows"],
    "recon_impressions": ["stg_rows", "fact_rows", "collapsed_rerenders", "rejected_rows"],
}


def snapshot(spark, cfg):
    out = {}
    for t, sums in OUTPUTS.items():
        if spark.catalog.tableExists(table_name(cfg, t)):
            out[t] = fingerprint(spark.table(table_name(cfg, t)), sums)
    return out


def _md_table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def write_counts(spark, cfg):
    snap = snapshot(spark, cfg)
    rows = [(t, f"{fp['rows']:,}") for t, fp in snap.items()]
    text = f"# Row counts after the full run\n\nGenerated {dt.datetime.utcnow():%Y-%m-%d %H:%M} UTC from " \
           f"`{cfg['warehouse']['catalog']}.{cfg['warehouse']['schema']}`.\n\n" + _md_table(["table", "rows"], rows)
    (REPORTS / "row_counts.md").write_text(text + "\n", encoding="utf-8")
    print(text)


def write_rerun_proof(spark, cfg, day):
    before = snapshot(spark, cfg)
    run_day(spark, cfg, day)
    after = snapshot(spark, cfg)
    rows, all_equal = [], True
    for t in before:
        b, a = before[t], after.get(t, {})
        same = b == a
        all_equal &= same
        sums_b = "; ".join(f"{k[4:]}={b[k]}" for k in b if k.startswith("sum_"))
        sums_a = "; ".join(f"{k[4:]}={a.get(k)}" for k in b if k.startswith("sum_"))
        rows.append((t, f"{b['rows']:,}", f"{a.get('rows', 0):,}", b["checksum"], a.get("checksum"),
                     sums_b or "-", sums_a or "-", "yes" if same else "**NO**"))
    text = (
        f"# Re-run proof\n\nRe-ran the whole pipeline for **{day}** (all layers) on top of the fully loaded "
        f"warehouse, and compared every table before and after.\n`checksum` = sum of xxhash64 over all "
        f"columns of every row (order independent); sums are the money / count columns, summed as decimals so they are exact.\n\n"
        + _md_table(["table", "rows before", "rows after", "checksum before", "checksum after",
                     "sums before", "sums after", "identical"], rows)
        + f"\n\n**All tables identical: {'yes' if all_equal else 'NO'}**\n"
    )
    (REPORTS / f"rerun_proof_{day}.md").write_text(text, encoding="utf-8")
    print(text)
    return all_equal


def write_dq(spark, cfg):
    dq = spark.table(table_name(cfg, "dq_report"))
    cols = ["load_date", "check", "table_name", "total_rows", "failed_rows", "action", "example"]
    rows = dq.select(*cols).orderBy("load_date", "check", "table_name").collect()
    with open(REPORTS / "dq_report_by_day.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows([[r[c] for c in cols] for r in rows])

    # summary over all days: one line per problem that occurred
    summary = (
        dq.where("failed_rows > 0").groupBy("check", "table_name").agg(
            F.sum("failed_rows").alias("failed_rows"), F.countDistinct("load_date").alias("days"),
            F.first("action").alias("action"), F.min("example").alias("example"))
        .orderBy(F.desc("failed_rows")).collect()
    )
    recon = spark.table(table_name(cfg, "recon_impressions")).orderBy("load_date").collect()
    rec_rows = [(r.load_date, f"{r.stg_rows:,}", f"{r.fact_rows:,}", f"{r.collapsed_rerenders:,}", r.rejected_rows,
                 r.difference, "yes" if r.reconciled else "**NO**") for r in recon]
    text = (
        "# Data quality summary (all days)\n\nFull per-day report: `dq_report_by_day.csv` "
        "(and table `dq_report`).\n\n"
        + _md_table(["check", "table", "failed rows", "days", "action", "example"],
                    [(s.check, s.table_name, f"{s.failed_rows:,}", s.days, s.action,
                      "`" + (s.example or "")[:160].replace("|", "/") + "`") for s in summary])
        + "\n\n# Impression reconciliation per day\n\nstaging rows = fact rows + collapsed re-renders + rejected rows\n\n"
        + _md_table(["load_date", "staging", "fact", "collapsed", "rejected", "difference", "reconciled"], rec_rows)
        + "\n"
    )
    (REPORTS / "dq_summary.md").write_text(text, encoding="utf-8")
    print(text[:4000])


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("what", choices=["counts", "rerun", "dq"])
    p.add_argument("--date")
    p.add_argument("--config")
    p.add_argument("--schema")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    for noisy in ("py4j", "databricks", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    cfg = load_config(args.config, args.schema)
    global REPORTS
    REPORTS = repo_path(cfg.get("output", {}).get("reports_dir", "reports"))
    REPORTS.mkdir(parents=True, exist_ok=True)
    spark = get_spark(cfg)
    if args.what == "counts":
        write_counts(spark, cfg)
    elif args.what == "rerun":
        ok = write_rerun_proof(spark, cfg, args.date)
        raise SystemExit(0 if ok else 1)
    else:
        write_dq(spark, cfg)


if __name__ == "__main__":
    main()
