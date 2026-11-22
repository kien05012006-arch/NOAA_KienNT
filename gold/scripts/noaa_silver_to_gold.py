#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NOAA Teleconnections Silver to Gold Pipeline
============================================
Builds one curated ML-ready NOAA Gold row per USDM week (130 Tuesdays from
2020-01-07 to 2022-06-28), following TeleconnectionsForDryad.R aggregation rules:

  - AO, NAO, PNA : 7-day rolling mean ending on USDM Tuesday (inclusive), rounded to 2 decimals.
  - ENSO         : Nino 3.4 SSTA (1991-2020 base) dated next Wednesday (Tuesday + 1 day),
                   rounded to 2 decimals.
  - enso14/28/84 : trailing 2 / 4 / 12 weekly SSTA means including that Wednesday,
                   rounded to 3 decimals.

Strict completeness: incomplete averaging windows raise ValueError instead of
computing partial averages (unlike R's na.rm=TRUE).

Outputs:
  - gold/data/climate_weekly.parquet
  - gold/data/climate_weekly.csv
  - gold/data/validation.txt
  - gold/data/comparison_reports/weekly_lineage.csv
  - gold/scripts/input_manifest.csv
  - gold/scripts/environment_info.txt
"""

from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import logging
import platform
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("NOAA_Gold_Pipeline")


def resolve_repo_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    candidate = script_dir.parent.parent
    return candidate if (candidate / "silver").exists() else script_dir.parent


REPO_ROOT = resolve_repo_root()
SILVER_SCRIPTS_DIR = REPO_ROOT / "silver" / "scripts"
if str(SILVER_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SILVER_SCRIPTS_DIR))

from noaa_bronze_to_silver import validate_table  # noqa: E402


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_usdm_weeks(path: Path) -> pd.DatetimeIndex:
    """Extract unique USDM Tuesday dates from usdmfinal.csv (YYYYMMDD)."""
    dates = set()
    for chunk in pd.read_csv(path, usecols=["time"], dtype={"time": "string"}, chunksize=100_000):
        if chunk["time"].isna().any() or not chunk["time"].str.fullmatch(r"\d{8}").all():
            raise ValueError("USDM time must contain YYYYMMDD dates without missing values")
        dates.update(chunk["time"].tolist())
    weeks = pd.DatetimeIndex(
        pd.to_datetime(sorted(dates), format="%Y%m%d")
    ).astype("datetime64[ns]")
    if weeks.empty or not (weeks.dayofweek == 1).all():
        raise ValueError("Expected nonempty USDM Tuesday dates")
    return weeks


def read_week_dates(path: Path) -> pd.DatetimeIndex:
    """Read the USDM Tuesday reference calendar from usdm_week_dates.csv."""
    weeks = pd.DatetimeIndex(
        pd.to_datetime(pd.read_csv(path)["week_date"], format="%Y-%m-%d")
    ).astype("datetime64[ns]")
    if weeks.empty or weeks.duplicated().any() or not (weeks.dayofweek == 1).all():
        raise ValueError("Expected nonempty, unique Tuesday dates in column week_date")
    return weeks.sort_values()


def build_gold(daily: pd.DataFrame, enso: pd.DataFrame, weeks: pd.DatetimeIndex) -> pd.DataFrame:
    """Aggregate Silver daily/weekly tables into the curated Gold climate_weekly table."""
    weeks = pd.DatetimeIndex(pd.to_datetime(weeks)).astype("datetime64[ns]")
    daily_clean = validate_table(daily[["date", "ao", "nao", "pna"]], "date", "D").set_index("date")
    enso_clean = validate_table(
        enso[["source_week_date", "nino34_sst", "nino34_ssta"]],
        "source_week_date",
        "7D",
    ).set_index("source_week_date")

    if not (enso_clean.index.dayofweek == 2).all():
        raise ValueError("Expected Wednesday ENSO source dates")

    daily_means = daily_clean.rolling(7, min_periods=7).mean().reindex(weeks)
    source_dates = weeks + pd.Timedelta(days=1)
    ssta = enso_clean["nino34_ssta"]

    gold = daily_means.rename_axis("week_date").reset_index()
    for column, window in [("enso", 1), ("enso14", 2), ("enso28", 4), ("enso84", 12)]:
        gold[column] = (
            ssta.rolling(window, min_periods=window)
            .mean()
            .reindex(source_dates)
            .to_numpy()
        )

    if gold.isna().any().any():
        bad = gold.loc[gold.isna().any(axis=1), "week_date"].dt.strftime("%Y-%m-%d").tolist()
        raise ValueError(
            f"Missing data or incomplete averaging windows for USDM weeks: {bad[:10]}"
        )

    gold["week_date"] = pd.to_datetime(gold["week_date"]).astype("datetime64[ns]")
    gold = gold.round(
        {
            "ao": 2,
            "nao": 2,
            "pna": 2,
            "enso": 2,
            "enso14": 3,
            "enso28": 3,
            "enso84": 3,
        }
    )
    return gold


def build_weekly_lineage(weeks: pd.DatetimeIndex) -> pd.DataFrame:
    """Create explicit per-week traceability mapping USDM Tuesday <-> source windows."""
    rows = []
    for w in weeks:
        wed = w + pd.Timedelta(days=1)
        rows.append(
            {
                "time_id": w.strftime("%Y%m%d"),
                "week_date": w.strftime("%Y-%m-%d"),
                "daily_window_start": (w - pd.Timedelta(days=6)).strftime("%Y-%m-%d"),
                "daily_window_end": w.strftime("%Y-%m-%d"),
                "enso_source_week_date": wed.strftime("%Y-%m-%d"),
                "enso14_window_start": (wed - pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
                "enso28_window_start": (wed - pd.Timedelta(days=21)).strftime("%Y-%m-%d"),
                "enso84_window_start": (wed - pd.Timedelta(days=77)).strftime("%Y-%m-%d"),
            }
        )
    return pd.DataFrame(rows)


def write_validation_report(gold: pd.DataFrame, out_path: Path, calendar_source: str) -> None:
    """Write gold/data/validation.txt mirroring USDM Gold validation report style."""
    weeks = gold["week_date"]
    value_cols = ["ao", "nao", "pna", "enso", "enso14", "enso28", "enso84"]
    stats = gold[value_cols].describe().loc[["count", "mean", "std", "min", "50%", "max"]].round(4)

    lines = [
        f"Weeks: {len(gold)}; range: {weeks.min().strftime('%Y-%m-%d')} - {weeks.max().strftime('%Y-%m-%d')}",
        f"Columns ({len(gold.columns)}): {', '.join(gold.columns)}",
        f"USDM calendar source: {calendar_source}",
        f"All Tuesdays: {bool((weeks.dt.dayofweek == 1).all())}; Duplicate weeks: {int(weeks.duplicated().sum())}; Null cells: {int(gold.isna().sum().sum())}",
        "Aggregation rules: AO/NAO/PNA = 7-day rolling mean ending on USDM Tuesday (inclusive); ENSO = Nino 3.4 SSTA (1991-2020 base) dated next Wednesday (Tuesday + 1d); enso14/28/84 = trailing 2/4/12-week SSTA means.",
        "Retrospective alignment note: matches TeleconnectionsForDryad.R (Erhardt et al., 2024); Silver inputs remain unchanged.",
        "",
        "Summary Statistics:",
        stats.to_string(),
        "",
    ]
    out_path.write_text("\n".join(lines), encoding="utf-8")


def write_input_manifest(
    manifest_path: Path,
    daily_path: Path,
    enso_path: Path,
    calendar_path: Path,
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["role", "filename", "relative_path", "size_bytes", "sha256"])
        writer.writeheader()
        for role, p in [
            ("silver_daily", daily_path),
            ("silver_enso", enso_path),
            ("usdm_calendar", calendar_path),
        ]:
            try:
                rel = str(p.resolve().relative_to(REPO_ROOT.resolve()))
            except ValueError:
                rel = str(p)
            writer.writerow(
                {
                    "role": role,
                    "filename": p.name,
                    "relative_path": rel,
                    "size_bytes": p.stat().st_size,
                    "sha256": sha256_file(p),
                }
            )


def write_environment_info(env_path: Path) -> None:
    info = [
        f"Generated UTC : {datetime.datetime.now(datetime.timezone.utc).isoformat()}",
        f"Python        : {sys.version.splitlines()[0]}",
        f"Platform      : {platform.platform()}",
        f"pandas        : {pd.__version__}",
        f"numpy         : {np.__version__}",
        f"pyarrow       : {pa.__version__}",
        "",
    ]
    env_path.write_text("\n".join(info), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build curated Gold climate_weekly table from NOAA Silver Parquet tables."
    )
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=REPO_ROOT / "silver" / "data",
        help="Path to Silver data directory.",
    )
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=REPO_ROOT / "gold" / "data",
        help="Path to output Gold data directory.",
    )
    parser.add_argument(
        "--week-dates-csv",
        type=Path,
        default=REPO_ROOT / "gold" / "data" / "usdm_week_dates.csv",
        help="Path to USDM Tuesday reference calendar CSV.",
    )
    parser.add_argument(
        "--usdm-csv",
        type=Path,
        default=None,
        help="Optional path to USDM Gold usdmfinal.csv; if given and exists, refreshes usdm_week_dates.csv.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Gold outputs if present.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    parquet_out = args.gold_root / "climate_weekly.parquet"
    csv_out = args.gold_root / "climate_weekly.csv"

    if parquet_out.exists() and csv_out.exists() and not args.overwrite:
        logger.info("Gold outputs already exist. Skipping (use --overwrite to rebuild).")
        return

    from_usdm = args.usdm_csv is not None and args.usdm_csv.exists()
    if from_usdm:
        weeks = read_usdm_weeks(args.usdm_csv)
        calendar_source = f"usdmfinal.csv ({args.usdm_csv.name})"
        calendar_file_used = args.usdm_csv
    else:
        weeks = read_week_dates(args.week_dates_csv)
        calendar_source = args.week_dates_csv.name
        calendar_file_used = args.week_dates_csv

    daily_path = args.silver_root / "climate_daily.parquet"
    enso_path = args.silver_root / "enso_weekly.parquet"
    daily = pd.read_parquet(daily_path)
    enso = pd.read_parquet(enso_path)

    gold = build_gold(daily, enso, weeks)

    args.gold_root.mkdir(parents=True, exist_ok=True)
    comparison_dir = args.gold_root / "comparison_reports"
    comparison_dir.mkdir(parents=True, exist_ok=True)

    gold.to_parquet(parquet_out, engine="pyarrow", compression="snappy", index=False)
    gold.to_csv(csv_out, index=False, date_format="%Y-%m-%d")

    if from_usdm:
        args.week_dates_csv.parent.mkdir(parents=True, exist_ok=True)
        gold[["week_date"]].to_csv(args.week_dates_csv, index=False, date_format="%Y-%m-%d")

    lineage_df = build_weekly_lineage(weeks)
    lineage_df.to_csv(comparison_dir / "weekly_lineage.csv", index=False)

    write_validation_report(gold, args.gold_root / "validation.txt", calendar_source)

    scripts_dir = Path(__file__).resolve().parent
    write_input_manifest(scripts_dir / "input_manifest.csv", daily_path, enso_path, calendar_file_used)
    write_environment_info(scripts_dir / "environment_info.txt")

    logger.info(f"Wrote {parquet_out}: {len(gold)} weeks ({weeks.min().date()} .. {weeks.max().date()})")
    logger.info(f"Wrote {csv_out}")
    logger.info(f"Wrote {args.gold_root / 'validation.txt'}")
    logger.info(f"Wrote {comparison_dir / 'weekly_lineage.csv'}")
    print("\n" + gold.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
