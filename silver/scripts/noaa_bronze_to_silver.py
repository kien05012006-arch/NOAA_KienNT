#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NOAA Teleconnections Bronze to Silver Pipeline
==============================================
Ingests raw Bronze NOAA/CPC files (AO, NAO, PNA daily CSVs + ENSO Nino 3.4
weekly fixed-width .for), validates temporal continuity, domain integrity,
and fixed-width parsing, and writes standardized Snappy Parquet tables plus
quality reports, schema metadata, and lineage manifests for the Silver Layer.

Architecture:
    Bronze: bronze/data/ingest_date=YYYY-MM-DD/ (3 CSVs + wksst9120.for + _manifest.json)
    Silver: silver/data/climate_daily.parquet + silver/data/enso_weekly.parquet
    Gold  : gold/data/climate_weekly.parquet (and climate_weekly.csv)
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("NOAA_Silver_Pipeline")

MONTHS: Dict[str, int] = {
    name: idx
    for idx, name in enumerate(
        "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1
    )
}

DAILY_SPECS: List[Tuple[str, str]] = [
    ("ao", "norm.daily.ao.cdas.z1000.19500101_current.csv"),
    ("nao", "norm.daily.nao.cdas.z500.19500101_current.csv"),
    ("pna", "norm.daily.pna.cdas.z500.19500101_current.csv"),
]
ENSO_FILENAME = "wksst9120.for"

# Glued negative SSTA pattern for 3-column sliced Nino 3.4 .for (e.g. ' 14DEC2022       25.7-0.9')
ENSO_NINO34_PATTERN = re.compile(
    r"\s*(\d{2})([A-Z]{3})(\d{4})\s+([+-]?\d+\.\d+)\s*([+-]?\d+\.\d+)\s*"
)
# Full raw NOAA wksst9120.for fixed widths (9 columns: week, 4 regions x (SST, SSTA))
ENSO_FULL_WIDTHS = [12, 7, 4, 9, 4, 9, 4, 9, 4]


@dataclass
class PipelineConfig:
    bronze_root: Path
    silver_root: Path
    snapshot_dir: Optional[Path] = None
    overwrite: bool = False


@dataclass
class TableQualityMetric:
    table_name: str
    source_files: List[str]
    row_count: int = 0
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    expected_frequency: str = ""
    is_date_continuous: bool = False
    duplicate_dates: int = 0
    null_values: int = 0
    sentinel_values: int = 0
    status: str = "PENDING"
    column_stats: Dict[str, Dict[str, float]] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


def resolve_repo_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    candidate = script_dir.parent.parent
    return candidate if (candidate / "bronze").exists() else script_dir.parent


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def find_latest_bronze_dir(bronze_root: Path) -> Path:
    """Return newest bronze/data/ingest_date=* folder, or bronze_root itself if flat."""
    folders = sorted(bronze_root.glob("ingest_date=*"))
    return folders[-1] if folders else bronze_root


def validate_table(frame: pd.DataFrame, date_column: str, frequency: str) -> pd.DataFrame:
    """Reject empty, missing-date, duplicate, gapped, non-finite, or sentinel-valued inputs."""
    if frame.empty or frame[date_column].isna().any():
        raise ValueError(f"Empty table or invalid dates: {date_column}")
    if frame[date_column].duplicated().any():
        raise ValueError(f"Duplicate dates: {date_column}")

    frame = frame.sort_values(date_column).reset_index(drop=True)
    frame[date_column] = pd.to_datetime(frame[date_column]).astype("datetime64[ns]")

    expected = pd.date_range(
        frame[date_column].min(),
        frame[date_column].max(),
        freq=frequency,
    ).astype("datetime64[ns]")
    if not pd.DatetimeIndex(frame[date_column]).equals(expected):
        raise ValueError(f"Missing or irregular dates: {date_column}")

    values = frame.drop(columns=date_column).to_numpy(dtype=float)
    if not np.isfinite(values).all() or (np.abs(values) >= 99).any():
        raise ValueError(
            "Missing/non-finite values or possible NOAA missing-value sentinels (|x| >= 99)"
        )
    return frame


def read_daily_csv(path: Path, index_name: str) -> pd.DataFrame:
    """Read a single NOAA CPC daily teleconnection CSV into (date, <index_name>)."""
    raw = pd.read_csv(path)
    value_col = f"{index_name}_index_cdas"
    required = {"year", "month", "day", value_col}
    if not required.issubset(raw.columns):
        raise ValueError(
            f"Missing required columns in {path.name}. Expected {sorted(required)}, got {list(raw.columns)}"
        )
    dates = pd.to_datetime(raw[["year", "month", "day"]], errors="raise").astype("datetime64[ns]")
    values = pd.to_numeric(raw[value_col], errors="raise").astype("float64")
    return validate_table(pd.DataFrame({"date": dates, index_name: values}), "date", "D")


def read_enso_for(path: Path) -> pd.DataFrame:
    """
    Parse NOAA weekly Nino 3.4 fixed-width file (.for).
    Supports both:
      1) Project-sliced 3-column format (Week, Nino34 SST, Nino34 SSTA; widths [12, 9, 4])
      2) Full raw 9-column NOAA wksst9120.for format (widths [12, 7, 4, 9, 4, 9, 4, 9, 4])
    Safely separates adjacent negative anomalies such as '25.7-0.9'.
    """
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    if len(lines) < 5 or "Nino34" not in " ".join(lines[:4]):
        raise ValueError("Expected wksst9120.for with four header lines mentioning Nino34")

    header_blob = " ".join(lines[:4])
    is_full_raw = "Nino1+2" in header_blob or "Nino4" in header_blob

    if is_full_raw:
        raw = pd.read_fwf(path, skiprows=4, header=None, widths=ENSO_FULL_WIDTHS)
        dates = pd.to_datetime(
            raw[0].astype(str).str.strip(), format="%d%b%Y", errors="raise"
        ).astype("datetime64[ns]")
        sst = pd.to_numeric(raw[5], errors="raise").astype("float64")
        ssta = pd.to_numeric(raw[6], errors="raise").astype("float64")
        frame = pd.DataFrame(
            {"source_week_date": dates, "nino34_sst": sst, "nino34_ssta": ssta}
        )
    else:
        rows: List[Tuple[pd.Timestamp, float, float]] = []
        for line_number, line in enumerate(lines[4:], 5):
            if not line.strip():
                continue
            match = ENSO_NINO34_PATTERN.fullmatch(line)
            if match is None:
                raise ValueError(
                    f"Unexpected ENSO format at line {line_number}; expected Nino34 SST/SSTA only"
                )
            day, month, year, sst_str, ssta_str = match.groups()
            dt = pd.Timestamp(year=int(year), month=MONTHS[month], day=int(day))
            rows.append((dt, float(sst_str), float(ssta_str)))
        frame = pd.DataFrame(
            rows, columns=["source_week_date", "nino34_sst", "nino34_ssta"]
        )

    frame = validate_table(frame, "source_week_date", "7D")
    if not frame["source_week_date"].dt.dayofweek.eq(2).all():
        raise ValueError("Expected Wednesday ENSO source dates (dayofweek == 2)")
    return frame


def compute_column_stats(frame: pd.DataFrame, numeric_cols: List[str]) -> Dict[str, Dict[str, float]]:
    stats: Dict[str, Dict[str, float]] = {}
    for col in numeric_cols:
        series = frame[col]
        stats[col] = {
            "min": round(float(series.min()), 6),
            "max": round(float(series.max()), 6),
            "mean": round(float(series.mean()), 6),
            "std": round(float(series.std()), 6),
        }
    return stats


class NOAABronzeToSilverPipeline:
    """Orchestrates NOAA Bronze ingestion -> validation -> Silver Parquet + governance artifacts."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.quality_reports_dir = self.config.silver_root / "quality_reports"
        self.manifests_dir = self.config.silver_root / "manifests"
        self.metadata_dir = self.config.silver_root / "metadata"
        self._init_directories()

    def _init_directories(self) -> None:
        for d in [
            self.config.silver_root,
            self.quality_reports_dir,
            self.manifests_dir,
            self.metadata_dir,
        ]:
            d.mkdir(parents=True, exist_ok=True)

    def _write_schema_metadata(self) -> Path:
        schema_doc = {
            "layer": "silver",
            "domain": "NOAA CPC Teleconnections",
            "climatology_baseline": "1991-2020 (for Nino 3.4 SSTA); CDAS standardized daily indices (AO, NAO, PNA)",
            "tables": {
                "climate_daily.parquet": {
                    "granularity": "1 row per calendar day (continuous daily buffer 2019-2022)",
                    "primary_key": ["date"],
                    "columns": [
                        {"name": "date", "dtype": "datetime64[ns]", "unit": "ISO date", "description": "Observation date"},
                        {"name": "ao", "dtype": "float64", "unit": "standardized index (dimensionless)", "description": "Arctic Oscillation daily index (z1000 CDAS)"},
                        {"name": "nao", "dtype": "float64", "unit": "standardized index (dimensionless)", "description": "North Atlantic Oscillation daily index (z500 CDAS)"},
                        {"name": "pna", "dtype": "float64", "unit": "standardized index (dimensionless)", "description": "Pacific/North American daily index (z500 CDAS)"},
                    ],
                },
                "enso_weekly.parquet": {
                    "granularity": "1 row per NOAA CPC week (Wednesdays, continuous 7D buffer 2019-2022)",
                    "primary_key": ["source_week_date"],
                    "columns": [
                        {"name": "source_week_date", "dtype": "datetime64[ns]", "unit": "ISO date (Wednesday)", "description": "NOAA CPC weekly centered date"},
                        {"name": "nino34_sst", "dtype": "float64", "unit": "degC", "description": "Nino 3.4 sea surface temperature"},
                        {"name": "nino34_ssta", "dtype": "float64", "unit": "degC", "description": "Nino 3.4 sea surface temperature anomaly (1991-2020 base)"},
                    ],
                },
            },
        }
        out_path = self.metadata_dir / "schema.json"
        out_path.write_text(json.dumps(schema_doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        return out_path

    def run(self) -> Dict[str, Any]:
        start_time = datetime.datetime.now()
        bronze_dir = self.config.snapshot_dir or find_latest_bronze_dir(self.config.bronze_root)
        daily_out = self.config.silver_root / "climate_daily.parquet"
        enso_out = self.config.silver_root / "enso_weekly.parquet"

        logger.info(f"Reading NOAA Bronze snapshot from: {bronze_dir}")

        if daily_out.exists() and enso_out.exists() and not self.config.overwrite:
            logger.info("Silver Parquet tables already exist. Skipping (use --overwrite to rebuild).")
            return {
                "status": "SKIPPED_EXISTING",
                "bronze_snapshot": str(bronze_dir),
                "outputs": [str(daily_out), str(enso_out)],
            }

        # 1. Build climate_daily
        daily_df: Optional[pd.DataFrame] = None
        daily_source_paths: List[Path] = []
        for idx_name, fname in DAILY_SPECS:
            fpath = bronze_dir / fname
            daily_source_paths.append(fpath)
            part = read_daily_csv(fpath, idx_name)
            daily_df = (
                part
                if daily_df is None
                else daily_df.merge(part, on="date", how="outer", validate="one_to_one")
            )
        assert daily_df is not None
        daily_df = validate_table(daily_df, "date", "D")
        daily_df.to_parquet(daily_out, engine="pyarrow", compression="snappy", index=False)
        logger.info(
            f"Wrote {daily_out.name}: {len(daily_df)} rows "
            f"({daily_df['date'].min().date()} .. {daily_df['date'].max().date()})"
        )

        # 2. Build enso_weekly
        enso_path = bronze_dir / ENSO_FILENAME
        enso_df = read_enso_for(enso_path)
        enso_df.to_parquet(enso_out, engine="pyarrow", compression="snappy", index=False)
        logger.info(
            f"Wrote {enso_out.name}: {len(enso_df)} rows "
            f"({enso_df['source_week_date'].min().date()} .. {enso_df['source_week_date'].max().date()})"
        )

        # 3. Quality metrics
        daily_metric = TableQualityMetric(
            table_name="climate_daily",
            source_files=[p.name for p in daily_source_paths],
            row_count=len(daily_df),
            start_date=str(daily_df["date"].min().date()),
            end_date=str(daily_df["date"].max().date()),
            expected_frequency="D",
            is_date_continuous=True,
            duplicate_dates=int(daily_df["date"].duplicated().sum()),
            null_values=int(daily_df.isna().sum().sum()),
            sentinel_values=int((np.abs(daily_df[["ao", "nao", "pna"]].to_numpy()) >= 99).sum()),
            status="SUCCESS",
            column_stats=compute_column_stats(daily_df, ["ao", "nao", "pna"]),
            notes=["Retains full 2019-2022 buffer to support Gold rolling aggregations without boundary NaNs."],
        )

        enso_metric = TableQualityMetric(
            table_name="enso_weekly",
            source_files=[enso_path.name],
            row_count=len(enso_df),
            start_date=str(enso_df["source_week_date"].min().date()),
            end_date=str(enso_df["source_week_date"].max().date()),
            expected_frequency="7D (Wednesday)",
            is_date_continuous=True,
            duplicate_dates=int(enso_df["source_week_date"].duplicated().sum()),
            null_values=int(enso_df.isna().sum().sum()),
            sentinel_values=int((np.abs(enso_df[["nino34_sst", "nino34_ssta"]].to_numpy()) >= 99).sum()),
            status="SUCCESS",
            column_stats=compute_column_stats(enso_df, ["nino34_sst", "nino34_ssta"]),
            notes=[
                "All source_week_date values verified as Wednesday (dayofweek == 2).",
                "Retains 2019 buffer required for 12-week trailing enso84 starting 2020-01-07.",
            ],
        )

        metrics = [daily_metric, enso_metric]
        run_ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        summary = {
            "execution_time_utc": run_ts,
            "elapsed_seconds": round((datetime.datetime.now() - start_time).total_seconds(), 4),
            "bronze_snapshot": bronze_dir.name,
            "tables_processed": len(metrics),
            "successful_tables": sum(1 for m in metrics if m.status == "SUCCESS"),
            "total_daily_rows": daily_metric.row_count,
            "total_enso_weekly_rows": enso_metric.row_count,
            "total_null_values": daily_metric.null_values + enso_metric.null_values,
            "total_sentinel_values": daily_metric.sentinel_values + enso_metric.sentinel_values,
        }

        quality_json = self.quality_reports_dir / "quality_report.json"
        quality_json.write_text(
            json.dumps(
                {"summary": summary, "tables": [asdict(m) for m in metrics]},
                indent=2,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )

        csv_rows = []
        for m in metrics:
            row = asdict(m)
            row["source_files"] = ";".join(m.source_files)
            row["notes"] = " | ".join(m.notes)
            row["column_stats"] = json.dumps(m.column_stats)
            csv_rows.append(row)
        quality_csv = self.quality_reports_dir / "quality_report.csv"
        pd.DataFrame(csv_rows).to_csv(quality_csv, index=False)

        # 4. Lineage manifest
        manifest_data = {
            "pipeline_name": "NOAA_Bronze_To_Silver",
            "pipeline_version": "1.0.0",
            "run_timestamp": run_ts,
            "bronze_snapshot": bronze_dir.name,
            "inputs": [
                {
                    "file": p.name,
                    "size_bytes": p.stat().st_size,
                    "sha256": sha256_file(p),
                }
                for p in daily_source_paths + [enso_path]
            ],
            "outputs": [
                {
                    "file": daily_out.name,
                    "rows": len(daily_df),
                    "start_date": daily_metric.start_date,
                    "end_date": daily_metric.end_date,
                    "size_bytes": daily_out.stat().st_size,
                    "sha256": sha256_file(daily_out),
                },
                {
                    "file": enso_out.name,
                    "rows": len(enso_df),
                    "start_date": enso_metric.start_date,
                    "end_date": enso_metric.end_date,
                    "size_bytes": enso_out.stat().st_size,
                    "sha256": sha256_file(enso_out),
                },
            ],
        }
        manifest_json = self.manifests_dir / "manifest.json"
        manifest_json.write_text(
            json.dumps(manifest_data, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        # 5. Schema metadata
        self._write_schema_metadata()

        logger.info("=" * 64)
        logger.info(
            f"SILVER PIPELINE COMPLETED: climate_daily ({len(daily_df)} rows), "
            f"enso_weekly ({len(enso_df)} rows)"
        )
        logger.info(f"Quality report : {quality_json}")
        logger.info(f"Lineage manifest: {manifest_json}")
        logger.info("=" * 64)
        return summary


def parse_arguments() -> argparse.Namespace:
    repo_root = resolve_repo_root()
    parser = argparse.ArgumentParser(
        description="NOAA Teleconnections Bronze to Silver Parquet Pipeline."
    )
    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=repo_root / "bronze" / "data",
        help="Path to Bronze data root containing ingest_date=* snapshots.",
    )
    parser.add_argument(
        "--snapshot-dir",
        type=Path,
        default=None,
        help="Optional explicit Bronze snapshot directory (overrides latest discovery).",
    )
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=repo_root / "silver" / "data",
        help="Path to output Silver data directory.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing Silver Parquet outputs.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    logger.setLevel(getattr(logging, args.log_level))
    config = PipelineConfig(
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        snapshot_dir=args.snapshot_dir,
        overwrite=args.overwrite,
    )
    pipeline = NOAABronzeToSilverPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
