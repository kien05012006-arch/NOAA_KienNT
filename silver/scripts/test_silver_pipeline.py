#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit and Integration Tests for NOAA Teleconnections Pipeline
============================================================
Tests fixed-width ENSO parsing (including glued negative SSTA values),
temporal continuity checks, NOAA sentinel rejection, Wednesday validation,
and Silver-to-Gold rolling window & Tuesday alignment rules.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(REPO_ROOT / "gold" / "scripts"))

from noaa_bronze_to_silver import read_daily_csv, read_enso_for, validate_table
from noaa_silver_to_gold import build_gold


class TestValidationRules(unittest.TestCase):
    def test_validate_daily_continuous_passes(self):
        df = pd.DataFrame(
            {
                "date": pd.date_range("2020-01-01", periods=5, freq="D"),
                "ao": [0.1, -0.2, 0.3, 0.4, -0.5],
            }
        )
        out = validate_table(df, "date", "D")
        self.assertEqual(len(out), 5)

    def test_validate_rejects_date_gap(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-04"]),
                "ao": [0.1, 0.2, 0.3],
            }
        )
        with self.assertRaises(ValueError):
            validate_table(df, "date", "D")

    def test_validate_rejects_duplicates(self):
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
                "ao": [0.1, 0.2],
            }
        )
        with self.assertRaises(ValueError):
            validate_table(df, "date", "D")

    def test_validate_rejects_noaa_sentinels_and_nan(self):
        df_sentinel = pd.DataFrame(
            {
                "date": pd.date_range("2020-01-01", periods=2, freq="D"),
                "ao": [0.5, -99.9],
            }
        )
        with self.assertRaises(ValueError):
            validate_table(df_sentinel, "date", "D")

        df_nan = pd.DataFrame(
            {
                "date": pd.date_range("2020-01-01", periods=2, freq="D"),
                "ao": [0.5, np.nan],
            }
        )
        with self.assertRaises(ValueError):
            validate_table(df_nan, "date", "D")


class TestEnsoFixedWidthParser(unittest.TestCase):
    def test_parse_glued_negative_ssta_trimmed_format(self):
        """Values like '25.7-0.9' without whitespace must parse cleanly."""
        sample = "\n".join(
            [
                " Weekly SST data starts week centered on 2Sept1981",
                "",
                "               Nino34",
                " Week             SST SSTA",
                " 14DEC2022       25.7-0.9",
                " 21DEC2022       25.8-0.8",
                " 28DEC2022       25.9 0.0",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "wksst9120.for"
            p.write_text(sample + "\n", encoding="utf-8")
            df = read_enso_for(p)
        self.assertEqual(len(df), 3)
        self.assertAlmostEqual(df.loc[0, "nino34_sst"], 25.7)
        self.assertAlmostEqual(df.loc[0, "nino34_ssta"], -0.9)
        self.assertAlmostEqual(df.loc[1, "nino34_ssta"], -0.8)
        self.assertAlmostEqual(df.loc[2, "nino34_ssta"], 0.0)
        self.assertTrue((df["source_week_date"].dt.dayofweek == 2).all())

    def test_reject_non_wednesday_enso(self):
        sample = "\n".join(
            [
                " Weekly SST data starts week centered on 2Sept1981",
                "",
                "               Nino34",
                " Week             SST SSTA",
                " 13DEC2022       25.7-0.9",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "wksst9120.for"
            p.write_text(sample + "\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                read_enso_for(p)

    def test_parse_daily_csv_valid(self):
        sample = "\n".join(
            [
                "year,month,day,ao_index_cdas",
                "2020,1,1,1.25",
                "2020,1,2,-0.75",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "ao.csv"
            p.write_text(sample + "\n", encoding="utf-8")
            df = read_daily_csv(p, "ao")
        self.assertEqual(list(df.columns), ["date", "ao"])
        self.assertEqual(len(df), 2)


class TestGoldAggregationLogic(unittest.TestCase):
    def test_tuesday_7day_and_wednesday_enso_rolling(self):
        # Daily: 2019-12-25 .. 2020-01-14
        dates = pd.date_range("2019-12-25", "2020-01-14", freq="D")
        daily = pd.DataFrame(
            {
                "date": dates,
                "ao": np.arange(1, len(dates) + 1, dtype=float),
                "nao": np.full(len(dates), 1.0),
                "pna": np.full(len(dates), -0.5),
            }
        )
        # ENSO Wednesdays: 13 consecutive Wednesdays ending 2020-01-15
        weds = pd.date_range("2019-10-23", "2020-01-15", freq="7D")
        ssta_vals = np.arange(1, len(weds) + 1, dtype=float) / 10.0  # 0.1 .. 1.3
        enso = pd.DataFrame(
            {
                "source_week_date": weds,
                "nino34_sst": np.full(len(weds), 27.0),
                "nino34_ssta": ssta_vals,
            }
        )
        weeks = pd.DatetimeIndex(["2020-01-07", "2020-01-14"])
        gold = build_gold(daily, enso, weeks)

        self.assertEqual(len(gold), 2)
        # 2020-01-07 is day index 13 (1-based value 14). Trailing 7 days: 8..14 -> mean = 11.0
        self.assertAlmostEqual(gold.loc[0, "ao"], 11.0)
        self.assertAlmostEqual(gold.loc[0, "nao"], 1.0)
        self.assertAlmostEqual(gold.loc[0, "pna"], -0.5)
        # 2020-01-07 pairs with Wednesday 2020-01-08 (12th obs -> 1.2)
        self.assertAlmostEqual(gold.loc[0, "enso"], 1.2)
        self.assertAlmostEqual(gold.loc[0, "enso14"], round(np.mean(ssta_vals[10:12]), 3))
        self.assertAlmostEqual(gold.loc[0, "enso28"], round(np.mean(ssta_vals[8:12]), 3))
        self.assertAlmostEqual(gold.loc[0, "enso84"], round(np.mean(ssta_vals[0:12]), 3))

    def test_incomplete_window_raises(self):
        # Only 5 days before first Tuesday -> must fail instead of partial average
        dates = pd.date_range("2020-01-03", "2020-01-07", freq="D")
        daily = pd.DataFrame({"date": dates, "ao": 1.0, "nao": 1.0, "pna": 1.0})
        weds = pd.date_range("2019-10-23", "2020-01-08", freq="7D")
        enso = pd.DataFrame(
            {"source_week_date": weds, "nino34_sst": 27.0, "nino34_ssta": 0.5}
        )
        with self.assertRaises(ValueError):
            build_gold(daily, enso, pd.DatetimeIndex(["2020-01-07"]))


if __name__ == "__main__":
    unittest.main()
