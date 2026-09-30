"""Regression tests for malformed market data and per-ticker failures."""

import importlib.util
import json
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd


os.environ.setdefault("NOTION_TOKEN", "test-token")
os.environ.setdefault("DATABASE_ID", "test-database")
sys.modules.setdefault("requests", types.ModuleType("requests"))
sys.modules.setdefault("yfinance", types.ModuleType("yfinance"))
spec = importlib.util.spec_from_file_location(
    "update_notion", Path(__file__).resolve().parents[1] / "scripts" / "update_notion.py"
)
updater = importlib.util.module_from_spec(spec)
spec.loader.exec_module(updater)


def history(rows=201):
    index = pd.date_range("2026-01-01", periods=rows, freq="D")
    return pd.DataFrame(
        {"Close": np.linspace(10, 20, rows), "High": np.linspace(11, 21, rows),
         "Low": np.linspace(9, 19, rows), "Volume": np.full(rows, 1000.0)},
        index=index,
    )


class StockDataTests(unittest.TestCase):
    def fetch(self, frame):
        with patch.object(updater.yf, "Ticker", return_value=types.SimpleNamespace(history=lambda **_: frame), create=True):
            return updater.fetch_stock_data("USAR")

    def test_invalid_close_row_does_not_poison_moving_average(self):
        frame = history()
        frame.iloc[0, frame.columns.get_loc("Close")] = np.nan
        data = self.fetch(frame)
        self.assertIsNotNone(data)
        self.assertTrue(np.isfinite(data["ma200"]))
        self.assertEqual(data["data_date"], frame.index[-1].strftime("%Y-%m-%d"))

    def test_invalid_latest_quote_uses_last_valid_trading_day(self):
        frame = history()
        frame.iloc[-1, frame.columns.get_loc("Close")] = np.inf
        data = self.fetch(frame)
        self.assertEqual(data["data_date"], frame.index[-2].strftime("%Y-%m-%d"))
        self.assertEqual(data["close"], float(frame.iloc[-2]["Close"]))

    def test_nonfinite_optional_data_never_reaches_notion_json(self):
        frame = history()
        frame.iloc[-1, frame.columns.get_loc("High")] = np.inf
        frame.iloc[-1, frame.columns.get_loc("Volume")] = np.inf
        data = self.fetch(frame)
        self.assertIsNone(data["vol_ratio"])
        response = types.SimpleNamespace(status_code=200)
        with patch.object(updater.requests, "patch", return_value=response, create=True) as request:
            self.assertTrue(updater.update_page("page-id", "USAR", data))
        json.dumps(request.call_args.kwargs["json"], allow_nan=False)

    def test_all_invalid_quotes_skip_ticker(self):
        frame = history(5)
        frame["Close"] = np.nan
        self.assertIsNone(self.fetch(frame))

    def test_one_ticker_error_does_not_prevent_later_updates(self):
        pages = [
            {"id": "first", "properties": {"ticker": {"type": "title", "title": [{"plain_text": "BAD"}]}}},
            {"id": "second", "properties": {"ticker": {"type": "title", "title": [{"plain_text": "GOOD"}]}}},
        ]
        with (
            patch.object(updater, "get_data_source_ids", return_value=["source"]),
            patch.object(updater, "get_schema_properties", return_value=set()),
            patch.object(updater, "fetch_all_pages", return_value=pages),
            patch.object(updater, "fetch_stock_data", return_value={"close": 10}),
            patch.object(updater, "update_page", side_effect=[ValueError("bad quote"), True]) as update,
            patch.object(updater.time, "sleep"),
        ):
            with self.assertRaises(SystemExit) as exit_result:
                updater.main()
        self.assertEqual(exit_result.exception.code, 1)
        self.assertEqual(update.call_count, 2)


if __name__ == "__main__":
    unittest.main()
