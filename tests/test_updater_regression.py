"""Regression tests for invalid Yahoo market data and Notion API failures."""
import json
import os
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

os.environ.setdefault("NOTION_TOKEN", "offline-test-only")
os.environ.setdefault("DATABASE_ID", "offline-test-only")
from scripts import update_notion as updater


def history(rows=210):
    dates = pd.date_range("2026-01-01", periods=rows, freq="B")
    close = pd.Series(np.linspace(100., 150., rows), index=dates)
    return pd.DataFrame({
        "Close": close, "High": close + 1, "Low": close - 1,
        "Volume": pd.Series(np.full(rows, 1000.), index=dates),
    })


class UpdaterRegressionTests(unittest.TestCase):
    def yahoo(self, dataframe):
        fake = Mock()
        fake.history.return_value = dataframe
        return patch.object(updater.yf, "Ticker", return_value=fake)

    def test_missing_historical_close_never_produces_nan_ma(self):
        h = history(200)
        h.iloc[25, h.columns.get_loc("Close")] = np.nan
        with self.yahoo(h):
            result = updater.fetch_stock_data("GS")
        self.assertIsNotNone(result)
        self.assertIsNone(result["ma200"])
        self.assertTrue(np.isfinite(result["close"]))

    def test_missing_high_low_volume_is_not_fatal(self):
        h = history()
        h["High"] = np.nan
        h["Low"] = np.inf
        h["Volume"] = np.inf
        with self.yahoo(h):
            data = updater.fetch_stock_data("MDB")
        self.assertIsNotNone(data)
        self.assertEqual(data["pullback_threshold"], updater.PULLBACK_THRESHOLD_FALLBACK)
        self.assertIsNone(data["vol_ratio"])

    def test_bad_latest_close_is_not_replaced_by_stale_quote(self):
        h = history()
        h.iloc[-1, h.columns.get_loc("Close")] = np.nan
        with self.yahoo(h):
            self.assertIsNone(updater.fetch_stock_data("USAR"))

    def test_infinite_latest_close_is_rejected(self):
        h = history()
        h.iloc[-1, h.columns.get_loc("Close")] = np.inf
        with self.yahoo(h):
            self.assertIsNone(updater.fetch_stock_data("GS"))

    def test_bad_moving_averages_omitted_from_notion_json(self):
        data = {"close": 101.2, "data_date": "2026-10-22",
                "ma20": 100., "ma50": np.nan, "ma200": np.inf}
        with patch.object(updater.requests, "patch", return_value=Mock(status_code=200)) as http:
            self.assertTrue(updater.update_page("page-id", "GS", data))
        payload = http.call_args.kwargs["json"]
        json.dumps(payload, allow_nan=False)
        self.assertNotIn("MA50", payload["properties"])
        self.assertNotIn("MA200", payload["properties"])
        self.assertEqual(http.call_args.kwargs["timeout"], 30)

    def test_invalid_latest_price_never_calls_notion(self):
        with patch.object(updater.requests, "patch") as http:
            self.assertFalse(updater.update_page("page-id", "GS", {"close": np.nan}))
        http.assert_not_called()

    def test_http_400_not_counted_as_success(self):
        resp = Mock(status_code=400)
        resp.json.return_value = {"message": "invalid schema"}
        data = {"close": 101., "data_date": "2026-10-22"}
        with patch.object(updater.requests, "patch", return_value=resp):
            self.assertFalse(updater.update_page("page-id", "GS", data))

    def test_timeout_not_counted_as_success(self):
        data = {"close": 101., "data_date": "2026-10-22"}
        with patch.object(updater.requests, "patch", side_effect=updater.requests.Timeout("timed out")):
            self.assertFalse(updater.update_page("page-id", "GS", data))

    def test_second_ticker_runs_even_if_first_crashes(self):
        pages = [
            {"id": "one", "properties": {"ticker": {"type": "title", "title": [{"plain_text": "GS"}]}}},
            {"id": "two", "properties": {"ticker": {"type": "title", "title": [{"plain_text": "MDB"}]}}},
        ]
        with (patch.object(updater, "get_data_source_ids", return_value=["ds"]),
              patch.object(updater, "get_schema_properties", return_value=set()),
              patch.object(updater, "fetch_all_pages", return_value=pages),
              patch.object(updater, "fetch_stock_data", side_effect=[{"close": 100}, {"close": 200}]),
              patch.object(updater, "update_page", side_effect=[ValueError("bad GS"), True]) as update,
              patch.object(updater.time, "sleep")):
            with self.assertRaises(SystemExit) as caught:
                updater.main()
        self.assertEqual(caught.exception.code, 1)
        self.assertEqual(update.call_count, 2)


if __name__ == "__main__":
    unittest.main()
