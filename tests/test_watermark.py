"""
tests/test_watermark.py
------------------------
Unit-Tests für den WatermarkManager.
"""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from src.ingestion.gwr_incremental import WatermarkManager, INITIAL_DATE


@pytest.fixture
def tmp_watermark(tmp_path):
    """WatermarkManager mit temporärem Pfad."""
    wm = WatermarkManager(use_databricks=False)
    wm._local_path = tmp_path / "watermark.json"
    return wm


class TestWatermarkManager:

    def test_initial_date_ohne_datei(self, tmp_watermark):
        date = tmp_watermark.get_last_run_date()
        assert date == INITIAL_DATE

    def test_update_setzt_neues_datum(self, tmp_watermark):
        tmp_watermark.update("2024-06-01")
        assert tmp_watermark.get_last_run_date() == "2024-06-01"

    def test_update_nur_vorwaerts(self, tmp_watermark):
        tmp_watermark.update("2024-06-01")
        tmp_watermark.update("2024-01-01")  # Älter → wird ignoriert
        assert tmp_watermark.get_last_run_date() == "2024-06-01"

    def test_persistenz_ueber_reload(self, tmp_watermark):
        tmp_watermark.update("2024-09-15")
        # Neues Objekt mit gleichem Pfad
        wm2 = WatermarkManager(use_databricks=False)
        wm2._local_path = tmp_watermark._local_path
        assert wm2.get_last_run_date() == "2024-09-15"

    def test_korrupte_datei_fallback(self, tmp_watermark):
        tmp_watermark._local_path.write_text("KEIN_JSON", encoding="utf-8")
        date = tmp_watermark.get_last_run_date()
        assert date == INITIAL_DATE

    def test_suggest_safe_watermark(self, tmp_watermark):
        records = [
            {"gdat": "2024-08-20"},
            {"gdat": "2024-08-22"},
            {"gdat": "2024-08-21"},
        ]
        safe = tmp_watermark.suggest_safe_watermark(records)
        assert safe == "2024-08-21"  # max(2024-08-22) - 1 Tag

    def test_suggest_safe_watermark_leer(self, tmp_watermark):
        safe = tmp_watermark.suggest_safe_watermark([])
        assert safe == INITIAL_DATE
