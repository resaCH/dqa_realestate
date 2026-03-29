"""
tests/test_gwr_fetcher.py
--------------------------
Unit-Tests für den GWR API Fetcher.
Verwendet 'responses' für HTTP-Mocking (kein echter API-Aufruf).
"""

from __future__ import annotations

import pytest
import responses as responses_lib

from src.ingestion.gwr_fetcher import GWRFetcher, _int, _float


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

MOCK_BUILDING = {
    "egid": "123456",
    "egrid": "CH123456789012",
    "gdekt": "AG",
    "ggdenr": "4001",
    "ggdename": "Aarau",
    "strname": "Bahnhofstrasse",
    "deinr": "12",
    "plz4": "5000",
    "plzname": "Aarau",
    "gstat": "4",
    "gkat": "1021",
    "gklas": "1110",
    "gbauj": "1985",
    "gbaum": "6",
    "gbaup": "8070",
    "gabbj": None,
    "garea": "240.5",
    "gvol": "850.0",
    "ganzwhg": "4",
    "gazzi": "12",
    "gkode": "2646123.5",
    "gkodn": "1249876.0",
    "gschutzr": "1",
    "gdatns": "2005-03-15",
    "gdat": "2024-06-20",
}


@pytest.fixture
def fetcher():
    return GWRFetcher(kanton="AG")


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

class TestGWRFetcher:

    @responses_lib.activate
    def test_fetch_buildings_single_batch(self, fetcher):
        """Fetcht eine Seite mit einem Gebäude."""
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            json={"buildings": [MOCK_BUILDING]},
            status=200,
        )
        # Zweite Seite leer → Pagination-Ende
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            json={"buildings": []},
            status=200,
        )

        records = list(fetcher.fetch_buildings())
        assert len(records) == 1
        assert records[0]["egid"] == 123456
        assert records[0]["gdekt"] == "AG"
        assert records[0]["gstat"] == 4

    @responses_lib.activate
    def test_fetch_buildings_with_since_date(self, fetcher):
        """Übergibt since_date als Query-Parameter."""
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            json={"buildings": [MOCK_BUILDING]},
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            json={"buildings": []},
            status=200,
        )

        records = list(fetcher.fetch_buildings(since_date="2024-01-01"))
        assert len(records) == 1
        # Prüfen ob der Parameter übergeben wurde
        assert "gdatMin=2024-01-01" in responses_lib.calls[0].request.url

    @responses_lib.activate
    def test_fetch_buildings_max_records(self, fetcher):
        """Bricht nach max_records ab."""
        # 3 Seiten mit je 1 Record
        for _ in range(3):
            responses_lib.add(
                responses_lib.GET,
                f"{fetcher.BASE_URL}/buildings",
                json={"buildings": [MOCK_BUILDING] * fetcher.PAGE_SIZE},
                status=200,
            )

        records = list(fetcher.fetch_buildings(max_records=2))
        assert len(records) == 2

    @responses_lib.activate
    def test_retry_on_timeout(self, fetcher):
        """Retry-Logik bei Timeout."""
        import requests

        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            body=requests.Timeout(),
        )
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            json={"buildings": [MOCK_BUILDING]},
            status=200,
        )
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            json={"buildings": []},
            status=200,
        )

        records = list(fetcher.fetch_buildings())
        assert len(records) == 1

    @responses_lib.activate
    def test_empty_response(self, fetcher):
        """Leere API-Antwort → 0 Records."""
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            json={"buildings": []},
            status=200,
        )

        records = list(fetcher.fetch_buildings())
        assert records == []

    def test_normalize_building_types(self, fetcher):
        """Normalisierung konvertiert Strings zu korrekten Typen."""
        normalized = fetcher._normalize_building(MOCK_BUILDING)
        assert isinstance(normalized["egid"], int)
        assert isinstance(normalized["garea"], float)
        assert normalized["gabbj"] is None  # None bleibt None

    def test_normalize_building_missing_fields(self, fetcher):
        """Fehlende Felder ergeben None."""
        normalized = fetcher._normalize_building({})
        assert normalized["egid"] is None
        assert normalized["gstat"] is None
        assert normalized["gdat"] is None

    @responses_lib.activate
    def test_connection_ok(self, fetcher):
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            json={"buildings": [MOCK_BUILDING]},
            status=200,
        )
        assert fetcher.test_connection() is True

    @responses_lib.activate
    def test_connection_fail(self, fetcher):
        responses_lib.add(
            responses_lib.GET,
            f"{fetcher.BASE_URL}/buildings",
            status=500,
        )
        assert fetcher.test_connection() is False


# ------------------------------------------------------------------
# Tests für Hilfsfunktionen
# ------------------------------------------------------------------

class TestTypeHelpers:

    def test_int_valid(self):
        assert _int("123") == 123
        assert _int(456) == 456

    def test_int_none(self):
        assert _int(None) is None
        assert _int("") is None
        assert _int("abc") is None

    def test_float_valid(self):
        assert _float("1.5") == 1.5
        assert _float(2) == 2.0

    def test_float_none(self):
        assert _float(None) is None
        assert _float("xyz") is None
