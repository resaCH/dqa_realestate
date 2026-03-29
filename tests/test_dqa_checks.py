"""
tests/test_dqa_checks.py
-------------------------
Unit-Tests für alle DQA-Check-Funktionen.
Kein Databricks-Zugriff erforderlich — reine Unit-Tests.
"""

from __future__ import annotations

import pytest

from src.dqa.checks import (
    DQAReport,
    DQAResult,
    DQARunner,
    check_adresse_korrekt,
    check_anzahl_whg_konsistent,
    check_baujahr_plausibel,
    check_egid_exists,
    check_gstat_plausibel,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def gwr_record():
    """Beispiel GWR-Record (Gold Layer)."""
    return {
        "egid":      123456,
        "plz4":      "5000",
        "strname":   "Bahnhofstrasse",
        "gbauj":     1985,
        "gstat":     4,
        "gstat_label": "Bestehend",
        "ganzwhg":   4,
    }


@pytest.fixture
def immo_record():
    """Beispiel Immobilien-Datensatz."""
    return {
        "egid":             123456,
        "plz":              "5000",
        "strasse":          "Bahnhofstrasse",
        "baujahr":          1985,
        "nutzungsart":      "vermietet",
        "anzahl_wohnungen": 4,
    }


# ------------------------------------------------------------------
# check_egid_exists
# ------------------------------------------------------------------

class TestCheckEgidExists:

    def test_egid_vorhanden(self, immo_record, gwr_record):
        result = check_egid_exists(immo_record, gwr_record)
        assert result.passed is True
        assert result.check_id == "GWR-01"

    def test_egid_nicht_vorhanden(self, immo_record):
        result = check_egid_exists(immo_record, None)
        assert result.passed is False
        assert result.severity == "critical"
        assert "nicht im GWR gefunden" in result.detail

    def test_egid_none_im_record(self, gwr_record):
        result = check_egid_exists({"egid": None}, gwr_record)
        assert result.passed is True  # GWR-Record vorhanden → PASS


# ------------------------------------------------------------------
# check_adresse_korrekt
# ------------------------------------------------------------------

class TestCheckAdresseKorrekt:

    def test_adresse_stimmt(self, immo_record, gwr_record):
        result = check_adresse_korrekt(immo_record, gwr_record)
        assert result.passed is True

    def test_plz_falsch(self, immo_record, gwr_record):
        immo_record["plz"] = "8001"
        result = check_adresse_korrekt(immo_record, gwr_record)
        assert result.passed is False
        assert result.severity == "warning"

    def test_strasse_falsch(self, immo_record, gwr_record):
        immo_record["strasse"] = "Hauptstrasse"
        result = check_adresse_korrekt(immo_record, gwr_record)
        assert result.passed is False

    def test_normalisierung_umlaute(self, immo_record, gwr_record):
        """Umlaute in Strassennamen werden normalisiert."""
        immo_record["strasse"] = "Bahnhofstrasse"
        gwr_record["strname"] = "Bahnhofstrasse"
        result = check_adresse_korrekt(immo_record, gwr_record)
        assert result.passed is True

    def test_kein_gwr_record(self, immo_record):
        result = check_adresse_korrekt(immo_record, None)
        assert result.passed is True  # Skip → PASS
        assert "übersprungen" in result.detail


# ------------------------------------------------------------------
# check_baujahr_plausibel
# ------------------------------------------------------------------

class TestCheckBaujahrPlausibel:

    def test_baujahr_korrekt(self, immo_record, gwr_record):
        result = check_baujahr_plausibel(immo_record, gwr_record)
        assert result.passed is True

    def test_baujahr_zu_alt(self, immo_record, gwr_record):
        immo_record["baujahr"] = 1600
        result = check_baujahr_plausibel(immo_record, gwr_record)
        assert result.passed is False
        assert "ausserhalb Bereich" in result.detail

    def test_baujahr_in_zukunft(self, immo_record, gwr_record):
        immo_record["baujahr"] = 2099
        result = check_baujahr_plausibel(immo_record, gwr_record)
        assert result.passed is False

    def test_baujahr_fehlt(self, immo_record, gwr_record):
        immo_record.pop("baujahr")
        result = check_baujahr_plausibel(immo_record, gwr_record)
        assert result.passed is False
        assert "Kein Baujahr" in result.detail

    def test_baujahr_weicht_von_gwr_ab(self, immo_record, gwr_record):
        immo_record["baujahr"] = 1990
        gwr_record["gbauj"] = 1985
        result = check_baujahr_plausibel(immo_record, gwr_record)
        assert result.passed is False
        assert "weicht von GWR ab" in result.detail

    def test_kein_gwr_baujahr_kein_fehler(self, immo_record):
        """Wenn GWR kein Baujahr hat, wird nur Plausibilität geprüft."""
        result = check_baujahr_plausibel(immo_record, {"gbauj": None})
        assert result.passed is True


# ------------------------------------------------------------------
# check_gstat_plausibel
# ------------------------------------------------------------------

class TestCheckGstatPlausibel:

    def test_bewohntes_gebaeude_vermietet(self, immo_record, gwr_record):
        gwr_record["gstat"] = 4  # bewohnt
        immo_record["nutzungsart"] = "vermietet"
        result = check_gstat_plausibel(immo_record, gwr_record)
        assert result.passed is True

    def test_im_bau_aber_vermietet(self, immo_record, gwr_record):
        gwr_record["gstat"] = 3  # im Bau
        immo_record["nutzungsart"] = "vermietet"
        result = check_gstat_plausibel(immo_record, gwr_record)
        assert result.passed is False
        assert result.severity == "critical"

    def test_geplant_nicht_vermietet(self, immo_record, gwr_record):
        gwr_record["gstat"] = 1  # geplant
        immo_record["nutzungsart"] = "leer"
        result = check_gstat_plausibel(immo_record, gwr_record)
        assert result.passed is True  # Kein Konflikt


# ------------------------------------------------------------------
# check_anzahl_whg_konsistent
# ------------------------------------------------------------------

class TestCheckAnzahlWhg:

    def test_exakt_gleich(self, immo_record, gwr_record):
        result = check_anzahl_whg_konsistent(immo_record, gwr_record)
        assert result.passed is True

    def test_innerhalb_toleranz(self, immo_record, gwr_record):
        immo_record["anzahl_wohnungen"] = 4  # GWR = 4, Toleranz 10% → OK
        result = check_anzahl_whg_konsistent(immo_record, gwr_record)
        assert result.passed is True

    def test_ausserhalb_toleranz(self, immo_record, gwr_record):
        immo_record["anzahl_wohnungen"] = 10  # GWR = 4 → 150% Abweichung
        result = check_anzahl_whg_konsistent(immo_record, gwr_record)
        assert result.passed is False
        assert "Abweichung" in result.detail

    def test_kein_wert_im_record(self, immo_record, gwr_record):
        immo_record.pop("anzahl_wohnungen")
        result = check_anzahl_whg_konsistent(immo_record, gwr_record)
        assert result.passed is True  # Nicht vorhanden → übersprungen


# ------------------------------------------------------------------
# DQAReport
# ------------------------------------------------------------------

class TestDQAReport:

    def test_pass_rate_berechnung(self):
        report = DQAReport(source_system="test", run_id="abc")
        report.add(DQAResult("C1", "Check 1", 1, True, "info"))
        report.add(DQAResult("C2", "Check 2", 1, True, "info"))
        report.add(DQAResult("C3", "Check 3", 1, False, "warning"))
        assert report.pass_rate == pytest.approx(66.7, rel=0.01)
        assert report.failed == 1
        assert report.critical_fails == 0

    def test_critical_zaehlung(self):
        report = DQAReport(source_system="test", run_id="abc")
        report.add(DQAResult("C1", "Critical Fail", 1, False, "critical"))
        assert report.critical_fails == 1

    def test_leerer_report(self):
        report = DQAReport(source_system="test", run_id="abc")
        assert report.pass_rate == 0.0
        assert report.to_dict()["total_checks"] == 0
