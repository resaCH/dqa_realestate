"""
src/dqa/checks.py
------------------
Datenqualitäts-Checks für Immobiliendaten, validiert gegen GWR-Referenzdaten.

Struktur:
- DQAResult:    Ergebnis eines einzelnen Checks
- DQARunner:    Führt alle Checks durch und aggregiert Ergebnisse
- check_*:      Einzelne Check-Funktionen (erweiterbar)

Checks die bereits implementiert sind:
1. egid_exists           – EGID in GWR vorhanden?
2. gstat_plausibel       – Gebäudestatus konsistent mit Nutzungsdaten?
3. adresse_korrekt       – Strasse/PLZ stimmt mit GWR überein?
4. baujahr_plausibel     – Baujahr im plausiblen Bereich?
5. anzahl_whg_konsistent – Anzahl Wohnungen stimmt mit GWR überein?
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loguru import logger

from src.utils.databricks_client import get_client

GOLD_TABLE = "gold_gwr_current"


# ------------------------------------------------------------------
# Datenstrukturen
# ------------------------------------------------------------------

@dataclass
class DQAResult:
    """Ergebnis eines einzelnen Datenqualitäts-Checks."""
    check_id:       str
    check_name:     str
    egid:           int | None
    passed:         bool
    severity:       str          # 'critical' | 'warning' | 'info'
    detail:         str = ""
    expected:       Any = None   # Erwarteter Wert (GWR)
    actual:         Any = None   # Tatsächlicher Wert (Immobiliendaten)
    checked_at:     str = field(
        default_factory=lambda: datetime.now().isoformat()
    )

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


@dataclass
class DQAReport:
    """Aggregiertes Ergebnis eines vollständigen DQA-Runs."""
    source_system:  str
    run_id:         str
    total_checks:   int = 0
    passed:         int = 0
    failed:         int = 0
    critical_fails: int = 0
    results:        list[DQAResult] = field(default_factory=list)
    started_at:     str = field(
        default_factory=lambda: datetime.now().isoformat()
    )

    @property
    def pass_rate(self) -> float:
        return round(self.passed / self.total_checks * 100, 1) if self.total_checks else 0.0

    def add(self, result: DQAResult) -> None:
        self.results.append(result)
        self.total_checks += 1
        if result.passed:
            self.passed += 1
        else:
            self.failed += 1
            if result.severity == "critical":
                self.critical_fails += 1

    def to_dict(self) -> dict:
        return {
            "source_system":  self.source_system,
            "run_id":         self.run_id,
            "total_checks":   self.total_checks,
            "passed":         self.passed,
            "failed":         self.failed,
            "critical_fails": self.critical_fails,
            "pass_rate":      self.pass_rate,
            "started_at":     self.started_at,
            "results":        [r.__dict__ for r in self.results],
        }


# ------------------------------------------------------------------
# DQA Runner
# ------------------------------------------------------------------

class DQARunner:
    """
    Führt Datenqualitäts-Checks für eine Liste von Immobilien-Datensätzen durch.

    Verwendung:
        runner = DQARunner(source_system="ImmoApp")
        report = runner.run(records)
        print(report.pass_rate)
    """

    def __init__(self, source_system: str = "unknown") -> None:
        self.source_system = source_system
        self.client = get_client()
        self._gwr_cache: dict[int, dict] = {}

    def run(self, records: list[dict]) -> DQAReport:
        """
        Führt alle Checks für alle Records durch.

        Args:
            records: Liste von Immobilien-Datensätzen.
                     Pflichtfeld: 'egid' (int)

        Returns:
            DQAReport mit allen Ergebnissen.
        """
        import uuid
        run_id = str(uuid.uuid4())[:8]
        report = DQAReport(source_system=self.source_system, run_id=run_id)

        logger.info(
            f"DQA Run gestartet | source={self.source_system} | "
            f"records={len(records)} | run_id={run_id}"
        )

        # GWR-Daten für alle EGIDs vorladen (1 SQL statt N)
        egids = [r["egid"] for r in records if r.get("egid")]
        self._preload_gwr(egids)

        for record in records:
            egid = record.get("egid")
            gwr = self._gwr_cache.get(egid) if egid else None

            report.add(check_egid_exists(record, gwr))
            report.add(check_adresse_korrekt(record, gwr))
            report.add(check_baujahr_plausibel(record, gwr))
            report.add(check_gstat_plausibel(record, gwr))
            report.add(check_anzahl_whg_konsistent(record, gwr))

        logger.info(
            f"DQA Run abgeschlossen | pass_rate={report.pass_rate}% | "
            f"total={report.total_checks} | failed={report.failed} | "
            f"critical={report.critical_fails}"
        )
        return report

    def _preload_gwr(self, egids: list[int]) -> None:
        """Lädt GWR-Daten für alle angegebenen EGIDs in einem Batch."""
        if not egids:
            return
        ids_str = ", ".join(str(e) for e in egids)
        try:
            rows = self.client.execute_sql(f"""
                SELECT *
                FROM {self.client.catalog}.{self.client.schema}.{GOLD_TABLE}
                WHERE egid IN ({ids_str})
            """)
            self._gwr_cache = {int(r["egid"]): r for r in rows}
            logger.debug(
                f"GWR-Cache geladen: {len(self._gwr_cache)}/{len(egids)} EGIDs gefunden"
            )
        except Exception as exc:
            logger.error(f"GWR-Daten konnten nicht geladen werden: {exc}")


# ------------------------------------------------------------------
# Einzelne Check-Funktionen
# ------------------------------------------------------------------

def check_egid_exists(record: dict, gwr: dict | None) -> DQAResult:
    """Check: Ist die EGID im GWR registriert?"""
    egid = record.get("egid")
    passed = gwr is not None
    return DQAResult(
        check_id="GWR-01",
        check_name="EGID im GWR vorhanden",
        egid=egid,
        passed=passed,
        severity="critical",
        detail="" if passed else f"EGID {egid} nicht im GWR gefunden oder gelöscht.",
        expected="vorhanden",
        actual="vorhanden" if passed else "nicht gefunden",
    )


def check_adresse_korrekt(record: dict, gwr: dict | None) -> DQAResult:
    """Check: Stimmt die Adresse (PLZ + Strassenname) mit GWR überein?"""
    egid = record.get("egid")
    if not gwr:
        return _skipped("GWR-02", "Adresse korrekt", egid)

    plz_match = str(record.get("plz", "")).strip() == str(gwr.get("plz4", "")).strip()
    str_match = _normalize_str(record.get("strasse", "")) == _normalize_str(
        gwr.get("strname", "")
    )
    passed = plz_match and str_match
    return DQAResult(
        check_id="GWR-02",
        check_name="Adresse korrekt (PLZ + Strasse)",
        egid=egid,
        passed=passed,
        severity="warning",
        detail=(
            ""
            if passed
            else (
                f"PLZ: {record.get('plz')} vs. GWR: {gwr.get('plz4')} | "
                f"Strasse: {record.get('strasse')} vs. GWR: {gwr.get('strname')}"
            )
        ),
        expected=f"{gwr.get('plz4')} {gwr.get('strname')}",
        actual=f"{record.get('plz')} {record.get('strasse')}",
    )


def check_baujahr_plausibel(record: dict, gwr: dict | None) -> DQAResult:
    """Check: Ist das Baujahr im plausiblen Bereich und stimmt mit GWR überein?"""
    egid = record.get("egid")
    baujahr = record.get("baujahr")

    if baujahr is None:
        return DQAResult(
            check_id="GWR-03",
            check_name="Baujahr plausibel",
            egid=egid,
            passed=False,
            severity="warning",
            detail="Kein Baujahr im Immobilien-Datensatz vorhanden.",
        )

    # Plausibilitätsprüfung: 1700 – aktuelles Jahr
    current_year = datetime.now().year
    range_ok = 1700 <= int(baujahr) <= current_year

    # GWR-Abgleich (wenn vorhanden)
    gwr_baujahr = gwr.get("gbauj") if gwr else None
    gwr_match = gwr_baujahr is None or int(baujahr) == int(gwr_baujahr)

    passed = range_ok and gwr_match
    return DQAResult(
        check_id="GWR-03",
        check_name="Baujahr plausibel und GWR-konform",
        egid=egid,
        passed=passed,
        severity="warning",
        detail=(
            ""
            if passed
            else (
                f"Baujahr {baujahr} ausserhalb Bereich (1700-{current_year})"
                if not range_ok
                else f"Baujahr {baujahr} weicht von GWR ab ({gwr_baujahr})"
            )
        ),
        expected=gwr_baujahr,
        actual=baujahr,
    )


def check_gstat_plausibel(record: dict, gwr: dict | None) -> DQAResult:
    """
    Check: Ist der Gebäudestatus konsistent?
    Vermietete/verkaufte Objekte sollten gstat=4 (bewohnt) haben.
    """
    egid = record.get("egid")
    if not gwr:
        return _skipped("GWR-04", "Gebäudestatus plausibel", egid)

    gstat = gwr.get("gstat")
    nutzung = record.get("nutzungsart", "")

    # Kritisch: Gebäude im Bau oder geplant aber als vermietet erfasst
    critical_states = {1, 2, 3}  # geplant, bewilligt, im Bau
    passed = gstat not in critical_states or "vermietet" not in str(nutzung).lower()

    return DQAResult(
        check_id="GWR-04",
        check_name="Gebäudestatus konsistent mit Nutzung",
        egid=egid,
        passed=passed,
        severity="critical",
        detail=(
            ""
            if passed
            else f"Gebäude hat GWR-Status {gstat} ({gwr.get('gstat_label', '')}), "
                 f"aber Nutzungsart ist '{nutzung}'."
        ),
        expected="gstat=4 für bewohntes Objekt",
        actual=f"gstat={gstat}",
    )


def check_anzahl_whg_konsistent(record: dict, gwr: dict | None) -> DQAResult:
    """Check: Stimmt die Wohnungsanzahl mit GWR überein (Toleranz ±10%)?"""
    egid = record.get("egid")
    if not gwr:
        return _skipped("GWR-05", "Anzahl Wohnungen konsistent", egid)

    gwr_whg = gwr.get("ganzwhg")
    rec_whg = record.get("anzahl_wohnungen")

    if rec_whg is None or gwr_whg is None:
        return DQAResult(
            check_id="GWR-05",
            check_name="Anzahl Wohnungen konsistent",
            egid=egid,
            passed=True,
            severity="info",
            detail="Wohnungsanzahl nicht in beiden Quellen vorhanden — übersprungen.",
        )

    diff_pct = abs(int(rec_whg) - int(gwr_whg)) / max(int(gwr_whg), 1) * 100
    passed = diff_pct <= 10  # 10% Toleranz

    return DQAResult(
        check_id="GWR-05",
        check_name="Anzahl Wohnungen konsistent (±10%)",
        egid=egid,
        passed=passed,
        severity="warning",
        detail=(
            ""
            if passed
            else f"Abweichung: {rec_whg} vs. GWR: {gwr_whg} ({diff_pct:.1f}%)"
        ),
        expected=gwr_whg,
        actual=rec_whg,
    )


# ------------------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------------------

def _skipped(check_id: str, check_name: str, egid: int | None) -> DQAResult:
    """Erzeugt einen 'übersprungen'-Result wenn GWR-Daten fehlen."""
    return DQAResult(
        check_id=check_id,
        check_name=check_name,
        egid=egid,
        passed=True,
        severity="info",
        detail="Kein GWR-Record vorhanden — Check übersprungen.",
    )


def _normalize_str(s: str) -> str:
    """Normalisiert einen String für den Vergleich (lower, strip, Sonderzeichen)."""
    return (
        str(s)
        .lower()
        .strip()
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("-", " ")
        .replace(".", "")
        .replace(",", "")
    )
