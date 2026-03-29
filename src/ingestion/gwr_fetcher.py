"""
src/ingestion/gwr_fetcher.py
-----------------------------
Client für das Gebäude- und Wohnungsregister (GWR).
Endpunkt: https://madd.bfs.admin.ch/eCH-0206?egid=<EGID>
Liefert XML (eCH-0206), wird zu dict normalisiert.

Für Bulk-Ingestion: EGID-Range iterieren (EGIDs sind sequenziell 1–n).
Für DQA-Checks: Einzelabruf pro EGID.
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from typing import Iterator

import requests
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.utils.config import settings

BASE_URL = "https://madd.bfs.admin.ch/eCH-0206"
NS = {
    "e": "http://www.ech.ch/xmlns/eCH-0206/2",
    "e129": "http://www.ech.ch/xmlns/eCH-0129/5",
}


class GWRFetcher:
    """
    Fetcht GWR-Daten via eCH-0206 XML-API.

    Einzelabruf:  fetch_building(egid)
    Bulk-Abruf:   fetch_buildings(egid_from, egid_to)  — iteriert Range
    """

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/xml",
            "User-Agent": "dqa-realestate/1.0",
        })

    # ------------------------------------------------------------------
    # Öffentliche API
    # ------------------------------------------------------------------

    def fetch_building(self, egid: int) -> dict | None:
        """
        Gibt einen normalisierten dict für eine EGID zurück.
        None wenn EGID nicht existiert oder gelöscht.
        """
        try:
            xml_text = self._get(egid)
            return self._parse(xml_text)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise
        except Exception as exc:
            logger.warning(f"EGID {egid}: Fehler — {exc}")
            return None

    def fetch_buildings(
        self,
        egid_from: int = 1,
        egid_to: int = 1000,
        skip_missing: bool = True,
    ) -> Iterator[dict]:
        """
        Iteriert EGIDs von egid_from bis egid_to und liefert Records.
        Fehlende/gelöschte EGIDs werden übersprungen (skip_missing=True).
        """
        found = 0
        missing = 0
        for egid in range(egid_from, egid_to + 1):
            record = self.fetch_building(egid)
            if record is None:
                missing += 1
                if not skip_missing:
                    yield {"egid": egid, "_not_found": True}
            else:
                found += 1
                yield record
            # Rate-Limiting: max ~5 Requests/Sekunde
            time.sleep(0.2)
            if egid % 100 == 0:
                logger.info(
                    f"Fortschritt: EGID {egid}/{egid_to} | "
                    f"gefunden={found} | fehlend={missing}"
                )

    def test_connection(self) -> bool:
        """Testet die API mit EGID 20 (immer vorhanden)."""
        try:
            result = self.fetch_building(20)
            ok = result is not None
            if ok:
                logger.info(
                    f"GWR API OK — EGID 20: "
                    f"{result.get('strname')} {result.get('deinr')}, "
                    f"{result.get('plz4')} {result.get('plzname')}"
                )
            return ok
        except Exception as exc:
            logger.error(f"GWR API-Verbindung fehlgeschlagen: {exc}")
            return False

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(4),
    )
    def _get(self, egid: int) -> str:
        resp = self.session.get(
            BASE_URL,
            params={"egid": egid},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.text

    # ------------------------------------------------------------------
    # XML → dict
    # ------------------------------------------------------------------

    def _parse(self, xml_text: str) -> dict | None:
        """Parst die eCH-0206 XML-Antwort in einen flachen dict."""
        root = ET.fromstring(xml_text)

        # Status prüfen
        code = root.findtext("e:status/e:code", namespaces=NS)
        if code != "100":
            return None

        item = root.find(".//e:buildingItem", NS)
        if item is None:
            return None

        egid = _int(item.findtext("e:EGID", namespaces=NS))
        b = item.find("e:building", NS)
        muni = item.find("e:municipality", NS)
        estate = item.find(".//e:realestateIdentificationItem", NS)
        entrance = item.find(".//e:buildingEntranceItem/e:buildingEntrance", NS)
        street = item.find(".//e:buildingEntrance/e:street", NS)
        locality = item.find(".//e:buildingEntrance/e:locality", NS)

        # Wohnungen zählen
        dwellings = item.findall(".//e:dwellingItem", NS)

        record = {
            # Identifikatoren
            "egid":      egid,
            "egrid":     estate.findtext("e:EGRID", namespaces=NS) if estate is not None else None,
            # Gemeinde
            "ggdenr":    _int(muni.findtext("e:municipalityId", namespaces=NS)) if muni is not None else None,
            "ggdename":  muni.findtext("e:municipalityName", namespaces=NS) if muni is not None else None,
            "gdekt":     muni.findtext("e:cantonAbbreviation", namespaces=NS) if muni is not None else None,
            # Adresse
            "strname":   _strname(street),
            "deinr":     entrance.findtext("e:buildingEntranceNo", namespaces=NS) if entrance is not None else None,
            "plz4":      locality.findtext("e:swissZipCode", namespaces=NS) if locality is not None else None,
            "plzname":   locality.findtext("e:placeName", namespaces=NS) if locality is not None else None,
            # Gebäude-Attribute
            "gstat":     _int(b.findtext("e:buildingStatus", namespaces=NS)) if b is not None else None,
            "gkat":      _int(b.findtext("e:buildingCategory", namespaces=NS)) if b is not None else None,
            "gklas":     _int(b.findtext("e:buildingClass", namespaces=NS)) if b is not None else None,
            "gbauj":     _int(b.findtext("e:dateOfConstruction/e:dateOfConstruction", namespaces=NS)) if b is not None else None,
            "gbaup":     _int(b.findtext("e:dateOfConstruction/e:periodOfConstruction", namespaces=NS)) if b is not None else None,
            "garea":     _float(b.findtext("e:surfaceAreaOfBuilding", namespaces=NS)) if b is not None else None,
            "ganzwhg":   len(dwellings),
            # Koordinaten (LV95)
            "gkode":     _float(b.findtext("e:coordinates/e:east", namespaces=NS)) if b is not None else None,
            "gkodn":     _float(b.findtext("e:coordinates/e:north", namespaces=NS)) if b is not None else None,
            # Mutation
            "gdat":      b.findtext("e:recordModification/e:updateDate", namespaces=NS) if b is not None else None,
            "gdatns":    b.findtext("e:recordModification/e:createDate", namespaces=NS) if b is not None else None,
        }
        return record


# ------------------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------------------

def _int(value) -> int | None:
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _float(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _strname(street_el) -> str | None:
    """Extrahiert den deutschen Strassennamen aus dem streetNameList-Element."""
    if street_el is None:
        return None
    # Bevorzuge Sprache 9901 (Deutsch) oder ersten verfügbaren Namen
    for item in street_el.findall(".//e:streetNameItem", NS):
        lang = item.findtext("e:language", namespaces=NS)
        name = item.findtext("e:descriptionLong", namespaces=NS)
        if lang == "9901" and name:
            return name
    # Fallback: erster Name
    name = street_el.findtext(".//e:descriptionLong", namespaces=NS)
    return name
