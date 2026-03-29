"""
src/pipeline/bronze.py
-----------------------
Bronze Layer: Rohe GWR-API-Responses werden als Delta-Tabelle gespeichert.

Prinzipien:
- Append-only (kein Update, kein Delete)
- Vollständige Rohdaten inkl. Metadaten (_ingested_at, _batch_id)
- Partitionierung nach ingestion_date für performante Abfragen
- Schema-Evolution erlaubt (mergeSchema=true)
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd
from loguru import logger

from src.ingestion.gwr_fetcher import GWRFetcher
from src.ingestion.gwr_incremental import WatermarkManager
from src.utils.databricks_client import get_client

BRONZE_TABLE = "bronze_gwr_buildings"
BRONZE_APARTMENTS_TABLE = "bronze_gwr_apartments"
BATCH_SIZE = 1000  # Datensätze pro Schreibvorgang


def run_buildings_ingestion(
    full_load: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Hauptfunktion für den GWR-Gebäude-Ingestion-Run.

    Args:
        full_load:  True = ignoriert Watermark, lädt alle Gebäude.
        dry_run:    True = kein Schreiben in Databricks (nur Logging).

    Returns:
        dict mit Run-Statistiken.
    """
    run_id = str(uuid.uuid4())[:8]
    started_at = datetime.now(timezone.utc)
    logger.info(f"=== Bronze Run gestartet | run_id={run_id} | full_load={full_load} ===")

    watermark_mgr = WatermarkManager(use_databricks=not dry_run)
    fetcher = GWRFetcher()
    client = get_client() if not dry_run else None

    # ------------------------------------------------------------------
    # Watermark bestimmen
    # ------------------------------------------------------------------
    since_date = None if full_load else watermark_mgr.get_last_run_date()

    # ------------------------------------------------------------------
    # Schema sicherstellen
    # ------------------------------------------------------------------
    if not dry_run:
        client.ensure_schema()
        _ensure_bronze_table(client)

    # ------------------------------------------------------------------
    # Daten abrufen und in Batches schreiben
    # ------------------------------------------------------------------
    all_records: list[dict] = []
    total_written = 0
    batch_count = 0
    now_str = started_at.isoformat()
    ingestion_date = started_at.date().isoformat()

    for record in fetcher.fetch_buildings(egid_from=1, egid_to=500):
        # Metadaten hinzufügen
        record["_ingested_at"]    = now_str
        record["_ingestion_date"] = ingestion_date
        record["_run_id"]         = run_id
        record["_batch_id"]       = f"{run_id}_{batch_count:04d}"
        record["_source"]         = "gwr_api"
        record["_is_full_load"]   = full_load

        all_records.append(record)

        if len(all_records) >= BATCH_SIZE:
            if not dry_run:
                _write_batch(client, all_records)
            total_written += len(all_records)
            batch_count += 1
            logger.info(f"Batch {batch_count} geschrieben: {total_written} Records total")
            all_records = []

    # Letzter Batch
    if all_records:
        if not dry_run:
            _write_batch(client, all_records)
        total_written += len(all_records)
        batch_count += 1

    # ------------------------------------------------------------------
    # Watermark aktualisieren
    # ------------------------------------------------------------------
    if total_written > 0 and not dry_run:
        # Watermark aus dem letzten Batch ableiten
        last_batch_records = all_records  # letzter Batch noch in Memory
        # Sicherer: nochmal alle abgerufenen Records würden benötigt;
        # in Produktion besser: max(gdat) per SQL aus der frisch geschriebenen Bronze
        new_watermark = _get_max_gdat_from_bronze(client, run_id)
        if new_watermark:
            watermark_mgr.update(new_watermark)

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    stats = {
        "run_id":        run_id,
        "since_date":    since_date,
        "total_written": total_written,
        "batch_count":   batch_count,
        "elapsed_s":     round(elapsed, 1),
        "dry_run":       dry_run,
    }
    logger.info(f"=== Bronze Run abgeschlossen | {stats} ===")
    return stats


# ------------------------------------------------------------------
# Interne Hilfsfunktionen
# ------------------------------------------------------------------

def _ensure_bronze_table(client) -> None:
    """Erstellt die Bronze-Tabelle falls nicht vorhanden."""
    client.execute_sql_no_result(f"""
        CREATE TABLE IF NOT EXISTS
            {client.catalog}.{client.schema}.{BRONZE_TABLE} (
            -- Identifikatoren
            egid            BIGINT,
            egrid           STRING,
            -- Ort
            gdekt           STRING,
            ggdenr          INT,
            ggdename        STRING,
            strname         STRING,
            deinr           STRING,
            plz4            STRING,
            plzname         STRING,
            -- Status & Kategorie
            gstat           INT,
            gkat            INT,
            gklas           INT,
            -- Baujahr / Abbruch
            gbauj           INT,
            gbaum           INT,
            gbaup           INT,
            gabbj           INT,
            -- Masse
            garea           DOUBLE,
            gvol            DOUBLE,
            ganzwhg         INT,
            gazzi           INT,
            -- Koordinaten
            gkode           DOUBLE,
            gkodn           DOUBLE,
            -- Schutz
            gschutzr        INT,
            -- Datum
            gdatns          STRING,
            gdat            STRING,
            -- Ingestion-Metadaten
            _ingested_at    STRING,
            _ingestion_date STRING,
            _run_id         STRING,
            _batch_id       STRING,
            _source         STRING,
            _is_full_load   BOOLEAN
        )
        USING DELTA
        PARTITIONED BY (_ingestion_date)
        COMMENT 'Bronze: Rohe GWR-API-Responses, append-only'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)


def _write_batch(client, records: list[dict]) -> None:
    """Schreibt einen Batch von Records in die Bronze-Tabelle."""
    df = pd.DataFrame(records)
    # Typ-Sicherung
    for col in ["egid", "ggdenr", "gstat", "gkat", "gklas",
                "gbauj", "gbaum", "gbaup", "gabbj", "ganzwhg", "gazzi", "gschutzr"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["garea", "gvol", "gkode", "gkodn"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    client.write_dataframe(df, BRONZE_TABLE, mode="append")


def _get_max_gdat_from_bronze(client, run_id: str) -> str | None:
    """Ermittelt das grösste GDAT aus dem aktuellen Run."""
    try:
        rows = client.execute_sql(f"""
            SELECT MAX(gdat) AS max_gdat
            FROM {client.catalog}.{client.schema}.{BRONZE_TABLE}
            WHERE _run_id = '{run_id}'
              AND gdat IS NOT NULL
        """)
        return rows[0]["max_gdat"] if rows and rows[0]["max_gdat"] else None
    except Exception as exc:
        logger.warning(f"max(GDAT) Abfrage fehlgeschlagen: {exc}")
        return None
