"""
src/pipeline/silver_scd2.py
----------------------------
Silver Layer: Historisierung via SCD Type 2 (Slowly Changing Dimension).

Bei jeder Änderung eines Gebäudes (erkannt via record_hash) wird:
1. Der alte Record geschlossen: valid_to = jetzt, is_current = false
2. Ein neuer Record angelegt: valid_from = jetzt, valid_to = '9999-12-31',
   is_current = true

Verschwundene EGIDs (aus API nicht mehr zurückgegeben) werden als
dqa_deleted = true markiert.

Schema der Silver-Tabelle:
    Alle Felder aus Bronze
    + valid_from      TIMESTAMP   Beginn der Gültigkeit
    + valid_to        TIMESTAMP   Ende der Gültigkeit (9999-12-31 = offen)
    + is_current      BOOLEAN     True = aktuellster Record
    + record_hash     STRING      SHA2 aller fachlichen Felder
    + dqa_deleted     BOOLEAN     True = EGID aus API verschwunden
    + _silver_loaded  TIMESTAMP   Zeitpunkt der Silver-Verarbeitung
"""

from __future__ import annotations

from datetime import datetime, timezone

from loguru import logger

from src.utils.databricks_client import get_client

BRONZE_TABLE = "bronze_gwr_buildings"
SILVER_TABLE = "silver_gwr_buildings"

# Fachliche Felder für den Hash (Änderung → neue Version)
HASH_FIELDS = [
    "gstat", "gkat", "gklas", "gbauj", "gbaum", "gbaup", "gabbj",
    "garea", "gvol", "ganzwhg", "gazzi", "gschutzr",
    "gdekt", "ggdenr", "ggdename", "strname", "deinr", "plz4", "plzname",
    "gkode", "gkodn",
]


def run_silver_processing(run_id: str | None = None) -> dict:
    """
    Verarbeitet neue Bronze-Records in die Silver-Schicht (SCD Type 2).

    Args:
        run_id: Nur Records aus diesem Bronze-Run verarbeiten.
                None = alle unverarbeiteten Bronze-Records.

    Returns:
        dict mit Verarbeitungsstatistiken.
    """
    client = get_client()
    started_at = datetime.now(timezone.utc)
    logger.info("=== Silver SCD2 Verarbeitung gestartet ===")

    # Tabelle sicherstellen
    _ensure_silver_table(client)

    # Staging-Tabelle aus Bronze aufbauen
    staging_table = f"_staging_gwr_{started_at.strftime('%Y%m%d_%H%M%S')}"
    _build_staging(client, staging_table, run_id)

    # SCD2 MERGE ausführen
    stats = _execute_scd2_merge(client, staging_table, started_at)

    # Staging bereinigen
    client.execute_sql_no_result(
        f"DROP TABLE IF EXISTS {client.catalog}.{client.schema}.{staging_table}"
    )

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    stats["elapsed_s"] = round(elapsed, 1)
    logger.info(f"=== Silver SCD2 abgeschlossen | {stats} ===")
    return stats


# ------------------------------------------------------------------
# Silver-Tabelle
# ------------------------------------------------------------------

def _ensure_silver_table(client) -> None:
    client.execute_sql_no_result(f"""
        CREATE TABLE IF NOT EXISTS
            {client.catalog}.{client.schema}.{SILVER_TABLE} (
            egid            BIGINT      NOT NULL,
            egrid           STRING,
            gdekt           STRING,
            ggdenr          INT,
            ggdename        STRING,
            strname         STRING,
            deinr           STRING,
            plz4            STRING,
            plzname         STRING,
            gstat           INT,
            gkat            INT,
            gklas           INT,
            gbauj           INT,
            gbaum           INT,
            gbaup           INT,
            gabbj           INT,
            garea           DOUBLE,
            gvol            DOUBLE,
            ganzwhg         INT,
            gazzi           INT,
            gkode           DOUBLE,
            gkodn           DOUBLE,
            gschutzr        INT,
            gdatns          STRING,
            gdat            STRING,
            -- SCD2 Felder
            valid_from      TIMESTAMP   NOT NULL,
            valid_to        TIMESTAMP   NOT NULL,
            is_current      BOOLEAN     NOT NULL,
            record_hash     STRING      NOT NULL,
            dqa_deleted     BOOLEAN     NOT NULL,
            _silver_loaded  TIMESTAMP   NOT NULL
        )
        USING DELTA
        COMMENT 'Silver: Historisierte GWR-Gebäudedaten (SCD Type 2)'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'delta.enableChangeDataFeed'       = 'true'
        )
    """)
    logger.debug(f"Silver-Tabelle bereit: {SILVER_TABLE}")


# ------------------------------------------------------------------
# Staging
# ------------------------------------------------------------------

def _build_staging(client, staging_table: str, run_id: str | None) -> None:
    """
    Erstellt eine Staging-Tabelle mit den neuesten Bronze-Records pro EGID,
    inklusive record_hash für Änderungserkennung.
    """
    run_filter = f"AND _run_id = '{run_id}'" if run_id else ""

    # Hash-Ausdruck: SHA2 über alle fachlichen Felder
    hash_expr = " || '|' || ".join(
        [f"COALESCE(CAST({f} AS STRING), '')" for f in HASH_FIELDS]
    )

    client.execute_sql_no_result(f"""
        CREATE OR REPLACE TABLE
            {client.catalog}.{client.schema}.{staging_table}
        USING DELTA
        AS
        WITH ranked AS (
            SELECT *,
                SHA2({hash_expr}, 256) AS record_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY egid
                    ORDER BY _ingested_at DESC
                ) AS rn
            FROM {client.catalog}.{client.schema}.{BRONZE_TABLE}
            WHERE egid IS NOT NULL
              {run_filter}
        )
        SELECT * EXCEPT (rn)
        FROM ranked
        WHERE rn = 1
    """)
    logger.debug(f"Staging erstellt: {staging_table}")


# ------------------------------------------------------------------
# SCD2 MERGE
# ------------------------------------------------------------------

def _execute_scd2_merge(client, staging_table: str, now: datetime) -> dict:
    """
    SCD Type 2 MERGE INTO:

    CASE 1 – Neues Gebäude (kein Match):
        → INSERT mit valid_from=now, valid_to='9999-12-31', is_current=true

    CASE 2 – Bestehendes Gebäude, Hash geändert:
        → UPDATE: altes Record schliessen (valid_to=now, is_current=false)
        → INSERT: neues Record (valid_from=now, valid_to='9999-12-31')

    CASE 3 – Bestehendes Gebäude, unverändert:
        → Kein Action (MERGE ignoriert)
    """
    now_str = now.isoformat()
    far_future = "9999-12-31T23:59:59"
    catalog = client.catalog
    schema = client.schema

    # -- Phase 1: Bestehende aktuelle Records mit geändertem Hash schliessen --
    rows_closed = client.execute_sql(f"""
        UPDATE {catalog}.{schema}.{SILVER_TABLE} AS silver
        SET
            valid_to       = TIMESTAMP '{now_str}',
            is_current     = false,
            _silver_loaded = TIMESTAMP '{now_str}'
        WHERE silver.is_current = true
          AND silver.dqa_deleted = false
          AND EXISTS (
              SELECT 1
              FROM {catalog}.{schema}.{staging_table} AS stg
              WHERE stg.egid = silver.egid
                AND stg.record_hash != silver.record_hash
          )
    """)

    # -- Phase 2: Neue Records für neue/geänderte Gebäude einfügen --
    rows_inserted = client.execute_sql(f"""
        INSERT INTO {catalog}.{schema}.{SILVER_TABLE}
        SELECT
            stg.egid,
            stg.egrid,
            stg.gdekt,
            stg.ggdenr,
            stg.ggdename,
            stg.strname,
            stg.deinr,
            stg.plz4,
            stg.plzname,
            stg.gstat,
            stg.gkat,
            stg.gklas,
            stg.gbauj,
            stg.gbaum,
            stg.gbaup,
            stg.gabbj,
            stg.garea,
            stg.gvol,
            stg.ganzwhg,
            stg.gazzi,
            stg.gkode,
            stg.gkodn,
            stg.gschutzr,
            stg.gdatns,
            stg.gdat,
            TIMESTAMP '{now_str}'        AS valid_from,
            TIMESTAMP '{far_future}'     AS valid_to,
            true                         AS is_current,
            stg.record_hash,
            false                        AS dqa_deleted,
            TIMESTAMP '{now_str}'        AS _silver_loaded
        FROM {catalog}.{schema}.{staging_table} AS stg
        WHERE
            -- Neues Gebäude: noch nicht in Silver
            NOT EXISTS (
                SELECT 1 FROM {catalog}.{schema}.{SILVER_TABLE} s
                WHERE s.egid = stg.egid AND s.is_current = true
            )
            -- ODER: bestehendes Gebäude mit geändertem Hash (wurde in Phase 1 geschlossen)
            OR EXISTS (
                SELECT 1 FROM {catalog}.{schema}.{SILVER_TABLE} s
                WHERE s.egid = stg.egid
                  AND s.is_current = false
                  AND s.valid_to = TIMESTAMP '{now_str}'
            )
    """)

    # -- Phase 3: Gelöschte EGIDs markieren --
    # Gebäude die im letzten Vollabzug-Staging nicht mehr vorkommen
    rows_deleted = client.execute_sql(f"""
        UPDATE {catalog}.{schema}.{SILVER_TABLE} AS silver
        SET
            dqa_deleted    = true,
            valid_to       = TIMESTAMP '{now_str}',
            is_current     = false,
            _silver_loaded = TIMESTAMP '{now_str}'
        WHERE silver.is_current = true
          AND silver.dqa_deleted = false
          AND NOT EXISTS (
              SELECT 1
              FROM {catalog}.{schema}.{staging_table} AS stg
              WHERE stg.egid = silver.egid
          )
    """)

    # Zählungen aus SQL-Ergebnis lesen (Databricks gibt rows affected zurück)
    def _count(result) -> int:
        if result and isinstance(result, list) and result[0]:
            first = list(result[0].values())
            return int(first[0]) if first else 0
        return 0

    return {
        "records_closed":   _count(rows_closed),
        "records_inserted": _count(rows_inserted),
        "records_deleted":  _count(rows_deleted),
    }
