"""
src/pipeline/gold.py
---------------------
Gold Layer: DQA-optimierte Views auf Silver-Daten.

Erstellt drei Views:
1. gold_gwr_current     – Nur aktuell gültige Gebäude (is_current=true)
2. gold_gwr_history     – Alle historischen Versionen
3. gold_gwr_changes     – Nur Gebäude die sich je geändert haben (für Trendanalysen)
"""

from __future__ import annotations

from loguru import logger

from src.utils.databricks_client import get_client

SILVER_TABLE = "silver_gwr_buildings"

VIEWS = {
    "gold_gwr_current": """
        -- Aktuell gültige Gebäude (Basis für DQA-Checks)
        SELECT
            egid,
            egrid,
            gdekt,
            ggdenr,
            ggdename,
            strname,
            deinr,
            plz4,
            plzname,
            gstat,
            CASE gstat
                WHEN 1001 THEN 'Geplant'
                WHEN 1002 THEN 'Bewilligt'
                WHEN 1003 THEN 'Im Bau'
                WHEN 1004 THEN 'Bestehend'
                WHEN 1006 THEN 'Nicht nutzbar'
                WHEN 1007 Abgebrochen'
                WHEN 8 THEN 'Nicht realisiert'
                ELSE    'Unbekannt'
            END                         AS gstat_label,
            gkat,
            gklas,
            gbauj,
            gbaup,
            gabbj,
            garea,
            gvol,
            ganzwhg,
            gazzi,
            gkode,
            gkodn,
            gschutzr,
            gdatns,
            gdat                        AS last_mutation_date,
            valid_from,
            record_hash,
            _silver_loaded
        FROM {catalog}.{schema}.{silver}
        WHERE is_current = true
          AND dqa_deleted = false
    """,

    "gold_gwr_history": """
        -- Alle historischen Versionen eines Gebäudes
        SELECT
            egid,
            egrid,
            gdekt,
            ggdenr,
            ggdename,
            strname,
            deinr,
            plz4,
            plzname,
            gstat,
            gkat,
            gklas,
            gbauj,
            gabbj,
            garea,
            gvol,
            ganzwhg,
            valid_from,
            valid_to,
            is_current,
            dqa_deleted,
            record_hash,
            DATEDIFF(
                COALESCE(valid_to, CURRENT_TIMESTAMP),
                valid_from
            )                           AS days_valid
        FROM {catalog}.{schema}.{silver}
        ORDER BY egid, valid_from
    """,

    "gold_gwr_changes": """
        -- Gebäude die mindestens einmal mutiert wurden (> 1 Version in Silver)
        SELECT
            egid,
            COUNT(*)            AS version_count,
            MIN(valid_from)     AS first_seen,
            MAX(valid_from)     AS last_change,
            MAX(gstat)          AS latest_gstat,
            MIN(gstat)          AS initial_gstat,
            COLLECT_LIST(gstat) AS gstat_history
        FROM {catalog}.{schema}.{silver}
        GROUP BY egid
        HAVING COUNT(*) > 1
        ORDER BY version_count DESC
    """,
}


def run_gold_views() -> None:
    """Erstellt oder aktualisiert alle Gold-Views."""
    client = get_client()
    catalog = client.catalog
    schema = client.schema

    client.ensure_schema()

    for view_name, view_sql in VIEWS.items():
        full_sql = view_sql.format(
            catalog=catalog,
            schema=schema,
            silver=SILVER_TABLE,
        )
        client.execute_sql_no_result(f"""
            CREATE OR REPLACE VIEW
                {catalog}.{schema}.{view_name}
            AS
            {full_sql}
        """)
        logger.info(f"Gold-View erstellt/aktualisiert: {view_name}")

    logger.info("=== Alle Gold-Views bereit ===")


def get_summary_stats() -> dict:
    """Gibt eine Übersicht über den aktuellen Datenbestand zurück."""
    client = get_client()
    catalog = client.catalog
    schema = client.schema

    rows = client.execute_sql(f"""
        SELECT
            COUNT(*)                        AS total_buildings,
            COUNT(DISTINCT gdekt)           AS kantone,
            COUNT(DISTINCT ggdenr)          AS gemeinden,
            SUM(CASE WHEN gstat = 1004 THEN 1 ELSE 0 END) AS bestehende,
            SUM(CASE WHEN gstat = 1003 THEN 1 ELSE 0 END) AS im_bau,
            SUM(CASE WHEN gstat = 1001 THEN 1 ELSE 0 END) AS geplant,
            MIN(last_mutation_date)                       AS oldest_mutation,
            MAX(last_mutation_date)                       AS latest_mutation
        FROM {catalog}.{schema}.gold_gwr_current
    """)
    return rows[0] if rows else {}
