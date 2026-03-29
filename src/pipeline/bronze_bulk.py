"""
src/pipeline/bronze_bulk.py
----------------------------
Schnelle Bulk-Ingestion via DBFS Upload + COPY INTO.
Statt 50k einzelne INSERTs: 1 Upload + 1 SQL-Statement.
"""
from __future__ import annotations
import io, zipfile, csv, tempfile, os, uuid, requests
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger
from src.utils.databricks_client import get_client
from src.pipeline.bronze import _ensure_bronze_table, BRONZE_TABLE

BASE_URL = "https://public.madd.bfs.admin.ch"


def run_bulk_ingestion(kanton: str) -> dict:
    kanton = kanton.lower()
    client = get_client()
    run_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc)
    started = now

    logger.info(f"=== Bulk Ingestion gestartet | Kanton={kanton.upper()} | run_id={run_id} ===")

    # 1. ZIP herunterladen
    url = f"{BASE_URL}/{kanton}.zip"
    logger.info(f"Download: {url}")
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    zip_bytes = resp.content
    logger.info(f"Download OK: {len(zip_bytes)/1024/1024:.1f} MB")

    # 2. CSV aus ZIP extrahieren und mit Metadaten anreichern
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist()
                         if "gebaeude" in n.lower() and n.endswith(".csv")), None)
        if not csv_name:
            raise RuntimeError(f"Keine Gebäude-CSV gefunden: {zf.namelist()}")

        logger.info(f"Extrahiere: {csv_name}")
        with zf.open(csv_name) as f:
            raw_csv = io.TextIOWrapper(f, encoding="utf-8-sig")
            reader = csv.DictReader(raw_csv, delimiter="\t")

            # Angereicherte CSV in Temp-Datei schreiben
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".csv", delete=False,
                encoding="utf-8", newline=""
            )
            out_fields = list(reader.fieldnames) + [
                "_ingested_at","_ingestion_date","_run_id","_batch_id","_source","_is_full_load"
            ]
            writer = csv.DictWriter(tmp, fieldnames=out_fields, delimiter="\t")
            writer.writeheader()

            count = 0
            for row in reader:
                row["_ingested_at"]    = now.isoformat()
                row["_ingestion_date"] = now.date().isoformat()
                row["_run_id"]         = run_id
                row["_batch_id"]       = f"{run_id}_bulk"
                row["_source"]         = "gwr_zip"
                row["_is_full_load"]   = "true"
                writer.writerow(row)
                count += 1
            tmp.close()

    logger.info(f"CSV vorbereitet: {count:,} Zeilen → {tmp.name}")

    # 3. Volume erstellen und CSV hochladen
    client.ensure_schema()
    client.execute_sql_no_result(f"""
        CREATE VOLUME IF NOT EXISTS
        {client.catalog}.{client.schema}.gwr_uploads
    """)
    vol_path = f"/Volumes/{client.catalog}/{client.schema}/gwr_uploads/gwr_bulk_{kanton}_{run_id}.csv"
    logger.info(f"Upload nach Volume: {vol_path}")
    with open(tmp.name, "rb") as f:
        client._ws.files.upload(vol_path, f, overwrite=True)
    os.unlink(tmp.name)
    logger.info("Upload abgeschlossen")

    # 4. Tabelle sicherstellen
    _ensure_bronze_table(client)

    # 5. COPY INTO (bulk load)
    full_table = f"{client.catalog}.{client.schema}.{BRONZE_TABLE}"
    logger.info(f"COPY INTO {full_table} ...")
    client.execute_sql_no_result(f"""
        COPY INTO {full_table}
        FROM (
            SELECT
                CAST(EGID AS BIGINT)           AS egid,
                EGRID                          AS egrid,
                GDEKT                          AS gdekt,
                CAST(GGDENR AS INT)            AS ggdenr,
                GGDENAME                       AS ggdename,
                CAST(GSTAT AS INT)             AS gstat,
                CAST(GKAT AS INT)              AS gkat,
                CAST(GKLAS AS INT)             AS gklas,
                CAST(GBAUJ AS INT)             AS gbauj,
                CAST(GBAUM AS INT)             AS gbaum,
                CAST(GBAUP AS INT)             AS gbaup,
                CAST(GABBJ AS INT)             AS gabbj,
                CAST(GAREA AS DOUBLE)          AS garea,
                CAST(GVOL AS DOUBLE)           AS gvol,
                CAST(GANZWHG AS INT)           AS ganzwhg,
                CAST(GAZZI AS INT)             AS gazzi,
                CAST(GKODE AS DOUBLE)          AS gkode,
                CAST(GKODN AS DOUBLE)          AS gkodn,
                CAST(GSCHUTZR AS INT)          AS gschutzr,
                GEXPDAT                        AS gdat,
                _ingested_at,
                _ingestion_date,
                _run_id,
                _batch_id,
                _source,
                CAST(_is_full_load AS BOOLEAN) AS _is_full_load
            FROM '{vol_path}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS (
            'header' = 'true',
            'delimiter' = '\t',
            'encoding' = 'UTF-8',
            'nullValue' = ''
        )
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)

    # 6. Volume-Datei löschen
    client._ws.files.delete(vol_path)

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info(f"=== Bulk Ingestion abgeschlossen | {count:,} Zeilen | {elapsed:.0f}s ===")
    return {"run_id": run_id, "kanton": kanton.upper(),
            "records": count, "elapsed_s": round(elapsed, 1)}


def _upload_to_dbfs(client, local_path: str, dbfs_path: str) -> None:
    """Lädt eine Datei chunk-weise auf DBFS hoch."""
    import base64
    file_size = Path(local_path).stat().st_size
    logger.info(f"Upload nach DBFS: {dbfs_path} ({file_size/1024/1024:.1f} MB)")

    handle = client._ws.dbfs.open(dbfs_path, write=True, overwrite=True)
    chunk_size = 1024 * 1024  # 1 MB chunks
    uploaded = 0
    with open(local_path, "rb") as f:
        while chunk := f.read(chunk_size):
            client._ws.dbfs.add_block(handle, base64.b64encode(chunk).decode())
            uploaded += len(chunk)
            if uploaded % (10 * 1024 * 1024) < chunk_size:
                logger.debug(f"  Upload: {uploaded/1024/1024:.0f}/{file_size/1024/1024:.0f} MB")
    client._ws.dbfs.close(handle)
    logger.info("Upload abgeschlossen")
    