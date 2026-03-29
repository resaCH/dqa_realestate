"""
src/ingestion/gwr_incremental.py
---------------------------------
Watermark-Management für den inkrementellen GWR-Abzug.

Das Watermark speichert das grösste bekannte GDAT-Datum.
Bei jedem Run werden nur Gebäude abgerufen mit GDAT > watermark.
Nach erfolgreichem Run wird das Watermark aktualisiert.

Das Watermark wird lokal als JSON gespeichert UND als Databricks-Tabelle
persistiert (Fallback & Audit-Trail).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

from loguru import logger

from src.utils.config import settings


WATERMARK_TABLE = "gwr_watermark"
INITIAL_DATE = "2000-01-01"  # Vollabzug beim ersten Run


class WatermarkManager:
    """
    Verwaltet den Watermark für den inkrementellen GWR-Abzug.

    Speichert das Watermark:
    1. Lokal als JSON (schneller Zugriff, Fallback)
    2. In Databricks (persistiert über Codespace-Restarts hinweg)
    """

    def __init__(self, use_databricks: bool = True) -> None:
        self._local_path = Path(settings.watermark_path)
        self._local_path.parent.mkdir(parents=True, exist_ok=True)
        self._use_databricks = use_databricks

    # ------------------------------------------------------------------
    # Öffentliche API
    # ------------------------------------------------------------------

    def get_last_run_date(self) -> str:
        """
        Gibt das Watermark-Datum zurück (Format: YYYY-MM-DD).
        Bei erstem Run: INITIAL_DATE → Vollabzug.
        """
        watermark = self._load()
        date_str = watermark.get("gwr_buildings", INITIAL_DATE)
        logger.info(f"Watermark geladen: {date_str}")
        return date_str

    def update(self, new_date: str) -> None:
        """
        Aktualisiert das Watermark nach erfolgreichem Run.

        Args:
            new_date: Neues Watermark-Datum (YYYY-MM-DD), typischerweise
                      das grösste GDAT im letzten Abzug.
        """
        watermark = self._load()
        old_date = watermark.get("gwr_buildings", INITIAL_DATE)

        # Nur vorwärts aktualisieren
        if new_date <= old_date:
            logger.debug(f"Watermark unverändert: {old_date}")
            return

        watermark["gwr_buildings"] = new_date
        watermark["last_updated"] = datetime.now().isoformat()
        self._save(watermark)

        if self._use_databricks:
            self._persist_to_databricks(new_date)

        logger.info(f"Watermark aktualisiert: {old_date} → {new_date}")

    def suggest_safe_watermark(self, fetched_records: list[dict]) -> str:
        """
        Berechnet einen sicheren Watermark aus einem Batch von Records.

        Verwendet max(GDAT) - 1 Tag als Puffer,
        um Teilabzüge am letzten Tag zu vermeiden.
        """
        dates = [r.get("gdat") for r in fetched_records if r.get("gdat")]
        if not dates:
            return self.get_last_run_date()

        max_date_str = max(dates)
        try:
            max_date = date.fromisoformat(max_date_str[:10])
            safe_date = max_date - timedelta(days=1)
            return safe_date.isoformat()
        except ValueError:
            logger.warning(f"Ungültiges GDAT-Format: {max_date_str}")
            return self.get_last_run_date()

    # ------------------------------------------------------------------
    # Interne Methoden
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        """Lädt Watermark aus lokaler JSON-Datei."""
        if self._local_path.exists():
            try:
                return json.loads(self._local_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                logger.warning("Watermark-Datei korrupt, verwende Initialwert.")
        return {}

    def _save(self, watermark: dict) -> None:
        """Speichert Watermark in lokale JSON-Datei."""
        self._local_path.write_text(
            json.dumps(watermark, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.debug(f"Watermark lokal gespeichert: {self._local_path}")

    def _persist_to_databricks(self, new_date: str) -> None:
        """
        Schreibt Watermark als Audit-Record in Databricks.
        Tabelle: gwr_watermark (append-only Log).
        """
        try:
            from src.utils.databricks_client import get_client
            import pandas as pd

            client = get_client()
            df = pd.DataFrame([{
                "source":         "gwr_buildings",
                "watermark_date": new_date,
                "recorded_at":    datetime.now().isoformat(),
            }])
            # Tabelle beim ersten Run anlegen
            if not client.table_exists(WATERMARK_TABLE):
                client.execute_sql_no_result(f"""
                    CREATE TABLE IF NOT EXISTS
                        {client.catalog}.{client.schema}.{WATERMARK_TABLE} (
                        source         STRING,
                        watermark_date DATE,
                        recorded_at    TIMESTAMP
                    )
                    USING DELTA
                    COMMENT 'Audit-Log für inkrementelle Abzüge'
                """)
            client.write_dataframe(df, WATERMARK_TABLE, mode="append")
        except Exception as exc:
            # Nicht-kritisch: lokales Watermark ist Fallback
            logger.warning(f"Watermark-Persistenz in Databricks fehlgeschlagen: {exc}")
