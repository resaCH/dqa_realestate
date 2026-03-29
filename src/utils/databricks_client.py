"""
src/utils/databricks_client.py
"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from loguru import logger

from src.utils.config import settings


class DatabricksClient:

    def __init__(self) -> None:
        self._ws = WorkspaceClient(
            host=settings.databricks_host,
            token=settings.databricks_token,
        )
        self.catalog = settings.databricks_catalog
        self.schema = settings.databricks_schema
        self._warehouse_id: str | None = None
        logger.info(
            f"Databricks-Client initialisiert: {settings.databricks_host} | "
            f"{self.catalog}.{self.schema}"
        )

    def get_warehouse_id(self) -> str:
        if self._warehouse_id:
            return self._warehouse_id
        warehouses = list(self._ws.warehouses.list())
        if not warehouses:
            raise RuntimeError("Kein SQL Warehouse im Workspace gefunden.")
        self._warehouse_id = warehouses[0].id
        logger.debug(f"SQL Warehouse: {self._warehouse_id}")
        return self._warehouse_id

    def execute_sql(self, sql: str, wait_timeout: str = "50s") -> list[dict[str, Any]]:
        logger.debug(f"SQL:\n{sql}")
        statement = self._ws.statement_execution.execute_statement(
            warehouse_id=self.get_warehouse_id(),
            statement=sql,
            wait_timeout=wait_timeout,
        )
        if statement.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(
                f"SQL fehlgeschlagen [{statement.status.state}]: "
                f"{statement.status.error}"
            )
        if not statement.result or not statement.result.data_array:
            return []
        columns = [col.name for col in statement.manifest.schema.columns]
        return [dict(zip(columns, row)) for row in statement.result.data_array]

    def execute_sql_no_result(self, sql: str) -> None:
        self.execute_sql(sql)

    def ensure_schema(self) -> None:
        self.execute_sql_no_result(
            f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}"
        )
        logger.info(f"Schema sichergestellt: {self.catalog}.{self.schema}")

    def table_exists(self, table_name: str) -> bool:
        rows = self.execute_sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {self.catalog}.information_schema.tables
            WHERE table_schema = '{self.schema}'
              AND table_name   = '{table_name}'
            """
        )
        return rows[0]["cnt"] > 0 if rows else False

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> None:
        full_name = f"{self.catalog}.{self.schema}.{table_name}"

        if mode == "overwrite":
            self.execute_sql_no_result(f"DELETE FROM {full_name}")

        if df.empty:
            logger.debug(f"Leerer DataFrame — nichts zu schreiben: {full_name}")
            return

        cols = ", ".join(df.columns)
        batch_size = 50
        total_batches = math.ceil(len(df) / batch_size)

        for i in range(total_batches):
            batch = df.iloc[i * batch_size:(i + 1) * batch_size]
            rows = []
            for _, row in batch.iterrows():
                vals = []
                for v in row:
                    import pandas as pd
                    if v is None or v is pd.NA or (isinstance(v, float) and math.isnan(v)):
                        vals.append("NULL")
                    elif isinstance(v, bool):
                        vals.append("true" if v else "false")
                    elif hasattr(v, 'item'):
                        v2 = v.item()
                        vals.append("NULL" if v2 is None else str(v2))
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    else:
                        escaped = str(v).replace("'", "''")
                        vals.append(f"'{escaped}'")
                rows.append(f"({', '.join(vals)})")

            values_sql = ",\n".join(rows)
            self.execute_sql_no_result(
                f"INSERT INTO {full_name} ({cols}) VALUES {values_sql}"
            )

        logger.info(f"Geschrieben ({mode}): {full_name} — {len(df)} Zeilen")


_client: DatabricksClient | None = None


def get_client() -> DatabricksClient:
    global _client
    if _client is None:
        _client = DatabricksClient()
    return _client
