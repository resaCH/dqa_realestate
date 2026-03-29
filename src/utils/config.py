"""
src/utils/config.py
-------------------
Zentrale Konfiguration via pydantic-settings.
Werte kommen aus Umgebungsvariablen oder der .env-Datei.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Databricks
    databricks_host: str = Field(..., description="Workspace URL")
    databricks_token: str = Field(..., description="Personal Access Token")
    databricks_catalog: str = Field(default="main")
    databricks_schema: str = Field(default="dqa_realestate")

    # GWR API
    gwr_base_url: str = Field(
        default="https://www.housing-stat.ch/regbl/api/ech0206/v2"
    )
    gwr_kanton: str = Field(default="", description="Leer = alle Kantone")

    # Watermark
    watermark_path: str = Field(default="./data/watermarks/gwr_watermark.json")

    # Logging
    log_level: str = Field(default="INFO")


# Singleton
settings = Settings()
