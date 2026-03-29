# Databricks notebook source
# MAGIC %md
# MAGIC # GWR Daten Explorer
# MAGIC Dieses Notebook dient der explorativen Analyse der historisierten GWR-Daten.
# MAGIC
# MAGIC **Struktur:**
# MAGIC - 01 Setup & Verbindung
# MAGIC - 02 Bronze Layer: Rohdaten
# MAGIC - 03 Silver Layer: Historisierung prüfen
# MAGIC - 04 Gold Layer: DQA-Referenzdaten
# MAGIC - 05 Änderungshistorie analysieren

# COMMAND ----------
# MAGIC %md ## 01 Setup & Verbindung

import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = os.environ.get("DATABRICKS_CATALOG", "main")
SCHEMA  = os.environ.get("DATABRICKS_SCHEMA", "dqa_realestate")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Aktiver Namespace: {CATALOG}.{SCHEMA}")

# COMMAND ----------
# MAGIC %md ## 02 Bronze Layer

bronze = spark.table("bronze_gwr_buildings")

print(f"Anzahl Records (Bronze): {bronze.count():,}")
print(f"Runs:")
bronze.groupBy("_run_id", "_ingestion_date", "_is_full_load") \
      .agg(F.count("*").alias("records")) \
      .orderBy("_ingestion_date", ascending=False) \
      .show(10)

# COMMAND ----------
# Kantonale Verteilung im letzten Run
latest_run = bronze.orderBy("_ingestion_date", ascending=False).first()["_run_id"]

bronze.filter(F.col("_run_id") == latest_run) \
      .groupBy("gdekt") \
      .count() \
      .orderBy("count", ascending=False) \
      .show(30)

# COMMAND ----------
# MAGIC %md ## 03 Silver Layer — Historisierung

silver = spark.table("silver_gwr_buildings")

print(f"Anzahl Records (Silver gesamt):   {silver.count():,}")
print(f"Davon aktuell:                    {silver.filter('is_current=true').count():,}")
print(f"Davon historisch:                 {silver.filter('is_current=false').count():,}")
print(f"Davon als gelöscht markiert:      {silver.filter('dqa_deleted=true').count():,}")

# COMMAND ----------
# Gebäude mit mehreren Versionen (haben sich geändert)
changes = silver.groupBy("egid") \
               .agg(F.count("*").alias("versionen")) \
               .filter("versionen > 1") \
               .orderBy("versionen", ascending=False)

print(f"Gebäude mit Statuswechseln: {changes.count():,}")
changes.show(20)

# COMMAND ----------
# Beispiel: Vollständige Historie eines Gebäudes mit Änderungen
egid_mit_aenderungen = changes.first()["egid"]

print(f"\nHistorie EGID {egid_mit_aenderungen}:")
silver.filter(F.col("egid") == egid_mit_aenderungen) \
      .select("egid", "gstat", "ganzwhg", "valid_from", "valid_to",
              "is_current", "dqa_deleted") \
      .orderBy("valid_from") \
      .show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 04 Gold Layer — DQA Referenzdaten

gold_current = spark.table("gold_gwr_current")
gold_history  = spark.table("gold_gwr_history")
gold_changes  = spark.table("gold_gwr_changes")

print(f"Aktuelle Gebäude (Gold): {gold_current.count():,}")

# Statusverteilung
gold_current.groupBy("gstat", "gstat_label") \
            .count() \
            .orderBy("gstat") \
            .show()

# COMMAND ----------
# Kantonale Übersicht
gold_current.groupBy("gdekt") \
            .agg(
                F.count("*").alias("gebaeude"),
                F.sum("ganzwhg").alias("wohnungen"),
                F.avg("gbauj").alias("durchschnitt_baujahr"),
            ) \
            .orderBy("gebaeude", ascending=False) \
            .show(30)

# COMMAND ----------
# Zeitliche Verteilung der letzten Mutierungen
gold_current.withColumn("mutation_jahr", F.year("last_mutation_date")) \
            .groupBy("mutation_jahr") \
            .count() \
            .orderBy("mutation_jahr", ascending=False) \
            .show(20)

# COMMAND ----------
# MAGIC %md ## 05 Änderungshistorie — Statuswechsel analysieren

# Häufige Statusübergänge (von → nach)
w = Window.partitionBy("egid").orderBy("valid_from")

transitions = silver \
    .withColumn("prev_gstat", F.lag("gstat").over(w)) \
    .filter(F.col("prev_gstat").isNotNull()) \
    .groupBy("prev_gstat", "gstat") \
    .agg(F.count("*").alias("anzahl")) \
    .orderBy("anzahl", ascending=False)

print("Häufigste Statusübergänge:")
transitions.show(20)

# COMMAND ----------
# MAGIC %md ## 06 Datenqualitäts-Schnellcheck auf Gold Layer

# Gebäude ohne Koordinaten
ohne_koord = gold_current.filter(F.col("gkode").isNull() | F.col("gkodn").isNull()).count()

# Gebäude mit Wohnungen aber ohne Baujahr
ohne_baujahr = gold_current.filter(
    F.col("ganzwhg") > 0 & F.col("gbauj").isNull()
).count()

# PLZ fehlt
ohne_plz = gold_current.filter(F.col("plz4").isNull()).count()

print(f"Gebäude ohne Koordinaten:        {ohne_koord:,}")
print(f"Gebäude ohne Baujahr:            {ohne_baujahr:,}")
print(f"Gebäude ohne PLZ:                {ohne_plz:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Nächste Schritte
# MAGIC
# MAGIC 1. `python main.py pipeline --kanton AG` — ersten Kanton laden
# MAGIC 2. Dieses Notebook ausführen — Daten explorieren
# MAGIC 3. `python main.py dqa --file meine_daten.csv` — DQA starten
