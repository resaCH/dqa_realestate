# DQA Real Estate

Datenqualitäts-Assessment für Immobiliendaten — mit historisierten GWR-Referenzdaten auf Databricks.

---

## Überblick

```
GWR API (BFS)
     │
     ▼
Bronze Layer       ← Rohe API-Responses, append-only
     │
     ▼
Silver Layer       ← Historisiert via SCD Type 2 (jede Änderung nachvollziehbar)
     │
     ▼
Gold Layer         ← Aktuelle & historische Views für DQA-Checks
     │
     ▼
DQA Application    ← Immobiliendaten gegen GWR validieren
```

---

## Setup

### 1. Codespace starten

Das Projekt enthält eine `.devcontainer`-Konfiguration — GitHub Codespace installiert alles automatisch.

### 2. Secrets konfigurieren

In GitHub unter **Settings → Codespaces → Secrets** folgende Variablen anlegen:

| Secret | Beschreibung |
|---|---|
| `DATABRICKS_HOST` | Workspace-URL, z.B. `https://adb-xxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal Access Token (Databricks → User Settings → Developer) |
| `DATABRICKS_CATALOG` | Catalog-Name, z.B. `main` |
| `DATABRICKS_SCHEMA` | Schema-Name, z.B. `dqa_realestate` |

Alternativ lokal: `.env.example` → `.env` kopieren und befüllen.

### 3. Verbindung prüfen

```bash
make connection
```

---

## GWR-Daten laden

### Erster Vollabzug (einmalig)

```bash
make bronze-full
```

Oder auf einen Kanton beschränken (empfohlen zum Testen):

```bash
make bronze-kanton KANTON=AG
```

### Täglicher inkrementeller Abzug

```bash
make pipeline
```

Dies führt Bronze → Silver → Gold in einem Schritt aus.

---

## DQA-Checks durchführen

Die Immobiliendaten müssen als CSV vorliegen. Pflichtfeld: `egid`.

Optionale Felder für detailliertere Checks:

| Feld | Beschreibung |
|---|---|
| `plz` | Postleitzahl |
| `strasse` | Strassenname |
| `baujahr` | Baujahr des Gebäudes |
| `anzahl_wohnungen` | Wohnungsanzahl |
| `nutzungsart` | z.B. `vermietet`, `leer`, `verkauft` |

```bash
make dqa FILE=meine_immobilien.csv
```

Ergebnis wird als JSON gespeichert unter `results/dqa_YYYYMMDD_HHMMSS.json`.

---

## Datenbestand-Übersicht

```bash
make status
```

---

## Tabellen-Übersicht

| Tabelle/View | Layer | Beschreibung |
|---|---|---|
| `bronze_gwr_buildings` | Bronze | Rohe API-Responses, append-only, partitioniert nach Datum |
| `silver_gwr_buildings` | Silver | Historisiert (SCD2): `valid_from`, `valid_to`, `is_current` |
| `gold_gwr_current` | Gold | Nur aktuell gültige Gebäude |
| `gold_gwr_history` | Gold | Alle historischen Versionen |
| `gold_gwr_changes` | Gold | Nur Gebäude mit Änderungshistorie |
| `gwr_watermark` | Meta | Audit-Log der inkrementellen Abzüge |

---

## Historisierung (SCD Type 2)

Jede Änderung eines Gebäudes (erkannt via SHA2-Hash aller fachlichen Felder) erzeugt einen neuen Record in der Silver-Tabelle:

```
EGID   │ GSTAT        │ valid_from  │ valid_to    │ is_current
───────┼──────────────┼─────────────┼─────────────┼───────────
12345  │ 1 (geplant)  │ 2024-01-15  │ 2024-09-22  │ false
12345  │ 3 (im Bau)   │ 2024-09-22  │ 2025-03-10  │ false
12345  │ 4 (bewohnt)  │ 2025-03-10  │ 9999-12-31  │ true  ← aktuell
```

Verschwindende EGIDs werden als `dqa_deleted = true` markiert (nicht gelöscht).

---

## DQA-Checks (implementiert)

| Check-ID | Name | Schweregrad |
|---|---|---|
| GWR-01 | EGID im GWR vorhanden | critical |
| GWR-02 | Adresse korrekt (PLZ + Strasse) | warning |
| GWR-03 | Baujahr plausibel und GWR-konform | warning |
| GWR-04 | Gebäudestatus konsistent mit Nutzung | critical |
| GWR-05 | Anzahl Wohnungen konsistent (±10%) | warning |

Neue Checks können in `src/dqa/checks.py` hinzugefügt werden.

---

## Tests

```bash
make test
```

---

## Projektstruktur

```
dqa-realestate/
├── .devcontainer/          GitHub Codespace Konfiguration
├── src/
│   ├── ingestion/
│   │   ├── gwr_fetcher.py          GWR API Client
│   │   └── gwr_incremental.py      Watermark-Management
│   ├── pipeline/
│   │   ├── bronze.py               Raw-Ingestion
│   │   ├── silver_scd2.py          Historisierung (SCD2)
│   │   └── gold.py                 DQA-optimierte Views
│   ├── dqa/
│   │   └── checks.py               DQA-Check-Engine
│   └── utils/
│       ├── config.py               Konfiguration
│       ├── databricks_client.py    Databricks SDK Wrapper
│       └── logger.py               Logging
├── notebooks/
│   └── 01_gwr_explore.py           Explorations-Notebook
├── tests/                          Unit-Tests
├── main.py                         CLI-Einstiegspunkt
├── Makefile                        Häufige Kommandos
├── requirements.txt
└── .env.example                    Secrets-Template
```

---

## GWR API Referenz

- Dokumentation: https://www.housing-stat.ch/de/madd/public.html
- EGID: Eidgenössischer Gebäudeidentifikator
- EWID: Eidgenössischer Wohnungsidentifikator
- GSTAT-Codes: 1=geplant, 2=bewilligt, 3=im Bau, 4=bestehend, 6=nicht nutzbar, 7=abgebrochen
