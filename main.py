"""
main.py
--------
CLI-Einstiegspunkt für alle Pipeline-Schritte.

Verwendung:
    python main.py bronze              # GWR-Daten inkrementell abrufen
    python main.py bronze --full-load  # Vollabzug (ignoriert Watermark)
    python main.py silver              # SCD2-Verarbeitung
    python main.py gold                # Gold-Views aktualisieren
    python main.py pipeline            # bronze → silver → gold in einem Schritt
    python main.py dqa --file data.csv # DQA-Checks auf CSV-Datei
    python main.py status              # Datenbestand-Übersicht
    python main.py test-connection     # Verbindungen prüfen
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from src.utils.logger import setup_logger

console = Console()
setup_logger()


@click.group()
def cli():
    """DQA Real Estate — Datenqualitäts-Assessment für Immobiliendaten."""
    pass


# ------------------------------------------------------------------
# Pipeline-Kommandos
# ------------------------------------------------------------------

@cli.command()
@click.option("--full-load", is_flag=True, default=False,
              help="Vollabzug — ignoriert Watermark.")
@click.option("--dry-run", is_flag=True, default=False,
              help="Kein Schreiben in Databricks.")
@click.option("--kanton", default="",
              help="Nur Gebäude dieses Kantons (z.B. AG, ZH).")
def bronze(full_load: bool, dry_run: bool, kanton: str):
    """GWR-Gebäudedaten abrufen und in Bronze Layer schreiben."""
    from src.pipeline.bronze import run_buildings_ingestion
    from src.utils.config import settings

    if kanton:
        settings.gwr_kanton = kanton

    console.print(Panel(
        f"[bold]Bronze Ingestion[/bold]\n"
        f"Modus: {'Vollabzug' if full_load else 'Inkrementell'} | "
        f"Kanton: {kanton or 'alle'} | "
        f"Dry-Run: {dry_run}",
        style="yellow"
    ))

    stats = run_buildings_ingestion(full_load=full_load, dry_run=dry_run)
    _print_stats(stats, "Bronze Run")


@cli.command()
@click.option("--run-id", default=None,
              help="Nur Records aus diesem Bronze-Run verarbeiten.")
def silver(run_id: str | None):
    """SCD Type 2 Verarbeitung: Bronze → Silver."""
    from src.pipeline.silver_scd2 import run_silver_processing

    console.print(Panel("[bold]Silver SCD2 Verarbeitung[/bold]", style="blue"))
    stats = run_silver_processing(run_id=run_id)
    _print_stats(stats, "Silver Run")


@cli.command()
def gold():
    """Gold-Views aktualisieren."""
    from src.pipeline.gold import run_gold_views, get_summary_stats

    console.print(Panel("[bold]Gold Views[/bold]", style="green"))
    run_gold_views()

    stats = get_summary_stats()
    if stats:
        table = Table(title="Datenbestand Gold Layer", show_header=True)
        table.add_column("Kennzahl", style="cyan")
        table.add_column("Wert", style="white")
        for k, v in stats.items():
            table.add_row(str(k), str(v))
        console.print(table)


@cli.command()
@click.option("--full-load", is_flag=True, default=False)
@click.option("--kanton", default="")
def pipeline(full_load: bool, kanton: str):
    """Vollständige Pipeline: Bronze → Silver → Gold."""
    from src.pipeline.bronze import run_buildings_ingestion
    from src.pipeline.silver_scd2 import run_silver_processing
    from src.pipeline.gold import run_gold_views

    from src.utils.config import settings
    if kanton:
        settings.gwr_kanton = kanton

    console.print(Panel(
        "[bold]Pipeline: Bronze → Silver → Gold[/bold]",
        style="magenta"
    ))

    with console.status("Bronze..."):
        bronze_stats = run_buildings_ingestion(full_load=full_load)
    _print_stats(bronze_stats, "Bronze")

    with console.status("Silver..."):
        silver_stats = run_silver_processing(run_id=bronze_stats.get("run_id"))
    _print_stats(silver_stats, "Silver")

    with console.status("Gold..."):
        run_gold_views()

    console.print("[bold green]Pipeline abgeschlossen.[/bold green]")


# ------------------------------------------------------------------
# DQA-Kommando
# ------------------------------------------------------------------

@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--source", default="csv", help="Name des Quellsystems.")
@click.option("--output", default=None,
              help="Ergebnis-JSON speichern unter diesem Pfad.")
def dqa(file: str, source: str, output: str | None):
    """
    DQA-Checks auf einer Immobilien-Datei durchführen.

    FILE: Pfad zur CSV-Datei mit Immobiliendaten.
    Pflichtfelder: egid. Optionale Felder: plz, strasse, baujahr,
    anzahl_wohnungen, nutzungsart.
    """
    import pandas as pd
    from src.dqa.checks import DQARunner

    console.print(Panel(
        f"[bold]DQA Checks[/bold]\nQuelle: {file} | System: {source}",
        style="cyan"
    ))

    df = pd.read_csv(file)
    records = df.to_dict(orient="records")
    console.print(f"  {len(records)} Records geladen.")

    runner = DQARunner(source_system=source)
    with console.status("Checks werden durchgeführt..."):
        report = runner.run(records)

    # Zusammenfassung
    table = Table(title="DQA Ergebnis", show_header=True)
    table.add_column("Check")
    table.add_column("PASS", style="green")
    table.add_column("FAIL", style="red")
    table.add_column("Schweregrad")

    from collections import defaultdict
    by_check: dict = defaultdict(lambda: {"pass": 0, "fail": 0, "severity": ""})
    for r in report.results:
        key = f"[{r.check_id}] {r.check_name}"
        by_check[key]["pass" if r.passed else "fail"] += 1
        by_check[key]["severity"] = r.severity

    for check, counts in by_check.items():
        table.add_row(
            check,
            str(counts["pass"]),
            str(counts["fail"]),
            counts["severity"],
        )

    console.print(table)
    console.print(
        f"\n[bold]Pass-Rate: {report.pass_rate}%[/bold] | "
        f"Total: {report.total_checks} | "
        f"Failed: {report.failed} | "
        f"Critical: {report.critical_fails}"
    )

    # Optional: als JSON speichern
    if output:
        Path(output).write_text(
            json.dumps(report.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        console.print(f"\nErgebnis gespeichert: {output}")


# ------------------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------------------

@cli.command("test-connection")
def test_connection():
    """Verbindungen zu Databricks und GWR API prüfen."""
    from src.utils.databricks_client import get_client
    from src.ingestion.gwr_fetcher import GWRFetcher

    console.print(Panel("[bold]Verbindungstest[/bold]"))

    # Databricks
    with console.status("Databricks..."):
        try:
            client = get_client()
            client.ensure_schema()
            console.print("[green]Databricks: OK[/green]")
        except Exception as exc:
            console.print(f"[red]Databricks: FEHLER — {exc}[/red]")

    # GWR API
    with console.status("GWR API..."):
        gwr_ok = GWRFetcher().test_connection()
        if gwr_ok:
            console.print("[green]GWR API: OK[/green]")
        else:
            console.print("[red]GWR API: FEHLER[/red]")


@cli.command()
def status():
    """Übersicht über den aktuellen Datenbestand."""
    from src.pipeline.gold import get_summary_stats

    with console.status("Lade Statistiken..."):
        stats = get_summary_stats()

    if not stats:
        console.print("[yellow]Keine Daten vorhanden. Bitte zuerst 'pipeline' ausführen.[/yellow]")
        return

    table = Table(title="Datenbestand Übersicht", show_header=False)
    table.add_column("Kennzahl", style="cyan", min_width=25)
    table.add_column("Wert", style="white")
    for k, v in stats.items():
        table.add_row(k, str(v) if v is not None else "—")
    console.print(table)


def _print_stats(stats: dict, label: str) -> None:
    table = Table(title=f"{label} Statistiken", show_header=False)
    table.add_column("Key", style="cyan")
    table.add_column("Value")
    for k, v in stats.items():
        table.add_row(str(k), str(v))
    console.print(table)



@cli.command("bronze-zip")
@click.argument("kanton")
@click.option("--dry-run", is_flag=True, default=False)
def bronze_zip(kanton: str, dry_run: bool):
    """ZIP-Vollabzug fuer einen Kanton. KANTON: z.B. ag, zh oder all."""
    from src.ingestion.gwr_zip_fetcher import GWRZipFetcher
    from src.pipeline.bronze import _ensure_bronze_table, _write_batch, BATCH_SIZE
    from src.utils.databricks_client import get_client
    import uuid
    from datetime import datetime, timezone
    fetcher = GWRZipFetcher()
    client = get_client() if not dry_run else None
    run_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc)
    console.print(Panel(f"[bold]Bronze ZIP[/bold] | Kanton: {kanton.upper()} | run_id: {run_id}", style="yellow"))
    if not dry_run:
        client.ensure_schema()
        _ensure_bronze_table(client)
    iterator = fetcher.fetch_kanton(kanton) if kanton.lower() != "all" else fetcher.fetch_all()
    buf, total = [], 0
    for record in iterator:
        record.update({"_ingested_at": now.isoformat(), "_ingestion_date": now.date().isoformat(),
                       "_run_id": run_id, "_batch_id": f"{run_id}_{total//BATCH_SIZE:04d}",
                       "_source": "gwr_zip", "_is_full_load": True})
        buf.append(record)
        if len(buf) >= BATCH_SIZE:
            if not dry_run: _write_batch(client, buf)
            total += len(buf)
            console.print(f"  {total:,} Records...")
            buf = []
    if buf:
        if not dry_run: _write_batch(client, buf)
        total += len(buf)
    console.print(f"\n[bold green]Fertig: {total:,} Records[/bold green]")

@cli.command("bronze-bulk")
@click.argument("kanton")
def bronze_bulk(kanton: str):
    """Schnelle Bulk-Ingestion via DBFS + COPY INTO. KANTON: z.B. ch, ag, zh"""
    from src.pipeline.bronze_bulk import run_bulk_ingestion
    console.print(Panel(f"[bold]Bronze Bulk[/bold] | Kanton: {kanton.upper()}", style="yellow"))
    stats = run_bulk_ingestion(kanton)
    _print_stats(stats, "Bulk Ingestion")

if __name__ == "__main__":
    cli()
