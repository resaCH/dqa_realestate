"""
src/ingestion/gwr_zip_fetcher.py
"""
from __future__ import annotations
import csv, io, zipfile
from typing import Iterator
import requests
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

BASE_URL = "https://public.madd.bfs.admin.ch"
KANTONE = ["ag","ai","ar","be","bl","bs","fr","ge","gl","gr","ju","lu","ne","nw","ow","sg","sh","so","sz","tg","ti","ur","vd","vs","zg","zh"]
FIELD_MAP = {"EGID":"egid","EGRID":"egrid","GDEKT":"gdekt","GGDENR":"ggdenr","GGDENAME":"ggdename","STRNAME":"strname","DEINR":"deinr","DPLZ4":"plz4","DPLZNAME":"plzname","GSTAT":"gstat","GKAT":"gkat","GKLAS":"gklas","GBAUJ":"gbauj","GBAUM":"gbaum","GBAUP":"gbaup","GABBJ":"gabbj","GAREA":"garea","GVOL":"gvol","GANZWHG":"ganzwhg","GAZZI":"gazzi","GKODE":"gkode","GKODN":"gkodn","GSCHUTZR":"gschutzr","GDATNS":"gdatns","GDAT":"gdat"}
INT_FIELDS = {"egid","ggdenr","gstat","gkat","gklas","gbauj","gbaum","gbaup","gabbj","ganzwhg","gazzi","gschutzr"}
FLOAT_FIELDS = {"garea","gvol","gkode","gkodn"}

class GWRZipFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "dqa-realestate/1.0"})

    def fetch_kanton(self, kanton: str) -> Iterator[dict]:
        kanton = kanton.lower()
        url = f"{BASE_URL}/{kanton}.zip"
        logger.info(f"Download: {url}")
        zip_bytes = self._download(url)
        logger.info(f"Download OK: {len(zip_bytes)/1024/1024:.1f} MB")
        yield from self._parse_zip(zip_bytes, kanton)

    def fetch_all(self, kantone: list[str] | None = None) -> Iterator[dict]:
        targets = [k.lower() for k in (kantone or KANTONE)]
        for i, kanton in enumerate(targets):
            logger.info(f"Kanton {kanton.upper()} ({i+1}/{len(targets)})")
            try:
                yield from self.fetch_kanton(kanton)
            except Exception as exc:
                logger.error(f"Kanton {kanton.upper()} fehlgeschlagen: {exc}")

    def get_zip_info(self, kanton: str) -> dict:
        url = f"{BASE_URL}/{kanton.lower()}.zip"
        resp = self.session.head(url, timeout=10)
        resp.raise_for_status()
        return {"kanton": kanton.upper(), "url": url,
                "size_mb": round(int(resp.headers.get("content-length", 0))/1024/1024, 1),
                "last_modified": resp.headers.get("last-modified", "?")}

    @retry(retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
           wait=wait_exponential(multiplier=1, min=5, max=60), stop=stop_after_attempt(3))
    def _download(self, url: str) -> bytes:
        chunks, downloaded = [], 0
        with self.session.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            for chunk in resp.iter_content(chunk_size=4*1024*1024):
                chunks.append(chunk)
                downloaded += len(chunk)
                if total and downloaded % (20*1024*1024) < 4*1024*1024:
                    logger.debug(f"  {downloaded/1024/1024:.0f}/{total/1024/1024:.0f} MB ({downloaded/total*100:.0f}%)")
        return b"".join(chunks)

    def _parse_zip(self, zip_bytes: bytes, kanton: str) -> Iterator[dict]:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            csv_name = next((n for n in zf.namelist() if "gebaeude" in n.lower() and n.endswith(".csv")), None)
            if not csv_name:
                logger.error(f"Keine Gebäude-CSV in ZIP: {zf.namelist()}")
                return
            logger.info(f"Parse: {csv_name}")
            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"), delimiter="\t")
                count = 0
                for raw in reader:
                    r = self._normalize(raw)
                    if r:
                        yield r
                        count += 1
                logger.info(f"{kanton.upper()}: {count:,} Gebäude")

    def _normalize(self, raw: dict) -> dict | None:
        record = {}
        for csv_col, field in FIELD_MAP.items():
            val = raw.get(csv_col, "").strip()
            if not val:
                record[field] = None
            elif field in INT_FIELDS:
                record[field] = _int(val)
            elif field in FLOAT_FIELDS:
                record[field] = _float(val)
            else:
                record[field] = val
        return record if record.get("egid") else None

def _int(v):
    try: return int(v)
    except: return None

def _float(v):
    try: return float(v)
    except: return None
    