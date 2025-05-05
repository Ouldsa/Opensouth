# scrapers.py – core scraping module for OpenSouth / OpenGlm
"""
Scrapers for election results, commune data, regional council, and Parliament
profiles.  Designed to plug into a Django/OpenParliament‑style backend.

Usage examples (CLI):
    # Download 2021 commune & regional election results once
    python scrapers.py elections --year 2021 --province "Guelmim"

    # Sync councillor roster + minutes PDFs for Commune de Guelmim
    python scrapers.py commune --output data/commune

    # Sync regional council documents (budgets, sessions)
    python scrapers.py region --output data/region

    # Cache MP profile pages (parliament.ma)
    python scrapers.py parliament --output data/parliament

All scrapers emit two artefacts:
    1) Raw files (PDF, HTML) in the output folder, organised by date.
    2) A *_index.json file describing the structured data extracted, which
       can later be loaded into Django management‑commands.

Dependencies (add these to pyproject.toml or requirements.txt):
    requests
    requests_html
    beautifulsoup4
    pdfplumber
    pytesseract
    pillow (PIL)
    tqdm
    click  # lightweight CLI parser

pytesseract assumes that Tesseract‑OCR is installed system‑wide and that the
Arabic and French language packs are present (`apt-get install tesseract-ocr-ara
 tesseract-ocr-fra`).

"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession
from tqdm import tqdm

try:
    import pdfplumber
    import pytesseract
    from PIL import Image
except ImportError:
    pdfplumber = None  
    pytesseract = None 

logger = logging.getLogger("opensouth.scrapers")
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)

###############################################################################
# Helper & base classes
###############################################################################

def sha1(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def slugify(text: str) -> str:
    """Very simple slugify helper (ASCII only)."""
    text = re.sub(r"[^\w\-]+", "-", text.lower())
    return re.sub(r"-+", "-", text).strip("-")


def safe_request(url: str, session: Optional[requests.Session] = None, **kw) -> requests.Response:
    sess = session or requests.Session()
    for retry in range(3):
        try:
            resp = sess.get(url, timeout=30, **kw)
            resp.raise_for_status()
            return resp
        except Exception as err:
            logger.warning("%s – retry %d/3", err, retry + 1)
            time.sleep(3 * (retry + 1))
    raise RuntimeError(f"Failed to fetch {url} after 3 attempts")


@dataclass
class BaseScraper:
    output_dir: Path

    def run(self):
        raise NotImplementedError

    def _save_binary(self, content: bytes, rel_path: Path) -> Path:
        """Save raw binary content and return absolute path."""
        file_path = self.output_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)
        return file_path

    def _save_json(self, data: Any, rel_path: Path) -> Path:
        file_path = self.output_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        return file_path

###############################################################################
# ElectionsScraper – results 2021 (commune + region + parliament)
###############################################################################

class ElectionsScraper(BaseScraper):
    """Scrape static HTML tables from elections.ma interactive results."""

    BASE_URL = (
        "https://www.elections.ma/elections/communales-provinciales/resultats/"
    )

    def __init__(self, year: int = 2021, province: str = "Guelmim", **kw):
        super().__init__(**kw)
        self.year = year
        self.province = province
        self.session = HTMLSession()

    # ---------------------------------------------------------------------
    def run(self):
        logger.info("Fetching election results for province = %s", self.province)
        # Step 1: load the results page and wait for JS to render tables
        url = (
            "https://www.elections.ma/elections/communales/resultats?province="
            f"{self.province}&annee={self.year}"
        )
        r = self.session.get(url)
        r.html.render(sleep=2, timeout=60)
        tables = r.html.find("table")
        if not tables:
            raise RuntimeError("No table found – did the site change?")

        results: List[Dict[str, Any]] = []
        for tbl in tables:
            headers = [th.text.strip() for th in tbl.find("thead th")]
            for row in tbl.find("tbody tr"):
                cells = [td.text.strip() for td in row.find("td")]
                if len(cells) != len(headers):
                    continue
                item = dict(zip(headers, cells))
                item["source_url"] = url
                results.append(item)

        # Save JSON index
        self._save_json(results, Path(f"elections_{self.year}_{slugify(self.province)}.json"))
        logger.info("Saved %d result rows", len(results))

###############################################################################
# CommuneScraper – councillors roster + minutes PDFs
###############################################################################

class CommuneScraper(BaseScraper):
    """Scrape commune website (roster, minutes, budgets)."""

    BASE = "https://communeguelmim.ma"
    ROSTER_SLUG  = "تشكيلة-المجلس"
    MINUTES_SLUG = "المجلس-الجماعي/ملخص-مقررات-الدورات"
    BUDGETS_SLUG = "الميزانية-المفتوحة/ميزانية-المواطن"

    def _url(self, slug: str) -> str:
        return f"{self.BASE}/ar/{slug}"
    
    def run(self):
        self._scrape_roster()
        self._scrape_minutes()
        self._scrape_budgets()

    # ------------------------------------------------------------------
    def _scrape_roster(self):
        url = self._url(self.ROSTER_SLUG)  
        soup = BeautifulSoup(safe_request(url).text, "html.parser")
        members: List[Dict[str, Any]] = []
        for card in soup.select("div.council-member, div.team-member, li.council"):
            name = card.select_one("h3").get_text(strip=True)
            position = card.select_one("p.position").get_text(strip=True) if card.select_one("p.position") else ""
            img_url = card.select_one("img")["src"]
            members.append({
                "name": name,
                "position": position,
                "photo_url": img_url,
                "source_url": url,
            })
        self._save_json(members, Path("commune/roster.json"))
        logger.info("Roster: %d members", len(members))

    # ------------------------------------------------------------------
    def _scrape_minutes(self):
        url = self._url(self.MINUTES_SLUG)
        soup = BeautifulSoup(safe_request(url).text, "html.parser")
        links = soup.select("a[href$='.pdf']")
        index: List[Dict[str, Any]] = []
        for a in tqdm(links, desc="minutes"):
            pdf_url = a["href"] if a["href"].startswith("http") else f"{self.BASE}{a['href']}"
            title = a.get_text(strip=True)
            pdf_bytes = safe_request(pdf_url).content
            digest = sha1(pdf_bytes)
            rel_path = Path("commune/minutes") / f"{digest}.pdf"
            self._save_binary(pdf_bytes, rel_path)
            index.append({
                "title": title,
                "source_url": pdf_url,
                "sha1": digest,
                "file": str(rel_path),
            })
        self._save_json(index, Path("commune/minutes_index.json"))

    # ------------------------------------------------------------------
    def _scrape_budgets(self):
        url = self._url(self.BUDGETS_SLUG)  # Budget citoyen page
        soup = BeautifulSoup(safe_request(url).text, "html.parser")
        links = soup.select("a[href$='.pdf']")
        idx: List[Dict[str, Any]] = []
        for a in links:
            pdf_url = a["href"] if a["href"].startswith("http") else f"{self.BASE}{a['href']}"
            year_match = re.search(r"(\d{4})", pdf_url)
            year = year_match.group(1) if year_match else "unknown"
            pdf_bytes = safe_request(pdf_url).content
            digest = sha1(pdf_bytes)
            rel_path = Path("commune/budgets") / f"{digest}.pdf"
            self._save_binary(pdf_bytes, rel_path)
            idx.append({"year": year, "source_url": pdf_url, "sha1": digest, "file": str(rel_path)})
        self._save_json(idx, Path("commune/budgets_index.json"))
        logger.info("Budgets: %d", len(idx))

###############################################################################
# RegionScraper – Région Guelmim‑Oued Noun
###############################################################################

class RegionScraper(BaseScraper):
    BASE = "https://rgon.ma"

    def run(self):
        self._scrape_sessions()
        self._scrape_budgets()

    # ---------------------------------------------------------------
    def _scrape_sessions(self):
        url = f"{self.BASE}/category/%D8%AF%D9%88%D8%B1%D8%A7%D8%AA-%D8%A7%D9%84%D9%85%D8%AC%D9%84%D8%B3"  # Session category
        soup = BeautifulSoup(safe_request(url).text, "html.parser")
        links = soup.select("article a[href$='.pdf']")
        idx: List[Dict[str, Any]] = []
        for a in links:
            pdf_url = a["href"]
            title = a.get_text(strip=True)
            pdf_bytes = safe_request(pdf_url).content
            digest = sha1(pdf_bytes)
            rel_path = Path("region/sessions") / f"{digest}.pdf"
            self._save_binary(pdf_bytes, rel_path)
            idx.append({"title": title, "source_url": pdf_url, "sha1": digest, "file": str(rel_path)})
        self._save_json(idx, Path("region/sessions_index.json"))
        logger.info("Regional sessions: %d", len(idx))

    # ---------------------------------------------------------------
    def _scrape_budgets(self):
        url = f"{self.BASE}/category/%D9%85%D9%8A%D8%B2%D8%A7%D9%86%D9%8A%D8%A7%D8%AA"  # Budgets category
        soup = BeautifulSoup(safe_request(url).text, "html.parser")
        links = soup.select("article a[href$='.pdf']")
        idx: List[Dict[str, Any]] = []
        for a in links:
            pdf_url = a["href"]
            pdf_bytes = safe_request(pdf_url).content
            digest = sha1(pdf_bytes)
            rel_path = Path("region/budgets") / f"{digest}.pdf"
            self._save_binary(pdf_bytes, rel_path)
            idx.append({"source_url": pdf_url, "sha1": digest, "file": str(rel_path)})
        self._save_json(idx, Path("region/budgets_index.json"))
        logger.info("Regional budgets: %d", len(idx))

###############################################################################
# ParliamentScraper – MP profiles & activity
###############################################################################

class ParliamentScraper(BaseScraper):
    BASE = "https://www.parlement.ma"

    def __init__(self, constituency: str = "كلميم", **kw):
        super().__init__(**kw)
        self.constituency = constituency
        self.session = requests.Session()

    def run(self):
        # There is no search API, but member listing page can be filtered by Arabic query param ?field=...
        # Fallback: hit the HTML sitemap and grep for the constituency.
        url = f"{self.BASE}/ar/members"
        soup = BeautifulSoup(safe_request(url, self.session).text, "html.parser")
        cards = soup.select("div.views-row")
        idx: List[Dict[str, Any]] = []
        for c in cards:
            if self.constituency not in c.get_text():
                continue
            link = c.select_one("a")
            profile_url = f"{self.BASE}{link['href']}"
            name = link.get_text(strip=True)
            profile_html = safe_request(profile_url, self.session).text
            profile_soup = BeautifulSoup(profile_html, "html.parser")
            party = profile_soup.select_one("div.field--name-field-party span").get_text(strip=True) if profile_soup.select_one("div.field--name-field-party span") else ""
            idx.append({
                "name": name,
                "party": party,
                "constituency": self.constituency,
                "profile_url": profile_url,
            })
        self._save_json(idx, Path("parliament/members.json"))
        logger.info("MP profiles saved: %d", len(idx))

###############################################################################
# CLI dispatch
###############################################################################

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="OpenSouth scrapers CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    # Elections
    sp = sub.add_parser("elections", help="Scrape election results")
    sp.add_argument("--year", type=int, default=2021)
    sp.add_argument("--province", default="Guelmim")
    sp.add_argument("--output", default="data")

    # Commune
    sp = sub.add_parser("commune", help="Scrape commune site")
    sp.add_argument("--output", default="data")

    # Region
    sp = sub.add_parser("region", help="Scrape regional site")
    sp.add_argument("--output", default="data")

    # Parliament
    sp = sub.add_parser("parliament", help="Scrape parliament profiles")
    sp.add_argument("--constituency", default="كلميم")
    sp.add_argument("--output", default="data")

    return p


def main(argv: List[str] | None = None):
    args = build_parser().parse_args(argv)
    output_path = Path(args.output).expanduser()

    if args.cmd == "elections":
        ElectionsScraper(output_dir=output_path, year=args.year, province=args.province).run()
    elif args.cmd == "commune":
        CommuneScraper(output_dir=output_path).run()
    elif args.cmd == "region":
        RegionScraper(output_dir=output_path).run()
    elif args.cmd == "parliament":
        ParliamentScraper(output_dir=output_path, constituency=args.constituency).run()
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
