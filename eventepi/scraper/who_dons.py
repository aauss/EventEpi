import sys

from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

sys.path.append(str((Path(__file__).parent.resolve() / Path("../..")).resolve()))
from eventepi.scraper.base import BaseScraper


class WhoDonScraper(BaseScraper):
    """A scraper for the Disease Outbreak News (DON) of the World Health organization (WHO)."""

    def __init__(
        self,
        headers,
        path=(Path(__file__).parent.resolve() / Path("../../data/corpus/who_dons/")),
    ) -> None:
        """Initializes WhoDonScraper.

        Args:
            headers: Set header for scraping
            path: Path for scraped files.
        """
        self.corpus_path: Path = path
        self.headers: Dict[str, str] = headers
        self.scrapable_urls: List[Optional[str]] = []

    def scrape(self) -> None:
        """Scrapes all WHO DONs up to today."""
        if self.scrapable_urls == []:
            self.find_urls()
        self._scrape_and_write_to_file()

    def find_urls(self) -> None:
        """Finds URLs to all WHO DONs."""
        archive_urls_by_year = self._find_archives_by_year()
        for url in tqdm(archive_urls_by_year, desc="Find scrapable urls"):
            self.scrapable_urls.extend(self._find_urls_in_archived_year(url))

    def _scrape_and_write_to_file(self) -> None:
        (self.corpus_path).mkdir(parents=True, exist_ok=True)
        for url in tqdm(self.scrapable_urls, desc=f"Scrape WHO DONs"):
            self._write_to_file(url)

    def _find_archives_by_year(self) -> List[str]:
        archive = requests.get(
            "http://www.who.int/csr/don/archive/year/en/", headers=self.headers
        )
        archive_soup = BeautifulSoup(archive.content, "html.parser")
        archive_urls_per_year = archive_soup.find(
            "ul", attrs={"class": "list"}
        ).find_all("a")
        return [
            "http://www.who.int" + link.get("href") for link in archive_urls_per_year
        ]

    def _find_urls_in_archived_year(self, url: str) -> List[str]:
        archive_by_year = requests.get(url)
        soup_by_year = BeautifulSoup(archive_by_year.content, "html.parser")
        archive_year = soup_by_year.find("ul", attrs={"class": "auto_archive"})
        return [
            "http://www.who.int" + link.get("href")
            for link in archive_year.find_all("a")
        ]

    def _write_to_file(self, url: str) -> None:
        path = (self.corpus_path / url.split("/")[-3]).with_suffix(".html")
        if not path.exists():
            page = requests.get(url, headers=self.headers).content.decode("utf-8")
            with open(path, "w", encoding="utf-8") as f:
                f.write(page)


if __name__ == "__main__":
    scraper = WhoDonScraper(headers={"bla": "blu"})
    scraper.scrape()
