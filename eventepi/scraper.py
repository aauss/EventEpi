import re
from datetime import date
from json import JSONDecodeError
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm



class WhoDonScraper:
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


class ProMedScraper:
    """A scraper for the ProMED Mail posts."""

    def __init__(
        self,
        headers: Dict[str, str],
        path: Path = (
            Path(__file__).parent.resolve() / Path("../../data/corpus/promed/")
        ),
    ) -> None:
        """A scraper for ProMED mails.

        Args:
            headers: Headers for scraping.
            path: Path to write scraped files to.
        """
        self.corpus_path: Path = path
        self.headers: Dict[str, str] = headers
        self.ids: List[Optional[str]] = []
        self._failed_ids: list = []
        self._res_count: Optional[int] = None

    def scrape(self) -> None:
        """Finds and scrapes ProMED Mail posts."""
        print("Start searching for scrapable ProMED posts")
        self.find_scrapable_ids()
        print("Start scraping")
        self._scrape_ids()

    def find_scrapable_ids(
        self, start="01/01/1994", end=date.today().strftime("%m/%d/%Y")
    ) -> None:
        """Finds scrapable ProMED Mail ids within a date range

        Args:
            start: Start date for ID search (mm/dd/yy)
            end: End date for ID search (mm/dd/yy)
        """
        for page_num in range(200):
            response = self.make_search_request(page_num, start, end)
            ids = self._extract_ids(response)
            self.ids.extend(ids)
            if self._id_search_done(response):
                break
        if int(response["res_count"]) > 10000:
            self._update_search()

    def _scrape_ids(self) -> None:
        """Scrapes ProMED Mail posts."""
        with Pool(cpu_count() - 1) as p:
            with tqdm(total=len(self.ids)) as pbar:
                for _ in p.imap_unordered(self._scrape_id, self.ids):
                    pbar.update()

    def _scrape_id(self, id_) -> None:
        post = self.request_post(id_)
        if post:
            try:
                published_date = re.search(
                    r"Published Date:.*? (\d{4}-\d\d-\d\d)", post
                )[1]
            except TypeError:
                print("HAS NO DATE: ", id_)
                self._failed_ids.append(id_)
                return
            self._write_to_file(published_date, id_, post)

    def make_search_request(
        self, page_num: int, start: str, end: str
    ) -> Dict[str, Union[str, int]]:
        """Use ProMEDs search API for old posts.

        Args:
            start: Start date for ID search (mm/dd/yy)
            end: End date for ID search (mm/dd/yy)
        Returns:
            Reponse as JSON
        """
        try:
            return requests.post(
                "https://promedmail.org/wp-admin/admin-ajax.php",
                headers={"User-Agent": "Mozilla/5.0 Gecko/20100101 Firefox/75.0",},
                data={
                    "action": "get_promed_search_content",
                    "query[0][name]": "pagenum",
                    f"query[0][value]": {page_num},
                    "query[1][name]": "kwby1",
                    "query[1][value]": "summary",
                    "query[2][name]": "search",
                    "query[2][value]": "",
                    "query[3][name]": "date1",
                    f"query[3][value]": {start},
                    "query[4][name]": "date2",
                    f"query[4][value]": {end},
                    "query[5][name]": "feed_id",
                    "query[5][value]": "1",
                    "query[6][name]": "submit",
                    "query[6][value]": "next",
                },
            ).json()
        except JSONDecodeError:
            return {"res_count": -1, "results": ""}

    def _extract_ids(self, response) -> List[str]:
        try:
            soup = BeautifulSoup(response["results"], features="lxml")
        except KeyError:
            return []
        ids = soup.find_all(id=re.compile(r"id\d+"))
        return [id_.get("id").replace("id", "") for id_ in ids]

    def _id_search_done(self, response) -> bool:
        if not self._res_count:
            self._res_count = response["res_count"]
        print("\r" + f"IDs found {len(self.ids)}/{self._res_count}", end="")
        if len(set(self.ids)) == self._res_count:
            return True

    def _update_search(self) -> None:
        post = self.request_post(self.ids[-1])
        published_date = re.search(r"Published Date:.*? (\d{4}-\d\d-\d\d)", post)[1]
        self.find_scrapable_ids(end=pd.to_datetime(published_date).strftime("%m/%d/%Y"))

    def request_post(self, id_: str) -> str:
        """Request a ProMED Mail post using the ProMED API

        Args:
            id_: ProMED ID for request

        Returns:
            Requested article by ID
        """
        response = requests.post(
            "https://promedmail.org/wp-admin/admin-ajax.php",
            headers={"User-Agent": "Mozilla/5.0 Gecko/20100101 Firefox/75.0",},
            data={"action": "get_latest_post_data", "alertId": str(id_)},
        )
        try:
            return response.json()["post"]
        except (JSONDecodeError, TypeError):
            print("ERROR in post: ", id_, " ", response)
            self._failed_ids.append(id_)
            return ""

    def _write_to_file(self, published_date, id_, body) -> None:
        (self.corpus_path).mkdir(parents=True, exist_ok=True)
        path = self.corpus_path / f"{published_date}_id{id_}.html"
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)


if __name__ == "__main__":
    scraper = WhoDonScraper(headers={"bla": "blu"})
    scraper.scrape()
    try:
        s = ProMedScraper(headers={"thy": "though"})
        s.scrape()
    except Exception:
        print("Scraping failed")
        with open("checkpoint.txt", "w") as f:
            f.write("\n".join(s.ids))
