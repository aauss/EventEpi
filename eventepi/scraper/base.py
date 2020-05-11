from abc import ABC, abstractmethod
from pathlib import Path
from typing import DefaultDict, Dict, List, Union


class BaseScraper(ABC):
    """An abstract class for news scraper."""

    def __init__(self, path: str, headers: Dict[str, str]) -> None:
        """Initiliazes scraper.

        Args:
            path: Path for scraped files.
            headers: Set header for scraping
        """
        self.path = path
        self.headers = headers

    @abstractmethod
    def scrape(self) -> None:
        """An abstract method that scrapes news up to the most recent one."""
        pass
