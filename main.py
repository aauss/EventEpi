import logging
import subprocess
from glob import glob
from multiprocessing import cpu_count
from pathlib import Path

import requests

from eventepi.classification import EmbeddingNormalizer, Trainer
from eventepi.corpus_reader import EpiCorpusReader
from eventepi.idb import IDB
from eventepi.scraper import ProMedScraper, WhoDonScraper

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


def download_wikipedia_corpus():
    wiki_dump_url = (
        "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
    )
    wiki_folder = (Path(__file__).resolve().parent / Path("data/wikipedia")).mkdir(
        parents=True, exist_ok=True
    )
    file_path: Path = (wiki_folder / Path("enwiki-latest-pages-articles.xml.bz2"))

    with requests.get(wiki_dump_url, stream=True,) as r:
        r.raise_for_status()
        with open(file_path, "wb",) as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def extract_text_from_wiki_dump():
    subprocess.check_output(
        [
            "python",
            "eventepi/wiki_extractor.py",
            "data/wikipedia/enwiki-latest-pages-articles.xml.bz2",
            "-o",
            "data/wikipedia/corpus",
            "--threads",
            f"{cpu_count}",
        ]
    )


def train_embeddings():
    epi_articles = glob("data/corpus_processed/*/*.pickle")
    wiki_articles = glob("data/wikipedia/corpus/*/wiki*")
    article_paths = epi_articles + wiki_articles

    normalizer = EmbeddingNormalizer()

    sentences = (
        normalizer.normalize(normalizer.read_file(path)) for path in article_paths
    )


def main(headers):
    WhoDonScraper(headers).scrape()  # Takes ~ 30 minutes (All timed on a Ryzen 3600)
    ProMedScraper(headers).scrape()  # Takes ~ six hours

    EpiCorpusReader().transform()  # Takes ~ 20 minutes

    IDB().preprocess()

    download_wikipedia_corpus()
    extract_text_from_wiki_dump()
    train_embeddings()

    trainer = Trainer()
    trainer.train_key_entity_classifications()  # Takes ~ 1h 20 minutes on one core
    trainer.train_relevance_scoring()  # Takes ~ 6h on one core


if __name__ == "__main__":
    headers = {"your": "header"}
    main(headers)
