import shutil
import subprocess
from glob import glob
from multiprocessing import cpu_count
from pathlib import Path

import requests


def extract_text_from_wiki_dump():
    print("Extracting Wikipedia dump...")

    subprocess.check_output(
        [
            "python",
            "eventepi/wiki_extractor.py",
            str(
                Path(__file__).resolve().parent
                / "data/wikipedia/enwiki-latest-pages-articles.xml.bz2"
            ),
            "-o",
            "data/wikipedia/corpus",
            "--processes",
            f"{cpu_count()}",
        ]
    )
    print("Done")


def download_wikipedia_corpus():
    print("Downloading Wikipedia dump...")
    wiki_dump_url = (
        "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"
    )
    wiki_folder = Path(__file__).resolve().parent / Path("data/wikipedia")
    wiki_folder.mkdir(parents=True, exist_ok=True)
    file_path: Path = wiki_folder / Path("enwiki-latest-pages-articles.xml.bz2")
    with requests.get(wiki_dump_url, stream=True) as r:
        with open(file_path, "wb",) as f:
            shutil.copyfileobj(r.raw, f)
    print("Done")


def main():
    download_wikipedia_corpus()
    extract_text_from_wiki_dump()


if __name__ == "__main__":
    main()
