import logging

import eventepi.embed
import eventepi.wiki_util
from eventepi.classification import Trainer
from eventepi.corpus_reader import EpiCorpusReader
from eventepi.idb import IDB
from eventepi.scraper import ProMedScraper, WhoDonScraper

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


def main(headers):
    # Estimations from Summer 2020. More data will increase duration.
    WhoDonScraper(headers).scrape()  # Takes ~ 30 minutes (All timed on a Ryzen 3600)
    ProMedScraper(headers).scrape()  # Takes ~ six hours

    EpiCorpusReader().transform()  # Takes ~ 20 minutes

    IDB().preprocess()

    eventepi.wiki_util.main()  # Download an extraction of wiki dump took 2h.
    eventepi.embed.main()  # This took 3 days on a cluster with 50 cores (Not Ryzen 3600 but slightly slower).

    trainer = Trainer()
    trainer.train_key_entity_classifications()  # Takes ~ 1h 20 minutes on one core
    trainer.train_relevance_scoring()  # Takes ~ 6h on one core


if __name__ == "__main__":
    headers = {"your": "header"}
    main(headers)
