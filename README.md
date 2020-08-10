# EventEpi

## Installation

1. Install environment `conda env create -f environment.yml`
2. Install external ressources

- `python -m nltk.downloader all`
- `python -m spacy download en_core_web_md`
- `python -m epitator.importers.import_geonames`
- `python -m epitator.importers.import_disease_ontology`
- `python -m epitator.importers.import_wikidata`

## Run

To try out EventEpi's functionality, run `python main.py`. This will start downloading news articles from WHO and ProMED Mail and the most recent Wikipedia dump. Afterwards, it generates a corpus out of both sources that is used to train word embeddings which are later used to apply relevance scoring and key entity extraction on epidemiological texts.

## Notebooks

One you run the main script, you can eyeball the data, look at how we plotted the data and how the corpus can be accessed using our corpus reader.
