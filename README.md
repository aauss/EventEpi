This repo contains the work of **EventEpi**. EventEpi is a framework to allow event-based surveillance. The framework contains three parts that build up the workflow of EventEpi:

- `event_db_preprocessing` which contains exemplary preprocessing steps that might be necessary to utilize an epidemiological database.

- `scraper` which contains a scraper for
  - [WHO DONs](https://www.who.int/csr/don/en/) [ProMed Mail](https://www.promedmail.org) articles
  - Wikipedia's [sovereign state list](https://de.wikipedia.org/wiki/Liste_der_Staaten_der_Erde)
  - A query to [Wikidata](https://www.wikidata.org/) containing all disease names in English and German.

  Furthermore, it contains text extraction functionality to extrect relevant text from raw HTML or PDFs

- `classifer` which contains the modules to extract key named entities out of epidemiological texts and determine the relevance of articles given a codebase.

# Installation
```pip install -r requirements.txt```
```pip install didyoumean.py```
```pip install boilerpipe3```
- NLTK needs corpora
- EpiTator needs data

# Functionality

## Preprocessing
Has the goal to translate German country and disease names to be comparable to [EpiTators](https://github.com/ecohealthalliance/EpiTator) output. This is important for the classifer, since they operate on EpiTator's output and require labels that are comparable to EpiTator's output. The translation requires the Wikidata and Wikipedia scrape since they are used as a translation dictionary. Also, it contains functions to correct spelling using Levensthein Distance and formatting errors such as wrong types.

## Scraper
You can use the WHO and ProMed scraper like so which returns a list of articles. Look into the documentation for more options.

```python
from nlp_surveillance.scraper import promed_scraper, who_scraper

year_range = (2012, 2018)
proxy = {"http": "you_http_proxy", "https": "your_https_proxy"}
promed_articles = promed_scraper.scrape(year_range, proxy=proxy)

list_of_years = [2012, 2013, 2017]
who_articles = who_scraper.scrape(list_of_years, proxy=proxy)
```

and for the translation of German disease and country names into English we used the following Wikidata and Wikipedia tables:

```python
from nlp_surveillance.scraper import wikidata_diseases, wikipedia_countries

proxy = {"http_proxy": "you_http_proxy", "https_proxy": "your_https_proxy"}

df_of_sovereign_states = wikipedia_countries.scrape_wikipedia_countries()
df_of_ger_and_eng_disease_names = wikidata_diseases.disease_name_query(proxy)
```

## Classifer

The classifier are trained using an annotated epidemiological database. One classification task is to find the key entity out of all entities (disease, country, confirmed case numbers) that EpiTator detects. We use either an modal-approach or a naive Bayes classfier on senteces that contain an entity. If the entity of a text is also in the database, we give it a positive label.

Also, we learn to determine the relevance of an article using a database. For this, we compared several [classifier](/notebooks/classification.ipynb) using the bag-of-words approach on whole epidemiological articles given a label whether they are interesting or not or on the document embedding.
Our API looks like this:
```python
from nlp_surveillance.classfier import summarize

article = """As of May 5 there five confirmed cases of Ebola disease virus in the Democratic Republic Congo
"""

# You need to have trained your own key entity and relevance classifier
summary = summarize.annotate_and_summarize(article, key_date_classifer, key_count_classifer)

print(summary)
>>{"geoname": "Democratic Republic Congo", "disease": "Ebola disease virus", "counts": 5, "date": "2019-05-05"}
```
