# EventEpi
[EventEpi](https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1008277) is a framework that shows how natural language processing and machine learning can help improve disease surveillance tasks (more specifically event-based surveillance).  
## Installation
To run the code and reproduce the results in the paper, you have to install the necessary libraries and ontologies.

1. Install environment `conda env create -f environment.yml`
2. Install external ressources

- `python -m nltk.downloader all`
- `python -m spacy download en_core_web_md`
- `python -m epitator.importers.import_geonames`
- `python -m epitator.importers.import_disease_ontology`
- `python -m epitator.importers.import_wikidata`

## Embeddings
If you got the embeddings from [figshare](https://figshare.com/articles/dataset/Epidemiological_word_embeddings_300_dimensional_/12575966) you can use them like this:

```python
from gensim.models import KeyedVectors

embeddings = KeyedVectors.load("embeddings_300").wv
```

## Structure
In the following I will briefly introduce the modules of this project by describing what they do and what they output.

### Scraper
The two main external data sources for this project are WHO's Disease Outbreak News and ProMED Mail events. The relevant texts can be downloaded using the `eventepi/scraper.py`. They will be saved under `data/corpus/`.

### Corpus
The scraped articles need to be preprocessed to feed them into the machine learning pipeline. The scraped data can be transformed to a corpus using `eventepi/corpus_reader.py`. It will create a corpus using NLTK's API where the articles are processed and serialized. The final corpus is saved under `data/corpus_processed/`

### IDB
The `eventepi/idb.py` module contains the code to preprocess the  incidence data base (IDB) that contains relevant outbreak events that were collected as part of event-based surveillance. It is a table that contains information on disease outbreaks and their respective key information The IDB itself is located under `data/idb.csv`. 

`eventepi/idb.py` is a module that reads and cleans the IDB. This preprocessing of the IDB allows us to later run our machine learning pipeline. The final processed IDB is saved under `data/idb_processed.csv`. 

### Embeddings
This project contains custom word embeddings. To build them, we will combine the scraped news articles and Wikipedia articles to train a word2vec model. First, we will download the latest Wikipedia dump and extract it by running `eventepi/wiki_util.py`. The dump and the extracted corpus will be saved under `data/wikipedia/`.

Second, we will use the WHO/ProMED corpus and the Wikipedia corpus to train a word2vec model. To do this, run `eventepi/embed.py`. The result will be saved under `data\embeddings`.

This whole process takes several days and I recommend using our final embeddings from [figshare](https://figshare.com/articles/dataset/Epidemiological_word_embeddings_300_dimensional_/12575966).

### ML-Pipeline
Finally, we can feed the corpus, the IDB, and the embeddings into our machine learning pipeline.

The whole pipeline to train the relevance scoring and entity recognition of this project is run by executing `python eventepi/classification.py`. 

EventEpi's whole functionality is wrapped into `main.py`. This will start downloading news articles from WHO and ProMED Mail and the most recent Wikipedia dump. Afterwards, it generates a corpus out of both sources that is used to train word embeddings which are later used to apply relevance scoring and key entity extraction on epidemiological texts.


## Notebooks

There are also some notebooks that help you understand this repo. You can eyeball the IDB (`notebooks/IDB_eyeballing.ipynb`), by looking at how we plotted the data and how the corpus can be accessed using the corpus reader (`notebooks/corpus_functionality.ipynb`).

You can see the base line for the entity extraction task in (`notebooks/disease_country_performance.ipynb`) which is build on top of `eventepi/summarize.py`.

`notebooks/cnn.ipynb` contains the code to train the convolutional neural network and display the layer-wise relevance propagation.

## Contact
If you have any questions, please don't hesitate to get in touch by contacting me here at GitHub!
