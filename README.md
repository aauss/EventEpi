# EventEpi

## Installation
1. Install environment ```conda env create -f environment.yml```
2. Install external ressources 
- ```python -m nltk.downloader all``` 
- ```python -m spacy download en_core_web_md```
-  ```python -m epitator.importers.import_geonames```
-  ```python -m epitator.importers.import_disease_ontology```
- ```python -m epitator.importers.import_wikidata```