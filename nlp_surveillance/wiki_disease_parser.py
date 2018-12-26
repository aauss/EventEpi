import os
import pickle
import pandas as pd
from .utils.my_utils import get_results_sparql


def get_wiki_disease_df():
    endpoint_url = "https://query.wikidata.org/sparql"
    query = """SELECT Distinct ?item ?itemLabel_DE ?itemLabel_EN WHERE {
                    ?item wdt:P31 wd:Q12136.
                    OPTIONAL{
                    ?item rdfs:label ?itemLabel_DE.
                    FILTER (lang(?itemLabel_DE) = "de"). }
                    ?item rdfs:label ?itemLabel_EN.
                    FILTER (lang(?itemLabel_EN) = "en").
                    } order by ?item"""
    dirname = os.path.dirname(__file__)
    path_wiki = os.path.join(dirname, 'pickles', 'disease_wikidata.p')
    if not os.path.exists(path_wiki):
        disease_translation_df = get_results_sparql(endpoint_url, query)
        pickle.dump(disease_translation_df, open(path_wiki, 'wb'))
    else:
        disease_translation_df = pickle.load(open(path_wiki, 'rb'))

    # Get official RKI codes for disease names
    path_code = os.path.join(dirname, 'pickles', 'disease_code.p')
    if not os.path.exists(path_code):
        path = os.path.join(os.path.dirname(__file__),'data', 'diseaseCodes.csv')
        disease_code_df = pd.read_csv(path, ';')
        disease_code_df = disease_code_df[['Code', 'TypeName']]
        pickle.dump(disease_code_df, open(path_code, 'wb'))
    else:
        disease_code_df = pickle.load(open(path_code, 'rb'))

    return disease_translation_df, disease_code_df
