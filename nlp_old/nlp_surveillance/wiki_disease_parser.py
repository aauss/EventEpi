import os
import pickle
import pandas as pd
import re

from didyoumean import didyoumean
from .utils.my_utils import get_results_sparql


def translate_disease_name(disease):
    # Initialization of databases
    disease_translation_df, disease_code_df = get_wiki_disease_df()
    disease_db_en = disease_translation_df['itemLabel_EN']
    disease_db_de = [d for d in disease_translation_df['itemLabel_DE'] if isinstance(d, str)]

    # Translate
    disease = str(disease)
    disease = disease.strip()
    match_for_abbreviation = re.match(r'[A-Z]{2,}', disease)

    if match_for_abbreviation:
        index_for_matched_abbreviation = disease_code_df[disease_code_df['Code'] == match_for_abbreviation[0]]

        if not index_for_matched_abbreviation.empty:
            disease = index_for_matched_abbreviation['TypeName'].tolist()[0]

    if disease in disease_db_de:
        return disease_translation_df['itemLabel_EN'].loc[disease_translation_df['itemLabel_DE'] == disease].tolist()[0]
    elif disease in disease_db_en:
        return disease
    elif ',' in disease:
        # Did not translate because it is a concatenation of disease names
        disease = disease.split(',')
        disease = [entry.strip() for entry in disease]
        return [translate_disease_name(entry) for entry in disease]

    else:
        # If typo occurred
        did_u_mean = didyoumean.didYouMean(disease, disease_db_de)
        if did_u_mean in disease_db_en:
            return did_u_mean
        elif did_u_mean in disease_db_de:
            return disease_translation_df['itemLabel_EN'].loc[disease_translation_df['itemLabel_DE']
                                                              == did_u_mean].tolist()[0]
        else:
            complete_disease_name = _complete_partial_words(disease, disease_db_de, disease_db_en)
            if disease != complete_disease_name:
                return translate_disease_name(complete_disease_name)
            else:
                return disease


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
        path = os.path.join(os.path.dirname(__file__), 'data', 'diseaseCodes.csv')
        disease_code_df = pd.read_csv(path, ';')
        disease_code_df = disease_code_df[['Code', 'TypeName']]
        pickle.dump(disease_code_df, open(path_code, 'wb'))
    else:
        disease_code_df = pickle.load(open(path_code, 'rb'))

    return disease_translation_df, disease_code_df


def _complete_partial_words(to_complete, list_of_possible_corrections_de, list_of_possible_corrections_en):
    # Takes a list of correctly written en/ger disease names and tries to complete them: Ebola -> Ebolavirus
    match = [s for s in list_of_possible_corrections_de if s.lower().startswith(to_complete.lower())]
    if not match:
        match = [s for s in list_of_possible_corrections_en if s.lower().startswith(to_complete.lower())]
    if match:
        return match[0]
    else:
        return to_complete
