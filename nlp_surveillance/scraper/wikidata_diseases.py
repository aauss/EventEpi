import os

import pandas as pd

from SPARQLWrapper import SPARQLWrapper, JSON


def disease_name_query(proxy: dict = None) -> pd.DataFrame:
    """Queries English and German disease names form Wikidata

    Args:
        proxy (dict, optional): A dict for proxy values. E.g. {'http_proxy': '<YOUR_PROXY>'}

    Returns:
        Lookup of English and German disease names as DataFrame

    """
    endpoint_url = "https://query.wikidata.org/sparql"
    query = """SELECT Distinct ?itemLabel_DE   ?itemLabel_EN WHERE {
                    ?item wdt:P31 wd:Q12136.
                    OPTIONAL{
                    ?item rdfs:label ?itemLabel_DE.
                    FILTER (lang(?itemLabel_DE) = "de"). }
                    ?item rdfs:label ?itemLabel_EN.
                    FILTER (lang(?itemLabel_EN) = "en").
                    }"""
    disease_translation_df = _get_results_sparql(endpoint_url, query, proxy)
    return disease_translation_df


def _get_results_sparql(endpoint_url, query, proxy: dict = None):
    if proxy:
        for key, value in proxy.items():
            os.environ[key] = value
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    df = pd.DataFrame(sparql.query().convert()["results"]["bindings"])
    return df.applymap(lambda x: x['value'] if isinstance(x, dict) else x)
