import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
from boilerpipe.extract import Extractor


def flatten_list(list_2d):
    # TODO: See whether this also could handle deeper nesting e.g. ["USA,["BLA",["a","b"]],"U]
    """Takes a nested list and returns a flattened list."""

    flattened = []
    for entry in list_2d:
        if type(entry) == str:
            flattened.append(entry)
        else:
            flattened.extend(flatten_list(entry))
    return flattened


def matching_elements(l1, l2):
    if len(l1) >= len(l2):
        matches = [i for i in l2 if i in l1]
    else:
        matches = [i for i in l1 if i in l2]
    return matches


def extract_from_url(list_of_links):
    """Extracts the main content from a list of links and returns a list of texts (str)

    list_of_links -- a list containing URLs of webpages to get the main content from
    """
    if type(list_of_links) == str:
        list_of_links = [list_of_links]
    return[Extractor(extractor='ArticleExtractor', url=url).getText().replace('\n', '') for url in tqdm(list_of_links)]


def get_results_sparql(endpoint_url, query):
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    df = pd.DataFrame(sparql.query().convert()["results"]["bindings"])
    return df.applymap(lambda x: x['value'] if isinstance(x, dict) else x)

