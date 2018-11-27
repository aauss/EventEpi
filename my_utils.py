import pandas as pd
from tqdm import tqdm
from boilerpipe.extract import Extractor
from edb_clean import *


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


def extract_from_url(list_of_links):
    """Extracts the main content from a list of links and returns a list of texts (str)

    list_of_links -- a list containing URLs of webpages to get the main content from
    """
    if type(list_of_links) == str:
        list_of_links = [list_of_links]
    return[Extractor(extractor='ArticleExtractor', url=url).getText().replace('\n', '') for url in tqdm(list_of_links)]


def get_edb():
    """

    Returns:
        pd.DataFrame: formatted edb
    """
    edb = pd.read_csv("edb.csv", sep=";")
    edb = edb.dropna(how="all").reset_index(drop=True)
    edb.columns = list(map(lambda x: x.strip(" "), edb.columns))
    edb
    return edb
