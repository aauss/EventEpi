import pandas as pd
import tika
import re
import unicodedata
from tika import parser
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
from boilerpipe.extract import Extractor
tika.TikaClientOnly = True


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
    # TODO: Rewrite to a function that only takes one link. Therefore, search for all uses of extract_from_url
    """Extracts the main content from a list of links and returns a list of texts (str)

    list_of_links -- a list containing URLs of webpages to get the main content from
    """
    if type(list_of_links) == str:
        list_of_links = [list_of_links]
    return [Extractor(extractor='ArticleExtractor', url=url).getText().replace('\n', '') for url in tqdm(list_of_links)]


def extract_from_pdf(url):
    raw = parser.from_file(url)
    return raw['content']


def get_results_sparql(endpoint_url, query):
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    df = pd.DataFrame(sparql.query().convert()["results"]["bindings"])
    return df.applymap(lambda x: x['value'] if isinstance(x, dict) else x)


def remove_nans(to_clean):
    return [entry for entry in to_clean if str(entry).lower() != 'nan']


def remove_guillemets(string):
    return re.sub(r'[<>]', '', string)


def remove_control_characters(string):
    return "".join(char for char in string if unicodedata.category(char)[0] != "C")


def get_sentence(annotated_span, text):
    # Get the first and last occurrence the end of a sentence to create a window for slicing.
    # Slice text. -1 is used to omit trailing whitespace and + 2 to include the last period.
    start_of_text = re.search("(?s:.*)\S\.\s[A-Z]", text[:annotated_span.start]).span()[1]
    end_of_text = re.search(r'\S\.\s[A-Z]', text[annotated_span.end:]).span()[0]
    return text[start_of_text-1:annotated_span.end+end_of_text+2]
