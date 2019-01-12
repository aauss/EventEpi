import pandas as pd
import numpy as np
import re
import unicodedata
import urllib.request

from SPARQLWrapper import SPARQLWrapper, JSON


def flatten_list(list_2d):
    # TODO: See whether this also could handle deeper nesting e.g. ["USA,["BLA",["a","b"]],"U]
    """Takes a nested list and returns a flattened list."""

    flattened = []
    for entry in list_2d:
        if isinstance(entry, str):
            flattened.append(entry)
        elif isinstance(entry, list):
            flattened.extend(flatten_list(entry))
        else:
            flattened.append(entry)

    return flattened


def split_and_flatten_list(to_split_and_flatten):
    # split, strip and then flatten
    return flatten_list(list(map(
                                lambda x: list(map(
                                    lambda y: y.strip(),
                                    x.split(',')))
                                if isinstance(x, str)
                                else x,
                                to_split_and_flatten)))


def matching_elements(l1, l2):
    if len(l1) >= len(l2):
        matches = [i for i in l2 if i in l1]
    else:
        matches = [i for i in l1 if i in l2]
    return matches


def get_results_sparql(endpoint_url, query):
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    df = pd.DataFrame(sparql.query().convert()["results"]["bindings"])
    return df.applymap(lambda x: x['value'] if isinstance(x, dict) else x)


def try_if_connection_is_possible():
    try:
        urllib.request.urlopen('https://www.rki.de/DE/Home/homepage_node.html', timeout=4)
        return True
    except urllib.request.URLError:
        return False


def remove_nans(to_clean):
    if not isinstance(to_clean, list):
        to_clean = [to_clean]
    return [entry for entry in to_clean if str(entry).lower() != 'nan']


def remove_guillemets(string):
    # Remove the first and last guillemets. Found in URLs of edb
    try:
        string = re.sub(r'<', '', string, 1)
        string = re.sub(r'>', '', string[::-1], 1)
        string = string[::-1]
    except TypeError:
        string = np.nan
    return string


def remove_control_characters(string):
    string = "".join(char for char in string if unicodedata.category(char)[0] != "C")
    return re.sub(r'(\s){2,}', ' ', string)


def get_sentence_and_date_from_annotated_span(annotated_span, anno):
    # Get the first and last occurrence the end of a sentence to create a window for slicing.
    # Slice text. -1 is used to omit trailing whitespace and + 2 to include the last period.
    text = anno.text
    start_of_text = re.search("(?s:.*)\S\.\s[A-Z]", text[:annotated_span.start]).span()[1]
    end_of_text = re.search(r'\S\.\s[A-Z]', text[annotated_span.end:]).span()[0]
    return text[start_of_text-1:annotated_span.end+end_of_text+2], annotated_span.datetime_range


def convert_open_epitator_range_to_smaller_range(annotated_span):
    pass


def check_url_validity(url):
    # Inspired from django
    regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    try:
        is_valid_url = re.match(regex, url) is not None
        return is_valid_url
    except TypeError:
        return False
