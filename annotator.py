import spacy
import numpy as np
from epitator.annotator import AnnoDoc
from epitator.geoname_annotator import GeonameAnnotator
from epitator.resolved_keyword_annotator import ResolvedKeywordAnnotator
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator
from itertools import groupby
from tqdm import tqdm
from typing import NamedTuple
from functools import wraps

spacy.load('en_core_web_sm')


class Entity(NamedTuple):
    """To have a name for the returned entity tuples"""

    entity: str
    resolved: list = []


def entity_tuple(entity_extractor):
    """Wraps the return value of and entity extractor into a Entity tuple"""

    @wraps(entity_extractor)
    def decorate(doc, **kwargs):
        resolved = entity_extractor(doc, kwargs)
        entity = entity_extractor.__name__
        if type(resolved) != list:
            resolved = [resolved]
        return Entity(entity, resolved)
    return decorate


def annotate(text):
    """Returns an document annotated for dates, disease counts, diseases, and geoneames

    :param text: a string to be annotated
    :return : an AnnoDoc object
    """
    doc = AnnoDoc(text)
    doc.add_tiers(GeonameAnnotator())
    doc.add_tiers(ResolvedKeywordAnnotator())
    doc.add_tiers(CountAnnotator())
    doc.add_tiers(DateAnnotator())
    return doc


@entity_tuple
def geonames(doc, raw=False):
    """Returns (the most occurring) geographical entity/entities in an annotated document

    :param doc: an annotated string
    :param raw: returns a not preprocessed annotation (Default False)
    :return:
    """
    geo_spans = doc.tiers["geonames"].spans
    if raw:
        return [geo_spans[i].geoname["name"] for i in range(len(geo_spans))]
    else:
        geonames = [geo_spans[i].geoname["name"] for i in range(len(geo_spans))]
        geoname_counts = [(key, len(list(group))) for key, group in groupby(sorted(geonames))]
        geoname_ranking = sorted(geoname_counts, key=lambda x: x[1], reverse=True)
        geoname_most_occure = [geoname[0] for geoname in geoname_ranking if geoname[1] == geoname_ranking[0][1]]
        return geoname_most_occure


@entity_tuple
def keywords(doc, raw=False):
    """Returns the most occurring disease entity in a annotated document

    :param doc: an annotated string
    :param raw: returns a not preprocessed annotation (Default False)
    :return:
    """
    keyword_spans = doc.tiers["resolved_keywords"].spans
    if raw:
        return [keyword_spans[i].resolutions[0]['entity']['label']
                for i in range(len(keyword_spans))
                if keyword_spans[i].resolutions['entity']['type'] == 'disease']

    else:
        keywords = [(keyword_spans[i].resolutions[0]['entity']['label'], keyword_spans[i].resolutions[0]["weight"])
                    for i in range(len(keyword_spans))
                    if keyword_spans[i].resolutions[0]['entity']['type']
                    == 'disease']

        # Ignores the included weights and only considers the most occurring disease name
        keywords_without_weight = [disease[0] for disease in keywords]
        keyword_counts = [(key, len(list(group))) for key, group in groupby(sorted(keywords_without_weight))]
        try:
            keyword = max(keyword_counts, key=lambda x: x[1])
        except ValueError:
            keyword = np.nan
        if type(keyword) is float:
            return keyword
        else:
            return keyword[0]  # Only returns the keyword, not the weight


@entity_tuple
def cases(doc, raw=False):
    """Returns the disease counts with the attribute "confirmed" in a annotated document

    doc -- an annotated string
    raw -- returns a not preprocessed annotation (Default False)
    """
    case_spans = doc.tiers["counts"].spans
    if raw:
        return [case_spans[i].metadata['count'] for i in range(len(case_spans))]
    else:
        return [case_spans[i].metadata['count'] for i in range(len(case_spans))
                if "confirmed" in case_spans[i].metadata['attributes']]


@entity_tuple
def dates(doc, raw=False):
    """Returns most mentioned date in a annotated document

    doc -- an annotated string
    raw -- returns a not preprocessed annotation (Default False)
    """
    date_spans = doc.tiers["dates"].spans
    dates = [date_spans[i].metadata["datetime_range"][0].strftime("%Y-%m-%d")
             for i in range(len(date_spans))]
    if raw:
        return dates
    else:
        date_count_tuple = [(key, len(list(group))) for key, group in groupby(sorted(dates))]
        try:
            date = max(date_count_tuple, key=lambda x: x[1])
        except ValueError:
            date = np.nan
        if type(date) is float:
            return date
        else:
            return date[0]


# Run this shit (a.k.a annotate all the scraped WHO DONs)
def create_annotated_database(texts, entity_funcs, raw=False):
    # TODO: create a dict, to specifically set raw for different annotators
    """Given a list of texts (str) annotate and extract disease keywords, geonames, and dates and return
    a dictionary of the text and the annotations

    texts -- a list of texts (str)
    raw -- returns a not preprocessed annotation (Default False)
    """
    if type(texts) == str:
        texts = [texts]
    database = {"texts": texts, "dates": [], "confirmed_cases": [], "keywords": [], "geonames": []}
    for i, text in enumerate(tqdm(texts)):
        doc = annotate(text)
        for entity_func in entity_funcs:
                try:
                    entity, resolved = entity_func(doc, raw)
                    database[entity].append(resolved)
                except TypeError as e:
                    print("Type error in text({})".format(i) + ": " + str(e))
    return database
