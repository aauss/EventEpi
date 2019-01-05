import spacy
import numpy as np
from epitator.annotator import AnnoDoc
from epitator.geoname_annotator import GeonameAnnotator
from epitator.resolved_keyword_annotator import ResolvedKeywordAnnotator
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator
from itertools import groupby
from tqdm import tqdm
from types import MethodType

from .EntityTuple import entity_tuple
from .utils.my_utils import remove_control_characters

spacy.load('en_core_web_sm')


def create_annotated_database(texts, entity_funcs_and_params=None):
    """Given a list of texts (str) annotate and extract disease keywords, geonames, and dates and return
    a dictionary of the text and the annotations
    """
    if entity_funcs_and_params is None:
        entity_funcs_and_params = [geonames, cases, dates, keywords]
    if isinstance(texts, str):
        texts = [texts]
    if not isinstance(entity_funcs_and_params, list):
        entity_funcs_and_params = [entity_funcs_and_params]

    # Convert all the functions not in tuples into tuples with empty kwargs
    entity_funcs_and_params = transform_to_tuple_of_function_and_arguments(entity_funcs_and_params)
    database = {"texts": texts, "dates": [], "cases": [], "keywords": [], "geonames": []}

    for i, text in enumerate(tqdm(texts)):
        doc = annotate(text)
        for entity_func, kwargs in entity_funcs_and_params:
            try:
                entity, resolved = entity_func(doc, **kwargs)
                database[entity].append(resolved)
            except ValueError as e:
                print("Type error in text({})".format(i) + ": " + str(e))
    return database


def annotate(text, tiers=None):
    # Returns an annotated text
    doc = AnnoDoc(remove_control_characters(text))
    # Add bound method to delete spacy tiers. Necessary because pickling does not work otherwise
    doc.delete_non_epitator_name_entity_tiers = MethodType(delete_non_epitator_name_entity_tiers, doc)
    if tiers is None:
        doc.add_tiers(GeonameAnnotator())
        doc.add_tiers(ResolvedKeywordAnnotator())
        doc.add_tiers(CountAnnotator())
        doc.add_tiers(DateAnnotator())
    else:
        if not isinstance(tiers, list):
            tiers = [tiers]
        list(map(lambda tier: doc.add_tiers(eval(tier)), tiers))

    return doc


@entity_tuple
def geonames(doc, raw=False):
    # Returns (the most occurring) geographical entity/entities in an annotated document

    geo_spans = doc.tiers["geonames"].spans
    if raw:
        return [geo_spans[i].geoname["name"] for i in range(len(geo_spans))]
    else:
        geonames_ = [geo_spans[i].geoname["name"] for i in range(len(geo_spans))]
        geoname_counts = [(key, len(list(group))) for key, group in groupby(sorted(geonames_))]
        geoname_ranking = sorted(geoname_counts, key=lambda x: x[1], reverse=True)
        geoname_most_occure = [geoname[0] for geoname in geoname_ranking if geoname[1] == geoname_ranking[0][1]]
        return geoname_most_occure


@entity_tuple
def keywords(doc, raw=False, with_label=False):
    # Returns the (most occurring) disease entity in a annotated document

    keyword_spans = doc.tiers["resolved_keywords"].spans
    if raw:
        if not with_label:
            return [keyword_spans[i].resolutions[0]['entity']['label']
                    for i in range(len(keyword_spans))
                    if keyword_spans[i].resolutions[0]['entity']['type'] == 'disease']
        else:
            return [keyword_spans[i].resolutions[0]['entity'] for i in range(len(keyword_spans))]

    else:
        if not with_label:
            keywords_ = [(keyword_spans[i].resolutions[0]['entity']['label'], keyword_spans[i].resolutions[0]["weight"])
                         for i in range(len(keyword_spans)) if keyword_spans[i].resolutions[0]['entity']['type']
                         == 'disease']
            # Ignores the included weights and only considers the most occurring disease name
            keywords_without_weight = [disease[0] for disease in keywords_]
            keyword_counts = [(key, len(list(group))) for key, group in groupby(sorted(keywords_without_weight))]
            try:
                keyword = max(keyword_counts, key=lambda x: x[1])
            except ValueError:
                keyword = np.nan
            if type(keyword) == float:
                return keyword
            else:
                return keyword[0]  # Only returns the keyword, not the weight
        else:
            keywords_ = [keyword_spans[i].resolutions[0]['entity'] for i in range(len(keyword_spans))
                         if keyword_spans[i].resolutions[0]['entity']['type'] == 'disease']
            keyword_counts = [(key, len(list(group))) for key, group in groupby(keywords_)]
            try:
                keyword = max(keyword_counts, key=lambda x: x[1])
            except ValueError:
                keyword = np.nan
            if type(keyword) == dict:
                return keyword
            else:
                return keyword[0]


@entity_tuple
def cases(doc, raw=False):
    # Returns the disease counts (with the attribute "confirmed") in a annotated document

    case_spans = doc.tiers["counts"].spans
    if raw:
        return [case_spans[i].metadata['count'] for i in range(len(case_spans))]
    else:
        return [case_spans[i].metadata['count'] for i in range(len(case_spans))
                if "confirmed" in case_spans[i].metadata['attributes']]


@entity_tuple
def dates(doc, raw=False):
    # Returns (most mentioned) date in a annotated document

    date_spans = doc.tiers["dates"].spans
    dates_ = [date_spans[i].metadata["datetime_range"][0].strftime("%Y-%m-%d")
              for i in range(len(date_spans))]
    if raw:
        return dates_
    else:
        date_count_tuple = [(key, len(list(group))) for key, group in groupby(sorted(dates_))]
        try:
            date = max(date_count_tuple, key=lambda x: x[1])
        except ValueError:
            date = np.nan
        if type(date) is float:
            return date
        else:
            return date[0]


def transform_to_tuple_of_function_and_arguments(entity_funcs_and_params):
    return [(should_be_tuple, {}) if callable(should_be_tuple)
            else should_be_tuple
            for should_be_tuple in entity_funcs_and_params]


def delete_non_epitator_name_entity_tiers(self):
    del self.tiers["spacy.nes"]
    del self.tiers["spacy.noun_chunks"]
    del self.tiers["spacy.sentences"]
    del self.tiers["spacy.tokens"]
    del self.tiers["nes"]
    del self.tiers["ngrams"]
    del self.tiers["tokens"]
