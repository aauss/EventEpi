from operator import itemgetter
# from tqdm import tqdm
# import pandas as pd
from memory_profiler import profile
from collections import defaultdict
from itertools import compress
from epitator.annotator import AnnoDoc
from epitator.geoname_annotator import GeonameAnnotator
from epitator.resolved_keyword_annotator import ResolvedKeywordAnnotator
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator

from utils.my_utils import delete_non_epitator_name_entity_tiers, return_most_occuring_string_in_list
from nlp_surveillance.classifier.extract_sentence import extract_sentence_from_found_entities

from nlp_surveillance.classifier.naive_bayes import remove_stop_words


# def annotate_and_summarize(df, clf_dates, clf_counts):
#     df['geonames'] = df['annotated'].apply()
#     # df_with_entites = _annotate_all_tiers(df)
#     #
#     # df_with_entites['geonames'] = df_with_entites['annotated'].apply(_choose_geonames)
#     # df_with_entites['disease'] = df_with_entites['annotated'].apply(_choose_disease)
#     # df_with_entites['date'] = df_with_entites['annotated'].apply(lambda x: _choose_date(x, clf_dates))
#     # df_with_entites['count'] = df_with_entites['annotated'].apply(lambda x: _choose_count(x, clf_counts))
#     # df_with_entites = df_with_entites.drop(columns='annotated')
#     return df_with_entites
@profile
def annotate_and_summarize(annotated, clf_dates, clf_counts):
    d = defaultdict(list)
    # for index, annotated in enumerate(tqdm(df['annotated'])):
    annotated = _annotate_all_tiers(annotated)
    d['geoname'].append(_choose_geonames(annotated))
    d['diseases'].append(_choose_disease(annotated))
    d['counts'].append(_choose_count(annotated, clf_counts))
    d['date'].append(_choose_date(annotated, clf_dates))
    # df.iat[index, 0] = None  # Set the just used annotated to None when done
    annotated = None
    return d


def _annotate_all_tiers(annotated):
    anno_tiers = [GeonameAnnotator(), CountAnnotator(), ResolvedKeywordAnnotator(), DateAnnotator()]
    if isinstance(annotated, AnnoDoc):
        for tier in anno_tiers:
            annotated.add_tiers(tier)
            annotated = delete_non_epitator_name_entity_tiers(annotated)
    return annotated


def _choose_geonames(annotated):
    try:
        geo_spans = annotated.tiers['geonames'].spans
        country_names = []
        for span in geo_spans:
            try:
                country_names.append(span.geoname.country_name)
            except AttributeError:
                continue

        most_frequent_country_name = return_most_occuring_string_in_list(country_names)
    except KeyError:
        most_frequent_country_name = None
    return most_frequent_country_name


def _choose_disease(annotated):
    try:
        keyword_spans = annotated.tiers["resolved_keywords"].spans

        entity_of_span = lambda x: keyword_spans[x].resolutions[0]['entity']
        found_diseases = [entity_of_span(i)['label'] for i in range(len(keyword_spans))
                          if entity_of_span(i)['type'] == 'disease']
        most_frequent_disease_name = return_most_occuring_string_in_list(found_diseases)
    except KeyError:
        most_frequent_disease_name = None
    return most_frequent_disease_name


def _choose_date(annotated, clf):
    try:
        classifier = clf
        extracted_sentences = extract_sentence_from_found_entities(annotated, 'dates')
        cleaned_sentences = [remove_stop_words(sentence) for sentence in extracted_sentences]
        list_of_proba_false_and_true = classifier.predict_proba(cleaned_sentences)
        mask_of_maximum = [True if all(i == max(list_of_proba_false_and_true, key=itemgetter(1)))
                           else False
                           for i in list_of_proba_false_and_true]
        date_spans = annotated.tiers['dates'].spans
        relevant_spans = list(compress(date_spans, mask_of_maximum))
        date = [span.datetime_range for span in relevant_spans]

    except (KeyError, ValueError) as  e:
        print(e)
        date = None
    return date


def _choose_count(annotated, clf):
    try:
        classifier = clf
        extracted_sentences = extract_sentence_from_found_entities(annotated, 'counts')
        cleaned_sentences = [remove_stop_words(sentence) for sentence in extracted_sentences]
        list_of_proba_false_and_true = classifier.predict_proba(cleaned_sentences)
        mask_of_maximum = [True if all(i == max(list_of_proba_false_and_true, key=itemgetter(1)))
                           else False
                           for i in list_of_proba_false_and_true]
        date_spans = annotated.tiers['counts'].spans
        relevant_spans = list(compress(date_spans, mask_of_maximum))
        count = [span.metadata['count'] for span in relevant_spans]
    except (KeyError, ValueError) as  e:
        print(e)
        count = None
    return count
