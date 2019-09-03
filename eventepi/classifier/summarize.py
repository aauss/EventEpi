from operator import itemgetter
from collections import defaultdict
from itertools import compress

from epitator.annotator import AnnoDoc
from epitator.geoname_annotator import GeonameAnnotator
from epitator.resolved_keyword_annotator import ResolvedKeywordAnnotator
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator

from eventepi.my_utils import return_most_occuring_string_in_list
from eventepi.classifier.extract_sentence import extract_entities_with_sentence


def annotate_and_summarize(text, clf_dates, clf_counts):
    d = defaultdict(list)
    annotated = _annotate_all_tiers(text)
    d['geoname'].append(_choose_geonames(annotated))
    d['diseases'].append(_choose_disease(annotated))
    d['counts'].append(_extract_entity_with_naive_bayes(annotated, clf_counts, "counts"))
    d['date'].append(_extract_entity_with_naive_bayes(annotated, clf_dates, "dates"))
    return d


def _annotate_all_tiers(text):
    annotated = AnnoDoc(text)
    anno_tiers = [GeonameAnnotator(), CountAnnotator(), ResolvedKeywordAnnotator(), DateAnnotator()]
    for tier in anno_tiers:
        annotated.add_tiers(tier)
    return annotated


def _choose_geonames(annotated: AnnoDoc):
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


def _choose_disease(annotated: AnnoDoc):
    try:
        keyword_spans = annotated.tiers["resolved_keywords"].spans
        entity_of_span = lambda x: keyword_spans[x].resolutions[0]['entity']
        found_diseases = [entity_of_span(i)['label']
                          for i in range(len(keyword_spans))
                          if entity_of_span(i)['type'] == 'disease']
        most_frequent_disease_name = return_most_occuring_string_in_list(found_diseases)
    except KeyError:
        most_frequent_disease_name = None
    return most_frequent_disease_name


def _extract_entity_with_naive_bayes(annotated, clf, entity):
    try:
        classifier = clf
        _, extracted_sentences = extract_entities_with_sentence(annotated, entity)
        list_of_proba_false_and_true = classifier.predict_proba(extracted_sentences)
        max_proba_for_true_of_proba_list: list = max(list_of_proba_false_and_true, key=itemgetter(1))
        # Select the sentence for which the key entity prediction was the highest
        mask_of_maximum = [True if all(i == max_proba_for_true_of_proba_list)
                           else False
                           for i in list_of_proba_false_and_true]
        entity_spans = annotated.tiers['dates'].spans
        relevant_spans = list(compress(entity_spans, mask_of_maximum))
        if entity == "dates":
            key_entities = [span.datetime_range for span in relevant_spans]
        elif entity == "counts":
            key_entities = [span.metadata['count'] for span in relevant_spans]
        else:
            raise KeyError
    except (KeyError, ValueError) as e:
        print(e)
        key_entities = None
    return key_entities
