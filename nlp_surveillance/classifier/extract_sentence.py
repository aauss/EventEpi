from nltk.tokenize import PunktSentenceTokenizer
from itertools import product
from collections import namedtuple
from epitator.annotator import AnnoDoc
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator
from nlp_surveillance.classifier.create_labels import create_labels


def from_entity(text, to_optimize, event_db_entry):
    annotated = _annotate(text, to_optimize)
    entities, sentences = extract_entities_with_sentence(annotated, to_optimize)
    labels = create_labels(entities, to_optimize, event_db_entry)
    label_sentence_tuple = namedtuple('label_sentence_tuple', ['label', 'sentence'])
    label_sentence_tuples = [label_sentence_tuple(*tuple_) for tuple_ in zip(labels, sentences)]
    return label_sentence_tuples


def _annotate(sentence, to_optimize):
    tier = {'counts': CountAnnotator(), 'dates': DateAnnotator()}
    annotated = AnnoDoc(sentence)
    annotated.add_tiers(tier[to_optimize])
    return annotated


def extract_entities_with_sentence(annotated, to_optimize):
    sentence_spans = PunktSentenceTokenizer().span_tokenize(annotated.text)
    span_entity_dict = _create_span_entity_dict(annotated, to_optimize)
    matched_entity_sentence_spans = _match_entity_and_sentence_spans(span_entity_dict.keys(), sentence_spans)
    entities = [span_entity_dict[tuple_.entity_span] for tuple_ in matched_entity_sentence_spans]
    sentences = [annotated.text[slice(*tuple_.sentence_span)]
                 for tuple_ in matched_entity_sentence_spans]
    return entities, sentences


def _match_entity_and_sentence_spans(entity_spans, sentence_spans):
    cartesian_product = product(entity_spans, sentence_spans)
    entity_sentence_tuple = namedtuple('entity_sentence', ['entity_span', 'sentence_span'])
    list_of_found_entity_sentence_spans = list(filter(_overlap, cartesian_product))
    list_of_found_entity_sentence_spans_named = [entity_sentence_tuple(*tuple_) for tuple_ in
                                                 list_of_found_entity_sentence_spans]
    return list_of_found_entity_sentence_spans_named


def _create_span_entity_dict(annotated, to_optimize):
    spans = annotated.tiers[to_optimize].spans
    to_metadata_attr = {'counts': 'count', 'dates': 'datetime_range'}
    attribute = to_metadata_attr[to_optimize]
    span_entity_dict = {(span.start, span.end): span.metadata[attribute] for span in spans}
    return span_entity_dict


def _overlap(tuple_of_tuples):
    entity_span, sent_span = tuple_of_tuples
    if sent_span[0] <= entity_span[0] and entity_span[1] <= sent_span[1]:
        return True
