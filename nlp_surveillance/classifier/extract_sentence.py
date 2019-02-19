from nltk import sent_tokenize
import numpy as np
from collections import namedtuple
from epitator.annotator import AnnoDoc
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator
from nlp_surveillance.classifier.create_labels import create_labels


def from_entity(text, to_optimize, event_db_entry):
    sentences = sent_tokenize(text)
    sentence_label_list = []
    for sentence in sentences:
        annotated = _annotate(sentence, to_optimize)
        entities = _extract_entities_from_sentence(annotated, to_optimize)
        labels = create_labels(entities, event_db_entry, to_optimize)
        labeled_as_list = _wrap_into_sent_label_tuple(sentence, labels)
        sentence_label_list.extend(labeled_as_list)
    return sentence_label_list


def _annotate(sentence, to_optimize):
    tier = {'counts': CountAnnotator(), 'dates': DateAnnotator()}
    annotated = AnnoDoc(sentence)
    annotated.add_tiers(tier[to_optimize])
    return annotated


def _extract_entities_from_sentence(annotated, to_optimize):
    spans = annotated.tiers[to_optimize].spans
    if to_optimize == 'dates':
        entities = [span.metadata['datetime_range'] for span in spans]
    elif to_optimize == 'counts':
        entities = [span.metadata['count'] for span in spans]
    else:
        raise NotImplementedError
    return entities


def _wrap_into_sent_label_tuple(sentence, labels):
    Labeled = namedtuple('labeled_sentence', ['sentence_repeated', 'label'])
    sentence_repeated = np.repeat(sentence, len(labels))
    labeled_as_list = [Labeled(sent, label) for sent, label in zip(sentence_repeated, labels)]
    return labeled_as_list



# from nltk import sent_tokenize
# import numpy as np
# from itertools import product
# from epitator.annotator import AnnoDoc
# from epitator.count_annotator import CountAnnotator
# from epitator.date_annotator import DateAnnotator
#
#
# def from_entity(text, to_optimize, event_db_entry):
#     tier = {'counts': CountAnnotator(), 'dates': DateAnnotator()}
#     annotated = AnnoDoc(text).add_tiers(tier[to_optimize])
#     extracted_sentences = extract_sentence_from_found_entities(annotated, to_optimize=to_optimize)
#     extracted_entity = _extract_entities_from_sentence(annotated, to_optimize=to_optimize)
#     repeated_event_db_entry = np.repeat(event_db_entry, len(extracted_entity))
#     try:
#         assert len(extracted_sentences) == len(extracted_entity) == len(repeated_event_db_entry)
#     except AssertionError:
#         print('daym')
#         pass
#     return extracted_sentences, extracted_entity, repeated_event_db_entry
#
#
# def extract_sentence_from_found_entities(annotated, to_optimize):
#     sentence_span_to_sentence_dict = _sentence_span_to_sentence_dict(annotated)
#
#     entity_spans_as_objects = annotated.tiers[to_optimize].spans
#     entity_spans = (range(span.start, span.end) for span in entity_spans_as_objects)
#
#     matched_sentences = _return_matching_sentences(entity_spans, sentence_span_to_sentence_dict)
#     return list(matched_sentences)
#
#
# def _sentence_span_to_sentence_dict(annotated):
#     sentences = sent_tokenize(annotated.text)
#
#     calculate_sentence_span = _generate_span_calculator()
#     sentence_spans = (calculate_sentence_span(sent) for sent in sentences)
#     return dict(zip(sentence_spans, sentences))
#
#
# def _extract_entities_from_sentence(annotated, to_optimize):
#     spans = annotated.tiers[to_optimize].spans
#     if to_optimize == 'dates':
#         entities = [span.metadata['datetime_range'] for span in spans]
#     elif to_optimize == 'counts':
#         entities = [span.metadata['count'] for span in spans]
#     else:
#         raise NotImplementedError
#     return entities
#
#
# def _generate_span_calculator():
#     start = 0
#
#     def add_length_of_string(sentence):
#         nonlocal start
#         start_sent = start
#         end_of_sent = start + len(sentence) + 1
#         start = end_of_sent
#         return range(start_sent, end_of_sent)
#
#     return add_length_of_string
#
#
# def _return_matching_sentences(entity_spans, sentence_span_to_sentence_dict):
#     for entity_span, sentence_span in product(entity_spans, sentence_span_to_sentence_dict.keys()):
#         if _span_subset_of(entity_span, sentence_span):
#             yield sentence_span_to_sentence_dict[sentence_span]
#
#
# def _span_subset_of(a_span, another_span):
#     return set(a_span).issubset(another_span)
