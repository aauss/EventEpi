from nltk import sent_tokenize
from itertools import product
from epitator.annotator import AnnoDoc
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator


def from_entity(text, to_optimize):
    tier = {'counts': CountAnnotator(), 'dates': DateAnnotator()}
    annotated = AnnoDoc(text).add_tiers(tier[to_optimize])
    extract_sentences = extract_sentence_from_found_entities(annotated, to_optimize=to_optimize)
    extract_entity = _extract_entities_from_sentence(annotated, to_optimize=to_optimize)
    return list(zip(extract_sentences, extract_entity))


def extract_sentence_from_found_entities(annotated, to_optimize):
    sentence_span_to_sentence_dict = _sentence_span_to_sentence_dict(annotated)

    entity_spans_as_objects = annotated.tiers[to_optimize].spans
    entity_spans = (range(span.start, span.end) for span in entity_spans_as_objects)

    matched_sentences = _return_matching_sentences(entity_spans, sentence_span_to_sentence_dict)
    return list(matched_sentences)


def _sentence_span_to_sentence_dict(annotated):
    sentences = sent_tokenize(annotated.text)

    calculate_sentence_span = _generate_span_calculator()
    sentence_spans = (calculate_sentence_span(sent) for sent in sentences)
    return dict(zip(sentence_spans, sentences))


def _extract_entities_from_sentence(annotated, to_optimize):
    spans = annotated.tiers[to_optimize].spans
    if to_optimize == 'dates':
        entities = [span.metadata['datetime_range'] for span in spans]
    elif to_optimize == 'counts':
        entities = [span.metadata['count'] for span in spans]
    else:
        raise NotImplementedError
    return entities


def _generate_span_calculator():
    start = 0

    def add_length_of_string(sentence):
        nonlocal start
        start_sent = start
        end_of_sent = start + len(sentence) + 1
        start = end_of_sent
        return range(start_sent, end_of_sent)

    return add_length_of_string


def _return_matching_sentences(entity_spans, sentence_span_to_sentence_dict):
    for entity_span, sentence_span in product(entity_spans, sentence_span_to_sentence_dict.keys()):
        if _span_subset_of(entity_span, sentence_span):
            yield sentence_span_to_sentence_dict[sentence_span]


def _span_subset_of(a_span, another_span):
    return set(a_span).issubset(another_span)
