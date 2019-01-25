from nltk import sent_tokenize
from itertools import product, starmap, compress, tee


def from_entity(event_db, to_optimize):
    event_db = _drop_unnecessary_columns_and_nans(event_db, to_optimize)

    event_db.annotated = event_db.annotated.apply(lambda x: _extract_sentence_from_found_entities(x, to_optimize))
    return event_db


def _drop_unnecessary_columns_and_nans(event_db, to_optimize):
    optimize_to_column_dict = {'dates': 'date_of_data', 'counts': 'count_edb'}
    column_to_optimize = optimize_to_column_dict[to_optimize]

    event_db = event_db[[column_to_optimize, 'annotated']]
    event_db = event_db.dropna()
    return event_db


def _extract_sentence_from_found_entities(annotated, to_optimize):
    sentences = sent_tokenize(annotated.text)

    calculate_sentence_span = _generate_span_calculator()
    sentence_spans = (calculate_sentence_span(sent) for sent in sentences)
    entity_spans_as_objects = annotated.tiers[to_optimize].spans
    entity_spans = (range(span.start, span.end) for span in entity_spans_as_objects)

    two_identical_cartesian_product_of_spans = tee(product(entity_spans, sentence_spans))
    cartesian_1, cartesian_2 = two_identical_cartesian_product_of_spans
    matched_entity_with_sentences = starmap(_span_subset_of, cartesian_1)
    matches = compress(cartesian_2, matched_entity_with_sentences)
    matched_sent_spans = [s for _, s in matches]
    matched_sentences = [annotated.text[min(r):max(r)] for r in matched_sent_spans]
    return matched_sentences


def _generate_span_calculator():
    start = 0

    def add_length_of_string(sentence):
        nonlocal start
        start_sent = start
        end_of_sent = start + len(sentence) + 1
        start = end_of_sent
        return range(start_sent, end_of_sent)

    return add_length_of_string


def _span_subset_of(a_span, another_span):
    return set(a_span).issubset(another_span)
