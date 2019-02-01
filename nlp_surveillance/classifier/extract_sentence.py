import warnings
import pandas as pd
from nltk import sent_tokenize
from itertools import product
from functools import partial

from utils.my_utils import split_list_and_distribute_to_new_rows


def from_entity(event_db, to_optimize):
    event_db = _drop_unnecessary_columns_and_nans(event_db, to_optimize)

    extract_sentences = partial(extract_sentence_from_found_entities, to_optimize=to_optimize)
    event_db['sentences'] = event_db.annotated.apply(extract_sentences)

    extract_entity = partial(_extract_entities_from_sentence, to_optimize=to_optimize)
    event_db[to_optimize] = event_db.annotated.apply(extract_entity)

    num_of_sent_and_entities_match = (event_db[['sentences', to_optimize]]
                                      .apply(lambda x: len(x[0]) == len(x[1]), axis=1))
    if not all(num_of_sent_and_entities_match):
        warnings.warn('There are unequal amounts of text and entities extracted')
        event_db = event_db[num_of_sent_and_entities_match]

    event_db['list_of_sentence_entity_tuples'] = event_db[['sentences', to_optimize]].apply(lambda x: list(zip(*x)),
                                                                                            axis=1)
    event_db = split_list_and_distribute_to_new_rows(event_db, 'list_of_sentence_entity_tuples')
    event_db[['sentence', to_optimize]] = event_db.list_of_sentence_entity_tuples.apply(pd.Series)
    return event_db.drop(columns=['annotated', 'sentences', 'list_of_sentence_entity_tuples'])


def _drop_unnecessary_columns_and_nans(event_db, to_optimize):
    optimize_to_column_dict = {'dates': 'date_of_data', 'counts': 'count_edb'}
    column_to_optimize = optimize_to_column_dict[to_optimize]

    event_db = event_db[[column_to_optimize, 'annotated']]
    event_db = event_db.dropna()

    text_long_enough = event_db.annotated.apply(lambda x: len(x.text) > 200)
    return event_db[text_long_enough]


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


def _zip_sentence_and_entities(to_optimze):
    pass


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
