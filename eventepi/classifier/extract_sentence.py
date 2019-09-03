from nltk.tokenize import PunktSentenceTokenizer
from itertools import product
from collections import namedtuple
from epitator.annotator import AnnoDoc
from epitator.count_annotator import CountAnnotator
from epitator.date_annotator import DateAnnotator
from typing import Tuple, Dict, Union, KeysView, List, Any

from eventepi.classifier.create_labels import create_labels


def from_entity(text: str, to_optimize: str, event_db_entry: Any) -> List[Tuple[bool, str]]:
    """Creates labeled sentences with EpiTator entities == to_optimize based on text from incident database entry

    Function takes an extracted text from one row of the incident database. Additionally it takes the
    entity==to_optimize of the corresponding incident database entry. It extracts all entities==to_optimize
    from the text using EpiTator. Then two lists are created, one for the found entities and the other for sentences
    those entities were found in. The incident database entity together with the found entities are used to label the
    found sentences. If the entity of the incident database equals the entity extracted from text
    using EpiTator, then the sentences is labeled as True, otherwise False.

    Args:
        text: Epidemiological article found in the corresponding incident database entry
        to_optimize: a string that specifies the entity to search for in sentences that we want to label
        event_db_entry: An row of a pd.DataFrame as an namedtuple ('Pandas')

    Returns:
        A list of tuples where the first entry is a bool specifying whether the entity is in the incident database
        entry and the corresponding sentences that contains this entity
    """
    annotated = _annotate(text, to_optimize)
    entities, sentences = extract_entities_with_sentence(annotated, to_optimize)
    labels = create_labels(entities, to_optimize, event_db_entry)
    label_sentence_tuple = namedtuple('label_sentence_tuple', ['label', 'sentence'])
    label_sentence_tuples = [label_sentence_tuple(*tuple_) for tuple_ in zip(labels, sentences)]
    return label_sentence_tuples


def _annotate(text: str, to_optimize: str) -> AnnoDoc:
    tier = {'counts': CountAnnotator(), 'dates': DateAnnotator()}
    annotated = AnnoDoc(text)
    annotated.add_tiers(tier[to_optimize])
    return annotated


def extract_entities_with_sentence(annotated: AnnoDoc, to_optimize: str) -> Tuple[list, List[str]]:
    sentence_spans = PunktSentenceTokenizer().span_tokenize(annotated.text)
    span_entity_dict = _create_span_entity_dict(annotated, to_optimize)
    matched_entity_sentence_spans = _match_entity_and_sentence_spans(span_entity_dict.keys(), sentence_spans)
    entities = [span_entity_dict[tuple_.entity_span] for tuple_ in matched_entity_sentence_spans]
    sentences = [annotated.text[slice(*tuple_.sentence_span)]
                 for tuple_ in matched_entity_sentence_spans]
    return entities, sentences


def _create_span_entity_dict(annotated: AnnoDoc, to_optimize: str) -> Dict[tuple, Union[list, int]]:
    spans = annotated.tiers[to_optimize].spans
    to_metadata_attr = {'counts': 'count', 'dates': 'datetime_range'}
    attribute = to_metadata_attr[to_optimize]
    span_entity_dict = {(span.start, span.end): span.metadata[attribute] for span in spans}
    return span_entity_dict


def _match_entity_and_sentence_spans(entity_spans: KeysView[tuple], sentence_spans: list) -> List[namedtuple]:
    cartesian_product = product(entity_spans, sentence_spans)
    list_of_found_entity_sentence_spans = list(filter(_overlap, cartesian_product))
    entity_sentence_tuple = namedtuple('entity_sentence', ['entity_span', 'sentence_span'])
    list_of_found_entity_sentence_spans_named = [entity_sentence_tuple(*tuple_) for tuple_ in
                                                 list_of_found_entity_sentence_spans]
    return list_of_found_entity_sentence_spans_named


def _overlap(tuple_of_tuples: tuple) -> bool:
    entity_span, sent_span = tuple_of_tuples
    if sent_span[0] <= entity_span[0] and entity_span[1] <= sent_span[1]:
        return True
