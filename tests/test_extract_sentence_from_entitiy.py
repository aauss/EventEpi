import pytest
import os
import pickle
import sys

from nlp_surveillance.classifier.extract_sentence import extract_entities_with_sentence
from nlp_surveillance.my_utils import delete_non_epitator_name_entity_tiers

sys.setrecursionlimit(5000)  # Otherwise serialization does not work


@pytest.fixture
def annotated_example():
    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, '..', 'data', 'fixtures', 'annotated_example.pkl')
    if not os.path.isfile(path):
        from epitator.annotator import AnnoDoc
        from epitator.count_annotator import CountAnnotator

        annotated = AnnoDoc('I am in Berlin. Here are 5 confirmed cases of influenza. '
                            'Still, less worse than those 100 confirmed and 200 suspected cases last year.')
        annotated.add_tiers(CountAnnotator())
        annotated = delete_non_epitator_name_entity_tiers(annotated)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as handel:
            pickle.dump(annotated, handel)
    else:
        with open(path, 'rb') as handel:
            annotated = pickle.load(handel)
    return annotated


def test_sentence_extraction(annotated_example):
    # For each entity found, I expect the sentence it occurs to be returned
    sentences_expected = ['Here are 5 confirmed cases of influenza.',
                          'Still, less worse than those 100 confirmed and 200 suspected cases last year.',
                          'Still, less worse than those 100 confirmed and 200 suspected cases last year.']
    _, sentences_example = extract_entities_with_sentence(annotated_example, 'counts')
    assert sentences_example == sentences_expected
