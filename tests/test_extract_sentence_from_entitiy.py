import pytest
import os
import pickle
import sys

from nlp_surveillance.classifier.extract_sentence import extract_sentence_from_found_entities
from utils.my_utils import delete_non_epitator_name_entity_tiers

sys.setrecursionlimit(5000)  # Otherwise serialization does not work


@ pytest.fixture
def annotated_example():
    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, '..', 'data', 'fixtures', 'annotated_example.pkl')
    if not os.path.isfile(path):
        from epitator.annotator import AnnoDoc
        from epitator.geoname_annotator import GeonameAnnotator

        annotated = AnnoDoc('I am in Berlin. Something is odd in Berlin.'
                            ' Still, I seem to like Berlin more than Frankfurt now.'
                            ' I am sad that I am not in Switzerland.')
        annotated.add_tiers(GeonameAnnotator())
        annotated = delete_non_epitator_name_entity_tiers(annotated)
        print(annotated.tiers)
        with open(path, 'wb') as handel:
            pickle.dump(annotated, handel)
    else:
        with open(path, 'rb') as handel:
            annotated = pickle.load(handel)
    return annotated


def test_sentence_extraction(annotated_example):
    # For each entity found, I expect the sentence it occurs to be returned
    sentences_expected = ['I am in Berlin.',
                          'Something is odd in Berlin.',
                          'Still, I seem to like Berlin more than Frankfurt now.',
                          'Still, I seem to like Berlin more than Frankfurt now.',
                          'I am sad that I am not in Switzerland.']
    sentences_example = extract_sentence_from_found_entities(annotated_example, 'geonames')
    assert sentences_example == sentences_expected
