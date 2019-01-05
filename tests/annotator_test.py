import os
import pickle
import pytest
from nlp_surveillance.annotator import *
from nlp_surveillance.utils.my_utils import matching_elements
from nlp_surveillance.EntityTuple import Entity


@pytest.fixture
def get_example_who_don_annotated_without_tiers():
    example_who_don = get_example_who_don()
    path_annotated = os.path.join("..", "nlp_surveillance", "pickles", "example_who_don_annotated.p")
    example_exists = os.path.exists(path_annotated)
    if not example_exists:
        example_who_don_annotated = annotate(example_who_don)
        example_who_don_annotated_without_tiers = example_who_don_annotated.delete_non_epitator_name_entity_tiers()
        pickle.dump(example_who_don_annotated_without_tiers, open(path_annotated, "wb"))
    else:
        example_who_don_annotated_without_tiers = pickle.load(open(path_annotated, "rb"))
        if not example_who_don_annotated_without_tiers:
            print("...retrieve failed. Pickle might be corrupted. Delete example_who_don.p and try again.")
    return example_who_don_annotated_without_tiers


def test_database_creation():
    example_who_don = get_example_who_don()
    database = create_annotated_database(example_who_don, [geonames, (keywords, {"raw": False, "with_label": True})])
    del database["texts"]  # Delete text since it is not affected by the function call and is to cumbersome to assert
    assert database == {'dates': [], 'cases': [], 'keywords': [
        [{'id': 'http://purl.obolibrary.org/obo/DOID_4325', 'label': 'Ebola hemorrhagic fever', 'type': 'disease'}]],
                        'geonames': [['Republic of Uganda', 'Republic of the Congo', 'South Sudan']]}


def test_geonames(get_example_who_don_annotated_without_tiers):
    assert geonames(get_example_who_don_annotated_without_tiers) == Entity(entity='geonames',
                                                                           resolved=
                                                                           ['Republic of Uganda',
                                                                            'South Sudan'
                                                                           ]), "geonames failed"


def test_keywords(get_example_who_don_annotated_without_tiers):
    assert keywords(get_example_who_don_annotated_without_tiers) == Entity(entity='keywords',
                                                                           resolved=['Ebola hemorrhagic fever']),\
        "keywords failed"

    assert keywords(get_example_who_don_annotated_without_tiers,
                    with_label=True) == Entity(entity='keywords', resolved=[{
                                                    'id': 'http://purl.obolibrary.org/obo/DOID_4325',
                                                    'label': 'Ebola hemorrhagic fever', 'type': 'disease'}])


def test_cases(get_example_who_don_annotated_without_tiers):
    case_numbers = cases(get_example_who_don_annotated_without_tiers, raw=True)
    expected_case_numbers = Entity(entity='cases', resolved=[2, 1, 31, 4, 3, 3, 3, 31, 12, 9, 2, 103, 14, 11, 341, 303,
                                                             38, 215, 177, 38, 11, 3, 1, 2])
    assert isinstance(case_numbers, Entity), "Cases has wrong entity"
    assert case_numbers[0] == "cases", "Wrong entity"
    matches = matching_elements(case_numbers[1], expected_case_numbers[1])
    # Assert that majority is correct, since analysis is not deterministic and the test otherwise fails sometimes
    assert len(matches) > int(len(expected_case_numbers[1]) * 0.8)


def test_dates(get_example_who_don_annotated_without_tiers):
    assert dates(get_example_who_don_annotated_without_tiers) == Entity(entity='dates',
                                                                        resolved=['2018-11-07']), "dates failed"

def test_removal_of_non_epitator_name_entity_tiers():
    before = ['spacy.sentences', 'spacy.noun_chunks', 'spacy.tokens', 'spacy.nes', 'tokens', 'ngrams', 'nes',
              'geonames', 'resolved_keywords', 'structured_data', 'structured_data.values', 'dates', 'raw_numbers',
              'counts']
    after = ['geonames', 'resolved_keywords', 'structured_data', 'structured_data.values', 'dates', 'raw_numbers',
             'counts']
    test_annotate = annotate('I love Frankfurt')
    assert before == list(test_annotate.tiers.keys()), 'Did not annotate correctly'
    test_annotate.delete_non_epitator_name_entity_tiers()
    assert after == list(test_annotate.tiers.keys()), 'Did not delete non epitat name entities tiers correctly'


def get_example_who_don():
    path_example_who_don = os.path.join('..', 'nlp_surveillance', 'data', 'example_who_don')
    with open(path_example_who_don, 'r') as handler:
        example_who_don = handler.read()
    return example_who_don
