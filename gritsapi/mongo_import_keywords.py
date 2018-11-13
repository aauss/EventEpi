#!/usr/bin/env python
"""
Load mongo with autocomplete data for the keywords for annie annotation and
potentially autocompletion.
"""
import sys, csv
import re
import pickle
import config
from pymongo import MongoClient


def load_keyword_array(file_path):
    with open(file_path) as f:
        keyword_array = pickle.load(f)
    return keyword_array

def insert_set(names_set, collection):
    """Insert a list of names into a collection"""

    for name in names_set:
        collection.insert({'_id': name})


if __name__ == '__main__':

    client = MongoClient(config.mongo_url)
    db = client.annotation

    category_labels = {
        'doid/diseases': 'diseases',
        'eha/disease': 'diseases',
        'pm/disease':  'diseases',
        'hm/disease': 'diseases',
        'biocaster/diseases': 'diseases',
        'eha/symptom': 'symptoms',
        'biocaster/symptoms': 'symptoms',
        'doid/has_symptom': 'symptoms',
        'pm/symptom': 'symptoms',
        'symp/symptoms': 'symptoms',
        'wordnet/hosts': 'hosts',
        'eha/vector': 'hosts',
        'wordnet/pathogens': 'pathogens',
        'biocaster/pathogens': 'pathogens',
        'pm/mode of transmission': 'modes',
        'doid/transmitted_by': 'modes',
        'eha/mode of transmission': 'modes'
    }

    collection_labels = set(category_labels.values())
    for collection in collection_labels:
        db[collection].drop()

    keyword_array = load_keyword_array('current_classifier/keyword_array.p')

    for keyword in keyword_array:
        if keyword['category'] in category_labels:
            collection = category_labels[keyword['category']]

            db[collection].insert(
                { '_id': keyword['keyword'],
                  'source': keyword['category'],
                  'linked_keywords': keyword['linked_keywords'],
                  'case_sensitive': keyword['case_sensitive']} )
