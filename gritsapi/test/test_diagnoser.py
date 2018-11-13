# coding=utf8
import os, sys; sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import unittest
import pickle
from diagnosis.Diagnoser import Diagnoser
import datetime, bson
def make_json_compat(obj):
    """
    Coerce the types in an object to values that can be jsonified.
    """
    base_types = [str, unicode, basestring, bool, int, long, float, type(None)]
    if type(obj) in base_types:
        return obj
    elif isinstance(obj, list):
        return map(make_json_compat, obj)
    elif isinstance(obj, dict):
        return { k : make_json_compat(v) for k,v in obj.items() }
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, bson.ObjectId):
        return str(obj)
    else:
        raise TypeError(type(obj))

class TestDiagnoser(unittest.TestCase):
    def setUp(self):
        with open('../classifier.p') as f:
            my_classifier = pickle.load(f)
        with open('../dict_vectorizer.p') as f:
            my_dict_vectorizer = pickle.load(f)
        with open('../keyword_array.p') as f:
            keyword_array = pickle.load(f)
        self.my_diagnoser = Diagnoser(
            my_classifier,
            my_dict_vectorizer,
            keyword_array=keyword_array,
            cutoff_ratio=.7
        )
    
    def test_problem_keywords(self):
        diagnosis = self.my_diagnoser.diagnose(
            "meningococcal septicaemia and respiratory illness"
        )
        keywords = set([k['name'] for k in diagnosis['keywords_found']])
        self.assertSetEqual(
            set([
                'meningococcal septicaemia',
                'respiratory illness']) - keywords,
            set())
    def test_duplicate_parents(self):
        diagnosis = self.my_diagnoser.diagnose(
            "Hepatitis B, Hepatitis C, and Hepatitis D and Hepatitis E"
        )
        diseases = [d['name'] for d in diagnosis['diseases']]
        self.assertEqual(len(diseases), len(set(diseases)))
    
    def test_duplicate_kw_categories(self):
        diagnosis = self.my_diagnoser.diagnose("influenza")
        category_sets = [kw['categories'] for kw in diagnosis['keywords_found']]
        for s in category_sets:
            self.assertEqual(len(s), len(set(s)))
    
    def test_url_keywords(self):
        diagnosis = self.my_diagnoser.diagnose(
        "A map showing the Akmola region UN-B can be found at: http://un-dx.ucoz.com/KZ-MAP.gif"
        )
        assert 'MAP' not in set([kw['name'] for kw in diagnosis['keywords_found']])

    def test_timing_and_serialization(self):
        from datetime import datetime, timedelta
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger('diagnosis.Diagnoser')
        logger.setLevel(logging.INFO)
        start = datetime.utcnow()
        import codecs
        with codecs.open('test_article.txt', encoding='utf-8') as f:
            diagnosis = self.my_diagnoser.diagnose(f.read())
            make_json_compat(diagnosis)
            self.assertLess(
                (datetime.utcnow() - start),
                timedelta(seconds=30)
            )

    def test_inference(self):
        import disease_label_table
        self.assertSetEqual(set(['Avian Influenza', 'Influenza']),
            set(disease_label_table.get_inferred_labels('Avian Influenza H7N9')))
