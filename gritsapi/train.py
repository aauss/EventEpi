# load the training/validation resources and ontology data from AWS
from boto.s3.connection import S3Connection, Location
import datetime
import os
import pickle
import config
import diagnosis
from diagnosis.KeywordExtractor import *
from diagnosis.Diagnoser import Diagnoser
import numpy as np
import re
import sklearn
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction import DictVectorizer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.linear_model import LogisticRegression
from diagnosis.utils import group_by, flatten
import warnings
import test_classifier
from DataSet import fetch_datasets
import ontology_file_helpers


def get_pickle(filename):
    """
    Download the pickle from the AWS bucket if it's stale,
    save it in the workspace, and return the loaded data.
    """
    try:
        conn = S3Connection(config.aws_access_key, config.aws_secret_key)
        bucket = conn.get_bucket('classifier-data')
        k = bucket.get_key(filename)
        from dateutil import parser
        from tzlocal import get_localzone
        tz = get_localzone()
        if os.path.exists(filename):
            local_copy_time = datetime.datetime.fromtimestamp(
                os.path.getctime(filename)
            )
        else:
            # This datetime that should always be before the remote copy timestamp.
            # However, if it is too close to datetime min it can't be
            # localized in some timezones.
            local_copy_time = datetime.datetime(1,2,3)
        remote_copy_time = parser.parse(k.last_modified)
        if tz.localize(local_copy_time) < remote_copy_time:
            print "Downloading", filename
            k.get_contents_to_filename(filename)
    except:
        print "Could not download fresh pickle: " + filename
    with open(filename) as f:
        result = pickle.load(f)
        print filename, "loaded"
        return result

def train(debug, pickle_dir):
    latestPickle = list(ontology_file_helpers.get_ontology_files())[-1].name
    keywords = get_pickle(latestPickle)
    
    categories = set([
        'hm/disease',
        'biocaster/pathogens',
        'biocaster/diseases',
        'biocaster/symptoms',
        'symp/symptoms',
        'eha/symptom',
        'eha/mode of transmission',
        'eha/environmental factors',
        'eha/vector',
        'eha/occupation',
        'eha/control measures',
        'eha/description of infected',
        'eha/disease category',
        'eha/host',
        'eha/host use',
        'eha/symptom',
        'eha/disease',
        'eha/location', 
        'eha/transmission',
        'eha/zoonotic type',
        'eha/risk',
        'wordnet/season',
        'wordnet/climate',
        'wordnet/pathogens',
        'wordnet/hosts',
        'wordnet/mod/severe',
        'wordnet/mod/painful',
        'wordnet/mod/large',
        'wordnet/mod/rare',
        'doid/has_symptom',
        'doid/symptoms',
        'doid/transmitted_by',
        'doid/located_in',
        'doid/diseases',
        'doid/results_in',
        'doid/has_material_basis_in',
        'usgs/terrain'
    ])

    keyword_array = [
        keyword_obj for keyword_obj in keywords
        if keyword_obj['category'] in categories
    ]
    
    unused_keyword_cats = set([
        keyword_obj['category'] for keyword_obj in keywords
        if keyword_obj['category'] not in categories
    ])
    
    if len(unused_keyword_cats) > 0:
        print "Unused keyword categories:"
        print unused_keyword_cats
    
    # Keyword Extraction
    feature_extractor = Pipeline([
        ('kwext', KeywordExtractor(keyword_array)),
        ('link', LinkedKeywordAdder(keyword_array)),
        ('limit', LimitCounts(1)),
    ])
    
    time_offset_test_set, mixed_test_set, training_set = fetch_datasets()
    
    time_offset_test_set.feature_extractor =\
    mixed_test_set.feature_extractor =\
    training_set.feature_extractor = feature_extractor
    
    #If we get sparse rows working with the classifier this might yeild some
    #performance improvments.
    my_dict_vectorizer = DictVectorizer(sparse=False).fit(training_set.get_feature_dicts())
    print 'Found keywords:', len(my_dict_vectorizer.vocabulary_)
    print "Keywords in the validation set that aren't in the training set:"
    print  (
        set(DictVectorizer(sparse=False).fit(
            mixed_test_set.get_feature_dicts()).vocabulary_
        ) -
        set(my_dict_vectorizer.vocabulary_)
    )
    
    time_offset_test_set.dict_vectorizer = \
    mixed_test_set.dict_vectorizer = \
    training_set.dict_vectorizer = my_dict_vectorizer

    time_offset_test_set.remove_zero_feature_vectors()
    mixed_test_set.remove_zero_feature_vectors()
    training_set.remove_zero_feature_vectors()

    my_classifier = OneVsRestClassifier(LogisticRegression(
        # When fit intercept is False the classifier predicts nothing when
        # all the features are zero.
        # On one hand, we can guess the article is most likely to be dengue or
        # another common disease and still occassionally be right,
        # and having a intercept offset could allow us
        # to create a model that is a tighter fit.
        # On one hand, predictions based off of nothing might puzzle users.
        # fit_intercept=False,
        # l1 penalty will produce sparser coefficients.
        # it seems to perform worse,
        # but the classifications will be easier to inspect,
        # and we might be able to avoid some overfitting based on weak
        # correlations that turn out to be false.
        # penalty='l1',
        # Using class weighting we might be able to avoid overpredicting
        # the more common labels.
        # However, auto weighting has only hurt our mico f-scores so far.
        # class_weight='auto',
    ), n_jobs=-1)
    
    # Parent labels are not added to the training data because we seem to do
    # better by adding them after the classification.
    # This may be due to the parent classification having such a
    # high confidence that the child labels are pushed below the cutoff.
    my_classifier.fit(
        np.array(training_set.get_feature_vectors()),
        np.array(training_set.get_labels()))
    
    # Pickle everything that will be needed for classification:
    with open(os.path.join(pickle_dir, 'classifier.p'), 'wb') as f:
        pickle.dump(my_classifier, f)
    with open(os.path.join(pickle_dir, 'dict_vectorizer.p'), 'wb') as f:
        pickle.dump(my_dict_vectorizer, f)
    with open(os.path.join(pickle_dir, 'keyword_array.p'), 'wb') as f:
        pickle.dump(keyword_array, f)
    
    test_classifier.run_tests(pickle_dir=pickle_dir)

if __name__ == '__main__':
    # This will run the classifier and save the output:
    # mkdir classifier_conf
    # unbuffer python train.py -pickle_dir classifier_conf | tee classifier_conf/result.txt
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-debug', action='store_true')
    parser.add_argument('-pickle_dir', default='')
    args = parser.parse_args()
    train(args.debug, args.pickle_dir)
