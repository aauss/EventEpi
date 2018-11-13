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
from DataSet import fetch_datasets

def run_tests(pickle_dir="classifier_conf"):
    with open(os.path.join(pickle_dir, 'classifier.p')) as f:
        my_classifier = pickle.load(f)
    with open(os.path.join(pickle_dir, 'dict_vectorizer.p')) as f:
        my_dict_vectorizer = pickle.load(f)
    with open(os.path.join(pickle_dir, 'keyword_array.p')) as f:
        keyword_array = pickle.load(f)
    
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
    
    time_offset_test_set.dict_vectorizer = \
    mixed_test_set.dict_vectorizer = \
    training_set.dict_vectorizer = my_dict_vectorizer

    time_offset_test_set.remove_zero_feature_vectors()
    mixed_test_set.remove_zero_feature_vectors()
    training_set.remove_zero_feature_vectors()
    
    my_diagnoser = Diagnoser(
        my_classifier,
        my_dict_vectorizer,
        keyword_array=keyword_array,
        cutoff_ratio=.7
    )
    with warnings.catch_warnings():
        # The updated version of scikit will spam warnings here.
        warnings.simplefilter("ignore")
        train_label_set = set(flatten(training_set.get_labels(), 1))
        for data_set, ds_label, print_label_breakdown in [
            (training_set, "Training set", False),
            (time_offset_test_set, "Time offset set", True),
            (mixed_test_set, "Mixed test set", False),
        ]:
            if len(data_set) == 0: continue
            print "Data set:", ds_label
            validation_label_set = set(flatten(data_set.get_labels(), 1))
            not_in_train = [
                label for label in validation_label_set
                if (label not in train_label_set)
            ]
            print "Labels we have no training data for:", len(not_in_train),'/', len(validation_label_set)
            print set(not_in_train)
            predictions = [
                tuple([
                    my_diagnoser.classifier.classes_[i]
                    for i, p in my_diagnoser.best_guess(X)
                ])
                for X in data_set.get_feature_vectors()
            ]
            # I've noticed that the macro f-score is not the harmonic mean of 
            # the percision and recall. Perhaps this could be a result of the 
            # macro f-score being computed as an average of f-scores.
            # Furthermore, the macro f-scrore can be smaller than the precision 
            # and recall which seems like it shouldn't be possible.
            print ("Validation set (macro avg):\n"
                "precision: %s recall: %s f-score: %s") %\
                sklearn.metrics.precision_recall_fscore_support(
                    data_set.get_labels(add_parents=True),
                    predictions,
                    average='macro')[0:3]
            print ("Validation set (micro avg):\n"
                "precision: %s recall: %s f-score: %s") %\
                sklearn.metrics.precision_recall_fscore_support(
                    data_set.get_labels(add_parents=True),
                    predictions,
                    average='micro')[0:3]
            
            if print_label_breakdown:
                print "Which classes are we performing poorly on?"
                labels = sorted(
                    list(set(flatten(data_set.get_labels(add_parents=True))) |\
                    set(flatten(predictions)))
                )
                prfs = sklearn.metrics.precision_recall_fscore_support(
                    data_set.get_labels(add_parents=True),
                    predictions,
                    labels=labels
                )
                for cl,p,r,f,s in sorted(zip(labels, *prfs), key=lambda k:k[3]):
                    print cl
                    print "precision:",p,"recall",r,"F-score:",f,"support:",s
                    
            print ""
            print "Misclassified articles:"
            print "(Some labels on the pages the links go to were ommited for not being human diseases)"
            for item, gt, p in zip(
                data_set.items,
                data_set.get_labels(add_parents=True),
                predictions
            )[:100]:
                if set(p) != set(gt):
                    print "http://healthmap.org/ai.php?" + item['name'][:-4]
                    print "gt:", gt, "prediction:", p
                    print ""
            
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-debug', action='store_true')
    parser.add_argument('-pickle_dir', default='')
    args = parser.parse_args()
    run_tests(args.pickle_dir)
