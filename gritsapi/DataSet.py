import disease_label_table
import numpy as np
from diagnosis.utils import group_by, flatten
from pymongo import MongoClient
import datetime
import re
import requests
import config
from dateutil import parser

label_overrides = {
    'http://healthmap.org/ai.php?1097880' : ['Gastroenteritis'],
    'http://healthmap.org/ai.php?1220150' : ['Tuberculosis'],
    'http://healthmap.org/ai.php?2612741' : ['Malaria', 'Diarrhoea', 'Dengue'],
    'http://healthmap.org/ai.php?2845361' : ['Dengue'],
    'http://healthmap.org/ai.php?2884661' : ['Echinococcosis'],
    # Articles to omit have no labels:
    # All the information is in the video
    "http://healthmap.org/ai.php?2960401" : [],
    # This article is actually a travel health notice aggregation page with
    # multiple diseases mentioned.
    # I think it is best to omit it.
    "http://healthmap.org/ai.php?1348711" : [],
    "http://healthmap.org/ai.php?2882489": ['Dengue', 'Chikungunya']
}
# Switch the urls in label overrides to be names in our mongo database.
label_overrides = {
    k.split('?')[1] + '0000' : v
    for k, v in label_overrides.items()
}

class DataSet(object):
    """
    A training or test dateset for a classifier
    """
    def __init__(self, items=None):
        self.items = []
        self.rejected_items = 0
        if items:
            for item in items:
                self.append(item)
    def append(self, item):
        if item['name'] in label_overrides:
            item['labels'] = label_overrides[item['name']]
        else:
            item['labels'] = [
                disease
                for event in item['meta']['events']
                for disease in event['diseases']
                if disease is not None
            ]
            if any([
                not disease_label_table.is_in_table(disease)
                for event in item['meta']['events']
                for disease in event['diseases']
            ]):
                self.rejected_items += 1
                return
        if len(item['labels']) == 0:
            self.rejected_items += 1
            # There are too many to list:
            # print "Warning: skipping unlabeled (or animal only) item at",\
            #     "http://healthmap.org/ai.php?" + item['name'][:-4]
            return
        return self.items.append(item)
    def extend(self, array):
        for item in array:
            self.append(item)
    def __len__(self):
        return len(self.items)
    def get_feature_dicts(self):
        if hasattr(self, '_feature_dicts'):
            return self._feature_dicts
        def get_cleaned_english_content(report):
            translation_dict = report\
                .get('private', {})\
                .get('englishTranslation')
            if translation_dict:
                assert translation_dict.get('error') is None
                assert translation_dict.get('content')
                return translation_dict.get('content')
            else:
                return  report\
                .get('private', {})\
                .get('cleanContent', {})\
                .get('content')
        self._feature_dicts = self.feature_extractor.transform(
            map(get_cleaned_english_content, self.items)
        )
        return self._feature_dicts
    def get_feature_vectors(self):
        """
        Vectorize feature_dicts, filter some out, and add parent labels.
        """
        if hasattr(self, '_feature_vectors'):
            return self._feature_vectors
        features = []
        for feature_vector in self.dict_vectorizer.transform(self.get_feature_dicts()):
            features.append(feature_vector)
        self._feature_vectors = np.array(features)
        return self._feature_vectors
    def get_labels(self, add_parents=False):
        def get_item_labels(item):
            if add_parents:
                all_labels = set(item['labels'])
                for label in item['labels']:
                    for l2 in disease_label_table.get_inferred_labels(label):
                        all_labels.add(l2)
                return list(all_labels)
            else:
                return item['labels']
        return map(get_item_labels, self.items)
    def remove_zero_feature_vectors(self):
        props = zip(self.items, self.get_feature_dicts(), self.get_feature_vectors())
        original_items = self.items
        self.items = []
        self._feature_dicts = []
        self._feature_vectors = []
        for item, f_dict, f_vec in props:
            if f_vec.sum() > 0:
                self.items.append(item)
                self._feature_dicts.append(f_dict)
                self._feature_vectors.append(f_vec)
        print "Articles removed because of zero feature vectors:"
        print len(original_items) - len(self.items), '/', len(original_items)

def clear_duplicates(data_set):
    data_dict = {}
    for item in data_set:
        if not (item["name"] in data_dict):
            data_dict[item["name"]] = item
        else:
            data_dict[item["name"]]["meta"]["events"][0]["diseases"].extend(
                item["meta"]["events"][0]["diseases"])
    return data_dict.values()

def fetch_promed_datasets():
    def promed_to_girder_format(report):
        return {
            "name" : "promed" + report["promedId"],
            "meta" : {
                "events" : [
                    {
                        "diseases" : report["plantDisease"] 
                    } 
                ]
            },
            "private" : {
                "cleanContent" : {
                    "content" : report["articles"][0]["content"]
                }
            }
        }
    client = MongoClient(config.mongo_url)
    db = client.promed
    posts = db.posts
    def processDisease(diseaseName):
        matchRE = re.compile(diseaseName, re.IGNORECASE)
        post_list = list(posts.find({
            "subject.description": matchRE,
            "articles": { "$ne": [] }
        }).sort("promedDate", pymongo.ASCENDING))
        print diseaseName, "has", len(post_list), "posts"
        for article in post_list:
            article["plantDisease"] = [diseaseName]
        return post_list
    # this could be updated to be a dictionary containing the display name and the search regex
    diseases = disease_label_table.get_promed_labels()
    training_set = []
    time_offset_test_set = []
    for disease in diseases:
        results = map(promed_to_girder_format, processDisease(disease))
        if len(results) < 10:
            training_set.extend(results)
        else:
            time_offset_test_set.extend(results[0:5])
            training_set.extend(results[5:])
    training_set = clear_duplicates(training_set)
    time_offset_test_set = clear_duplicates(time_offset_test_set)
    #remove items in the test set that are also in the training set
    deduped_test = []
    for test in time_offset_test_set:
        if all([x["name"] != test["name"] for x in training_set]):
            deduped_test.append(test)
    return training_set, deduped_test

def fetch_eha_curated_datasets():
    def eha_to_girder_format(report):
        return {
            "name" : "eha" + report['id'],
            "meta" : {
                "events" : [
                    {
                        "diseases" : report["labels"]
                    }
                ]
            },
            "private" : {
                "cleanContent" : {
                    "content" : report["content"]
                }
            }
        }
    resp = requests.get("https://grits.ecohealthalliance.org/trainingData",
      data={
        "email": config.grits_curator_email,
        "password": config.grits_curator_password
      })
    training_set = []
    time_offset_test_set = []
    label_to_articles = {}
    for item in resp.json():
        item['created'] = parser.parse(item['created'].replace("+00:00", "Z"))
        for label in item['labels']:
            label_to_articles[label] = label_to_articles.get(label, []) + [item]
    for label, articles in label_to_articles.items():
        articles = sorted(articles, key=lambda a: a['created'])
        articles = map(eha_to_girder_format, articles)
        if len(articles) < 10:
            training_set.extend(articles)
        else:
            time_offset_test_set.extend(articles[0:5])
            training_set.extend(articles[5:])
    training_set = clear_duplicates(training_set)
    time_offset_test_set = clear_duplicates(time_offset_test_set)
    #remove items in the test set that are also in the training set
    deduped_test = []
    for test in time_offset_test_set:
        if all([x["name"] != test["name"] for x in training_set]):
            deduped_test.append(test)
    return training_set, deduped_test

datasets = tuple()
def fetch_datasets():
    global datasets
    if len(datasets) > 0:
        print "Returning cached datasets"
        return datasets
    # The train set is 90% of all data after the first ~7 months of HM data
    # that we have access to.
    # The mixed-test set is the other 10% of the data.
    # The time-offset test set is the first ~6 months.
    # There is a 1 month buffer between the train and test set
    # to avoid overlapping events.
    # We use the first 6 months rather than the last because we keep adding 
    # new data and want this test set to stay the same.
    girder_db = client.girder
    start_date = datetime.datetime(2013, 1, 8, 0, 9, 12)
    time_offset_test_set = DataSet(girder_db.item.find({
        "meta.date" : {
            "$lte" : start_date + datetime.timedelta(180),
            "$gte" : start_date
        },
        "private.cleanContent.content": { "$ne" : None },
        "private.cleanContent.malformed": { "$ne" : True },
        "private.cleanContent.clearnerVersion" : "0.0.0",
        # There must be no english translation, or the english translation
        # must have content (i.e. no errors occurred when translating).
        "$or" : [
            { "private.englishTranslation": { "$exists" : False } },
            { "private.englishTranslation.content": { "$ne" : None } },
        ],
        "meta.events": { "$ne" : None },
        "private.scrapedData.scraperVersion" : "0.0.3",
        # Some unscrapable articles have content from previous scrapes.
        # This condition filters them out since they may have been
        # cleaned/translated by obsolete code.
        "private.scrapedData.unscrapable" : { "$ne" : True },
        "private.scrapedData.url": { "$exists" : True },
        # This filters out articles that appear to redirect to a different page.
        "$where" : "this.private.scrapedData.sourceUrl.length < this.private.scrapedData.url.length + 12"
    }))
    remaining_reports = girder_db.item.find({
        "meta.date" : {
            "$gt" : start_date + datetime.timedelta(210)
        },
        "private.cleanContent.content": { "$ne" : None },
        "private.cleanContent.malformed": { "$ne" : True },
        "private.cleanContent.clearnerVersion" : "0.0.0",
        # There must be no english translation, or the english translation
        # must have content (i.e. no errors occurred when translating).
        "$or" : [
            { "private.englishTranslation": { "$exists" : False } },
            { "private.englishTranslation.content": { "$ne" : None } },
        ],
        "meta.events": { "$ne" : None },
        "private.scrapedData.scraperVersion" : "0.0.3",
        # Some unscrapable articles have content from previous scrapes.
        # This condition filters them out since they may have been
        # cleaned/translated by obsolete code.
        "private.scrapedData.unscrapable" : { "$ne" : True },
        "private.scrapedData.url": { "$exists" : True },
        # This filters out articles that appear to redirect to a different page.
        "$where" : "this.private.scrapedData.sourceUrl.length < this.private.scrapedData.url.length + 12"
    })
    training_set = DataSet()
    mixed_test_set = DataSet()
    
    # If there are too many reports we will run out of memory when training
    # the classifier, so a portion of the reports will not be used if we go
    # over this limit.
    # It could probably be higher, but when I tried 20000 I ran into an 
    # OutOfMemeory exception (even though `top` showed 5GB of free swap memory).
    report_limit = 18000
    usable_portion = float(report_limit) / remaining_reports.count()

    for report in remaining_reports:
        # Choose 1/10 articles for the mixed test set
        if int(report['name'][:-4]) % 10 == 9:
            mixed_test_set.append(report)
        else:
            if int(report['name'][:-4]) % 10 < int(usable_portion * 10):
                training_set.append(report)

    promed_training_set, promed_time_offset_test_set = fetch_promed_datasets()
    training_set.extend(promed_training_set)
    time_offset_test_set.extend(promed_time_offset_test_set)
    
    eha_training_set, eha_test_set = fetch_eha_curated_datasets()
    training_set.extend(eha_training_set)
    time_offset_test_set.extend(eha_test_set)
    
    print "time_offset_test_set size", len(time_offset_test_set), " | rejected items:", time_offset_test_set.rejected_items
    print "mixed_test_set size", len(mixed_test_set), " | rejected items:", mixed_test_set.rejected_items
    print "training_set size", len(training_set), " | rejected items:", training_set.rejected_items
    
    print "Label counts:"
    label_counts = {}
    all_labels = training_set.get_labels(True) + time_offset_test_set.get_labels(True) + mixed_test_set.get_labels(True)
    for label in flatten(all_labels):
        label_counts[label] = label_counts.get(label, 0) + 1
    for label, count in sorted(label_counts.items(), key=lambda k: k[1]):
        print "    " + label, ":", count
    
    # Check that plant disease aritcles are in test set.
    assert "Downy Mildew" in flatten(time_offset_test_set.get_labels())
    
    datasets = (
        time_offset_test_set,
        mixed_test_set,
        training_set
    )
    return datasets
