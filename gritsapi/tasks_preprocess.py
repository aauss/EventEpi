# This file is separate from tasks.py so that it can function without loading
# the classifier data pickles. This makes it possible to preprocess articles
# without training a classifier, and it reduces the memory footprint.
import celery
import json
import bson
from celery import Celery
import datetime
from distutils.version import StrictVersion
import config
import logging
import datetime
from scraper.process_resources import extract_clean_content
from scraper import scraper
from scraper.translation import Translator
import os

my_translator = Translator()

processor_version = '0.1.0'

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
    elif isinstance(obj, bson.int64.Int64):
        return int(obj)
    else:
        raise TypeError(type(obj))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

celery_tasks = Celery('tasks', broker=os.environ.get('BROKER_URL') or config.BROKER_URL)

celery_tasks.conf.update(
    CELERY_TASK_SERIALIZER='pickle',
    CELERY_ACCEPT_CONTENT=['pickle'],  # Ignore other content
    CELERY_RESULT_SERIALIZER='pickle',
    CELERY_RESULT_BACKEND=os.environ.get('BROKER_URL') or config.BROKER_URL,
    CELERYD_TASK_SOFT_TIME_LIMIT=60,
    CELERYD_TASK_TIME_LIMIT=65,
)

celery_tasks.conf.broker_transport_options = {'visibility_timeout': 3600}  # 1 hour.

@celery_tasks.task(name='tasks.scrape')
def scrape(url):
    """
    Scrape the meta.link of the girder item with the given id.
    Update the entry with the results, then update it with cleaned and 
    translated versions of the scraped content.
    """
    return make_json_compat(scraper.scrape(url))

@celery_tasks.task(name='tasks.process_text')
def process_text(text_obj):
    result = {}
    # These first conditions are for handling scraper output. They are currently
    # unnecessairy since we don't allow users to submit URLs.
    if text_obj.get('unscrapable'):
        result['scrapedData'] = text_obj
        result['error'] = text_obj.get('error', text_obj.get('exception'))
        return make_json_compat(result)
    if 'htmlContent' in text_obj:
        result['scrapedData'] = text_obj
        clean_content = extract_clean_content(text_obj['htmlContent'])
    else:
        clean_content = text_obj
    if my_translator.is_english(clean_content['content']):
        result['cleanContent'] = clean_content
    else:
        result['englishTranslation'] = my_translator.translate_to_english(
            clean_content['content'])
        result['cleanContent'] = clean_content
    return make_json_compat(result)
