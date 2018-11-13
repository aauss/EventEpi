import celery
import json
import bson
import pickle
import diagnosis
import datetime
from distutils.version import StrictVersion
import datetime

import tasks_preprocess
from tasks_preprocess import celery_tasks
from tasks_preprocess import make_json_compat
from celery.exceptions import SoftTimeLimitExceeded

from diagnosis.Diagnoser import Diagnoser
class DiagnoserTask(celery.Task):
    """
    This abstract base class is used so the diagnoser is only loaded
    when the tasks start running.
    There are cases where this file may be imported when the the diagnoser
    cannot be loaded (because of missing pickles).
    """
    abstract = True
    _diagnoser = None
    @property
    def diagnoser(self):
        if self._diagnoser is None:
            with open('current_classifier/classifier.p') as f:
                my_classifier = pickle.load(f)
            with open('current_classifier/dict_vectorizer.p') as f:
                my_dict_vectorizer = pickle.load(f)
            with open('current_classifier/keyword_array.p') as f:
                keyword_array = pickle.load(f)
            self._diagnoser = Diagnoser(
                my_classifier,
                my_dict_vectorizer,
                keyword_array=keyword_array,
                cutoff_ratio=.7
            )
        return self._diagnoser

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@celery_tasks.task(base=DiagnoserTask, name='tasks.diagnose')
def diagnose(text_obj, extra_args):
    try:
        english_translation = text_obj.get('englishTranslation', {}).get('content')
        if english_translation:
            clean_english_content = english_translation
        else:
            clean_english_content = text_obj.get('cleanContent', {}).get('content')
        if clean_english_content:
            logger.info('Diagnosing text:\n' + clean_english_content)
            return make_json_compat(diagnose.diagnoser.diagnose(
                clean_english_content, **extra_args))
        else:
            return { 'error' : 'No content available to diagnose.' }
    except SoftTimeLimitExceeded:
        return { 'error' : 'Timelimit exceeded.' }
