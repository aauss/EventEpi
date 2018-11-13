# This is intended to prevent the celery worker from running if the
# classifier has not been trained.
with open('current_classifier/classifier.p') as f: pass
with open('current_classifier/dict_vectorizer.p') as f: pass
with open('current_classifier/keyword_array.p') as f: pass
import tasks_preprocess
import tasks_diagnose
from tasks_preprocess import celery_tasks
