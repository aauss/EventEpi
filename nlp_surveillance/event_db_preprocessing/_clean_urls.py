import re

import utils


def clean_urls(event_db):
    event_db = utils.split_strings_at_comma_and_distribute_to_new_rows(event_db, 'URL_1')
    event_db.URL_1 = event_db.URL_1.apply(_remove_guillemets())
    return event_db


def _remove_guillemets(url):
    # Remove the first and last guillemets. Found in URLs of edb
    try:
        url = re.sub(r'<', '', url, 1)
        url = re.sub(r'>', '', url[::-1], 1)
        url = url[::-1]
    except TypeError:
        url = None
    return url
