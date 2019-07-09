import re

from nlp_surveillance import my_utils


def clean_urls(event_db):
    event_db = _combine_url_columns_row_wise_with_comma(event_db)
    event_db = my_utils.split_strings_at_comma_and_distribute_to_new_rows(event_db, 'URL')
    event_db['URL'] = event_db['URL'].str.strip()
    event_db['URL'] = event_db['URL'].apply(_remove_guillemets).apply(_only_keep_valid_urls).apply(_clean_promed_urls)
    return event_db


def _combine_url_columns_row_wise_with_comma(event_db):
    url_columns = list(filter(lambda x: 'url' in x.lower(), event_db.columns))
    event_db['URL'] = event_db[url_columns].apply(_combine_columns_without_nones, axis=1)  # Actually combine
    event_db['URL'] = event_db.URL.apply(lambda x: ','.join(x))
    event_db = event_db.drop(columns=url_columns)
    return event_db


def _combine_columns_without_nones(url_list):
    return list(filter(lambda x: x is not None, url_list))


def _remove_guillemets(url):
    # Remove the first and last guillemets. Found in URLs of edb
    try:
        url = re.sub(r'<', '', url, 1)
        url = re.sub(r'>', '', url[::-1], 1)
        url = url[::-1]
    except TypeError:
        url = None
    return url


def _only_keep_valid_urls(url):

    necessary = ['http', 'www.', '.org', '.com', '.int']
    if (url is not None) and any(nec in url for nec in necessary):
        return url
    else:
        return None


def _clean_promed_urls(url):
    # Necessary since there are many URL writings for the same article
    if (url is not None) and 'promedmail' in url:
        url = url.replace('http:', 'https:')
        no_www_match = re.match(r'(https://)(promedmail.org/post/.*)', url)
        if no_www_match:
            url = no_www_match[1] + 'www.' + no_www_match[2]
        url = re.sub(r'(https://www.promedmail.org/post/)\d+\.(\d+)', r'\1\2', url)
        if 'direct.php?' in url:
            url = re.sub(r'(https://www.promedmail.org/)direct\.php\?id=\d+\.(\d+)', r'\1\2', url)
    return url
