import tika
import logging
import unicodedata
import re
import requests
from tika import parser
from boilerpipe.extract import Extractor
from urllib.error import URLError
from socket import timeout


    Returns:
        Extracted text as string or None
    """
    # if url is None:
    #     return url
    # else:
    if not my_utils.connection_is_possible():
        import os
        proxy = my_utils.load_rki_header_and_proxy_dict()["proxy"]
        for proxy_type, proxy in proxy.items():
            print(proxy_type, proxy)
            os.environ[proxy_type] = proxy
    if 'pdf' in url:
        tika.TikaClientOnly = True
        extracted = _extract_cleaned_text_from_pdf(url)
    else:
        if 'pdf' in url:
            tika.TikaClientOnly = True
            extracted = _extract_cleaned_text_from_pdf(url)
        else:
            kwargs = {'url': url}
            if 'promed' in url:
                html = get_html_from_promed_url(url)
                kwargs = {'html': html}
            extracted = _extract_cleaned_text_from_html_webpage(**kwargs)
        return _remove_control_characters(extracted)


def _extract_cleaned_text_from_pdf(url):
    # Extract text from pdf and also remove unrecognized symbols
    try:
        log = logging.getLogger('tika.tika')
        log.disabled = True
        raw = parser.from_file(url)
        text = raw['content'].replace('�', '')
    except (URLError, ValueError, timeout) as e:
        print(f'{url} caused {e}')
        text = None
    return text


def _extract_cleaned_text_from_html_webpage(**kwargs):
    try:
        text = Extractor(extractor='ArticleExtractor', **kwargs).getText()
    except (UnicodeDecodeError, URLError, ValueError, timeout) as e:
        print(e)
        text = None
    return text


def get_html_from_promed_url(url):
    id_ = re.search(r'(\d+)', url)[0]
    url = f'http://www.promedmail.org/ajax/getPost.php?alert_id={id_}'
    ajax_request_for_post_as_html = requests.get(url, headers={"Referer": "http://www.promedmail.org/"}).json().get('post')
    return ajax_request_for_post_as_html


def _remove_control_characters(string):
    if string is None:
        return string
    else:
        string = "".join(char if unicodedata.category(char)[0] != "C" else ' ' for char in string)
        string_without_double_spaces = re.sub(r'(\s){2,}', ' ', string)  # Spaces added after removal of characters
        return string_without_double_spaces
