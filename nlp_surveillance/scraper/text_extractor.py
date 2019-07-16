import tika
import logging
import unicodedata
import re
import requests

from typing import Union
from requests.exceptions import ConnectionError
from tika import parser
from boilerpipe.extract import Extractor
from urllib.error import URLError
from socket import timeout


def extract_cleaned_text_from_url(url: str, proxy: dict) -> Union[str, None]:
    """Extracts the main text from an URL using boilerpipe if its an HTML and tika if it is an PDF

    Args:
        url: An url to extract text from
        proxy: Proxy settings for requests

    Returns:
        Extracted text
    """

    if 'pdf' in url:
        tika.TikaClientOnly = True
        extracted = _extract_cleaned_text_from_pdf(url)
    else:
        kwargs = {'url': url}
        if 'promed' in url:
            html = get_html_from_promed_url(url, proxy)
            kwargs = {'html': html}
        try:
            extracted = Extractor(extractor='ArticleExtractor', **kwargs).getText()
        except Exception as e:
            print(f'{url} caused {e}')
            extracted = None
    return _remove_control_characters(extracted)


def _extract_cleaned_text_from_pdf(url):
    try:
        log = logging.getLogger('tika.tika')
        log.disabled = True
        raw = parser.from_file(url)
        text = raw['content'].replace('�', '')
    except (URLError, ValueError, timeout) as e:
        print(f'{url} caused {e}')
        text = None
    return text


def get_html_from_promed_url(url: str, proxy: dict = None):
    try:
        id_ = re.search(r'(\d+)', url)[0]
        url = f'http://www.promedmail.org/ajax/getPost.php?alert_id={id_}'
        ajax_request_for_post_as_html = (requests.get(url,
                                                      headers={"Referer": "http://www.promedmail.org/"},
                                                      proxies=proxy)
                                         .json()
                                         .get('post'))
    except (TypeError, ConnectionError):
        ajax_request_for_post_as_html = None
    return ajax_request_for_post_as_html


def _remove_control_characters(string):
    if string is None:
        return string
    else:
        string = "".join(char if unicodedata.category(char)[0] != "C" else ' ' for char in string)
        string_without_double_spaces = re.sub(r'(\s){2,}', ' ', string)  # Spaces added after removal of characters
        return string_without_double_spaces
