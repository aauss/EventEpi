import tika
import logging
import unicodedata
import re
from tika import parser
from boilerpipe.extract import Extractor
from urllib.error import URLError
from socket import timeout


def extract_cleaned_text_from_url(url):
    if url is None:
        return url
    else:
        if 'pdf' in url:
            tika.TikaClientOnly = True
            extracted = _extract_cleaned_text_from_pdf(url)
        else:
            extracted = _extract_cleaned_text_from_html_webpage(url)
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


def _extract_cleaned_text_from_html_webpage(url):
    try:
        text = Extractor(extractor='ArticleExtractor', url=str(url)).getText()
    except (UnicodeDecodeError, URLError, ValueError, timeout) as e:
        print(f'{url} caused {e}')
        text = None
    return text


def _remove_control_characters(string):
    if string is None:
        return string
    else:
        string = "".join(char for char in string if unicodedata.category(char)[0] != "C")
        string_without_double_spaces = re.sub(r'(\s){2,}', ' ', string)  # Spaces added after removal of characters
        return string_without_double_spaces
