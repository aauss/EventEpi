import tika
import logging
from tika import parser
from boilerpipe.extract import Extractor
from nltk.stem import WordNetLemmatizer
from nltk.corpus import names, stopwords
from nltk.tokenize import word_tokenize
from urllib.error import URLError

from.my_utils import remove_control_characters


def extract_cleaned_text_from_url(url):
    if 'pdf' in url:
        tika.TikaClientOnly = True
        extracted = extract_cleaned_text_from_pdf(url)
    else:
        extracted = extract_cleaned_text_from_html_webpage(url)
    return remove_control_characters(extracted)


def extract_cleaned_text_from_pdf(url):
    # Extract text from pdf and also remove unrecognized symbols
    try:
        log = logging.getLogger('tika.tika')
        log.disabled = True
        raw = parser.from_file(url)
        text = raw['content'].replace('�', '')
    except URLError as e:
        print('{url} caused {e}'.format(url=url, e=e))
        text = ""
    return text


def extract_cleaned_text_from_html_webpage(url):
    # Use boilerpipe to extract main text from url
    try:
        text = Extractor(extractor='ArticleExtractor', url=str(url)).getText()
    except (UnicodeDecodeError, URLError) as e:
        print('{url} caused {e}'.format(url=url, e=e))
        text = ""
    return text


def clean_text(text):
    word_token = word_tokenize(text)
    stop_words = set(stopwords.words('english'))
    cleaned_text = [_lemmatize(word) for word in word_token
                    if word not in stop_words
                    and _letters_only(word)]
    return cleaned_text


def _lemmatize(word):
    lemmatizer = WordNetLemmatizer()
    list_of_names = set(names.words())
    if word not in list_of_names:
        word = lemmatizer.lemmatize(word)
    return word


def _letters_only(astr):
    return astr.isalpha()
