import tika
from boilerpipe.extract import Extractor


def extract_cleaned_text_from_url(url):
    if 'pdf' in url:
        tika.TikaClientOnly = True
        extracted = extract_text_from_pdf(url)
    else:
        extracted = extract_text_from_html_webpage(url)
    return extracted


def extract_text_from_pdf(url):
    # TODO: Refactor to 'cleaned' text...
    # Extract text from pdf and also remove unrecognized symbols
    raw = tika.parser.from_file(url)
    return raw['content'].replace('�', '')


def extract_text_from_html_webpage(url):
    # Use boilerpipe to extract main text from url
    return Extractor(extractor='ArticleExtractor', url=str(url)).getText()
