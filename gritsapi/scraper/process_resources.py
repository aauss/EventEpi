import goose
from bs4 import BeautifulSoup
import random
import ctypes
import concurrent.futures
import os, sys, json
import readability
import lxml
import re
__version__ = '0.0.0'
def extract_clean_content(content):
    global __version__
    # I found out about goose and readability here:
    # http://stackoverflow.com/questions/14164350/identifying-large-bodies-of-text-via-beautifulsoup-or-other-python-based-extract
    # The poster seems to like goose more.
    # One difference is that goose cleans up all the html, while readability
    # usually just remove cruft that isn't related to the article text.
    # There is a trade off between retaining links and formatting, and
    # getting cleaner text.
    # Readability seems to be better at finding the content in some cases
    # so it is used for initial cleaning, then goose is used since its
    # plain text output is easier to deal with downstream.
    method = None
    cleaned_content = ''
    ###### Readability code:
    readability_error = None
    try:
        document = readability.readability.Document(content)
        cleaner_content = document.summary().strip()
        if len(cleaner_content) > 50:
            content = cleaner_content
        else:
            readability_error = "Readability content too short: " + cleaner_content
    except readability.readability.Unparseable as e:
        readability_error = '\n'.join([str(i) for i in sys.exc_info()])
    except (lxml.etree.XMLSyntaxError,
            lxml.etree.DocumentInvalid,
            lxml.etree.ParserError) as e:
        readability_error = '\n'.join([str(i) for i in sys.exc_info()])
    except (AttributeError, ValueError, TypeError) as e:
        # This ought to be handled by readability.
        readability_error = '\n'.join([str(i) for i in sys.exc_info()])
    ######
    
    if not content.startswith('<html>'):
        content = '<html><body>' + content + '</body></html>'
    try:
        cleaned_content = goose.Goose({
            'parser_class':'soup',
            'enable_image_fetching' : False,
        }).extract(raw_html=content).cleaned_text
    except ValueError:
        cleaned_content = ''
    if len(cleaned_content) < 1:
        # Goose doesn't do well with foreign language content.
        # If we can't find content with goose try extracting
        # all the text with Beautiful soup.
        # Beautiful soup doesn't attempt to extract the article,
        # it just finds all the text in the html, which seems to be
        # good enough since we've already used readability on the articles.
        content = re.sub('\<br\s?\/?\>', '\n', content)
        cleaned_content = BeautifulSoup(content).text
    return  {
        'clearnerVersion' : __version__,
        'method' : method,
        'content' : cleaned_content,
        'readability_error' : readability_error,
        # Malformed should be true whenever we can detect an issue with the
        # content that was extracted.
        'malformed' : len(cleaned_content) < 50
    }
