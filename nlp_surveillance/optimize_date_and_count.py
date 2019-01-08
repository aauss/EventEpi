import pickle
import os
import numpy as np
import pandas as pd
import urllib.error
from datetime import datetime
from tqdm import tqdm_notebook as tqdm
from pandas.errors import OutOfBoundsDatetime
from .utils.my_utils import (remove_nans,
                             check_url_validity,
                             remove_guillemets,
                             split_and_flatten_list,
                             get_sentence_and_date_from_annotated_span)
from .utils.text_from_url import extract_cleaned_text_from_url
from .edb_clean import get_cleaned_edb
from .annotator import annotate


def get_date_optimization_edb(edb=None):
    if edb is None:
        edb = get_cleaned_edb()
    path = os.path.join(os.path.dirname(__file__), 'pickles', 'date_opt_edb.p')
    if os.path.isfile(path):
        return pickle.load(open(path, 'rb'))
    else:
        edb_links_combined = _get_edb_with_combined_link_columns(edb)
        date_optimization_edb = _get_optimization_edb(edb_links_combined, to_optimize='date')
        date_optimization_edb_extracted_text = _extract_text_from_edb_urls(date_optimization_edb)
        date_optimization_edb_with_annos = _annotate_text_in_edb(date_optimization_edb_extracted_text)
        date_optimization_edb_extracted_sentences = _extract_sentences_from_spans(date_optimization_edb_with_annos)
        date_optimization_edb_extracted_sentences = date_optimization_edb_extracted_sentences.dropna(axis='rows')
        pickle.dump(date_optimization_edb_extracted_sentences, open(path, 'wb'))
    return date_optimization_edb_extracted_sentences


def _get_edb_with_combined_link_columns(edb):
    link_columns = [column for column in edb.columns.tolist() if 'link' in column.lower()]
    edb_with_any_link = edb[link_columns].dropna(how='all')
    urls = edb_with_any_link.apply(lambda x: list(split_and_flatten_list(x)), axis=1)
    edb_links_combined = edb.drop(link_columns, axis=1)
    edb_links_combined['links'] = pd.Series(list(map(lambda x: split_and_flatten_list(x), urls)))
    return _remove_invalid_urls(edb_links_combined)


def _get_optimization_edb(edb, to_optimize):
    # Data Frame for date or count?
    if to_optimize == 'date':
        edb = edb[edb['Datenstand für Fallzahlen gesamt*'].notna()]
        edb = edb[['Datenstand für Fallzahlen gesamt*', 'links']]
        edb['Datenstand für Fallzahlen gesamt*'] = pd.to_datetime(edb['Datenstand für Fallzahlen gesamt*'].apply(
                                                        lambda x: datetime.strptime(x, '%Y-%m-%d')))
    else:
        edb = edb  # placeholder for count optimized edb
    return edb


def _extract_text_from_edb_urls(edb, disable_tqdm=False):
    # TODO: think about a test for this function
    file_dir = os.path.dirname(__file__)
    path = os.path.join(file_dir, 'pickles', 'edb_with_text.p')
    if not os.path.exists(path):
        edb_with_text = pd.DataFrame(columns=['Datenstand für Fallzahlen gesamt*', 'links', 'text'])
        for i, urls in enumerate(tqdm(edb['links'], disable=disable_tqdm, postfix='Extract text from link')):
            for j, url in enumerate(urls):
                edb_with_text = _try_to_extract_text_from_url_and_fill_edb_with_text(edb, edb_with_text, url, i)
        pickle.dump(edb_with_text, open(path, 'wb'))
    else:
        edb_with_text = pickle.load(open(path, 'rb'))
    return edb_with_text


def _annotate_text_in_edb(edb):
    annos = pd.Series(np.zeros(len(edb)))
    for i, text in enumerate(tqdm(edb['text'], postfix='Annotate text')):
        annotated = annotate(text, tiers='DateAnnotator()')
        annos.loc[i] = annotated
    edb['annotated'] = annos
    return edb


def _extract_sentences_from_spans(edb, drop_annotated=True):
    edb_with_sentences = pd.DataFrame(columns=['Datenstand für Fallzahlen gesamt*',
                                               'from',
                                               'to',
                                               'sentence'])
    for i, row in tqdm(edb[['Datenstand für Fallzahlen gesamt*', 'annotated']].iterrows(),
                       total=edb.shape[0],
                       postfix='Extract sentences and dates from spans'):
        target_date, anno = row
        for span in anno.tiers['dates'].spans:
            sentence, date_in_sentence = _try_extract_sentences(span, anno)
            _from = _try_to_convert_to_timestamp(date_in_sentence[0])
            _to = _try_to_convert_to_timestamp(date_in_sentence[1])
            to_append = pd.Series({'Datenstand für Fallzahlen gesamt*': target_date,
                                   'from': pd.to_datetime(_from),
                                   'to': pd.to_datetime(_to),
                                   'sentence': sentence})
            edb_with_sentences = edb_with_sentences.append(to_append, ignore_index=True)
    if not drop_annotated:
        edb_with_sentences['annotated'] = edb['annotated']  # AnnoDocs cannot be pickled, so I drop them
    return edb_with_sentences


def _try_extract_sentences(span, anno):
    try:
        date_sentences, date_range = get_sentence_and_date_from_annotated_span(span, anno)
    except AttributeError as e:
        date_sentences, date_range = ([], [np.nan, np.nan])  # Occurs when sentences is empty
    return date_sentences, date_range


def _try_to_extract_text_from_url_and_fill_edb_with_text(edb, edb_with_text, url, i):
    try:
        text_extracted = extract_cleaned_text_from_url(url)
        date = edb['Datenstand für Fallzahlen gesamt*'].iloc[i]
        to_append = pd.Series({'Datenstand für Fallzahlen gesamt*': date,
                               'links': url,
                               'text': text_extracted})
        edb_with_text = edb_with_text.append(to_append, ignore_index=True)
    except (ConnectionResetError, urllib.error.HTTPError):
        print(url, 'caused ConnectionResetError')
    return edb_with_text


def _remove_invalid_urls(edb):
    # Valid URL
    edb['links'] = edb['links'].apply(remove_nans)
    edb['links'] = edb['links'].apply(_only_keep_valid_urls)
    valid_url_edb = edb.reset_index(drop=True)
    return valid_url_edb


def _create_new_row(date, link, text_extracted):
    return pd.Series({'Datenstand für Fallzahlen gesamt*': date,
                      'links': link,
                      'text': text_extracted})


def _only_keep_valid_urls(list_of_urls):
    removed_guillemets = map(remove_guillemets, list_of_urls)
    valid_urls = filter(check_url_validity, removed_guillemets)
    return list(valid_urls)


def _try_to_convert_to_timestamp(datetime_obj):
    try:
        return pd.to_datetime(datetime_obj)
    except OutOfBoundsDatetime:
        return np.nan
