import pickle
import os
import numpy as np
import pandas as pd
import urllib.error
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


def get_count_optimization_edb(edb=None, use_pickle=True, filter_margin='1day', label_margin='2days'):
    edb = get_cleaned_edb()
    edb_links_combined = _get_edb_with_combined_link_columns(edb)
    date_optimization_edb = _get_optimization_edb(edb_links_combined)
    date_optimization_edb_extracted_text = _extract_text_from_edb_urls(date_optimization_edb)
    date_optimization_edb_with_annos = _annotate_text_in_edb(date_optimization_edb_extracted_text)

    return date_optimization_edb_with_annos


def _add_labels_and_clear_edb(edb, filter_margin, label_margin):
    date_optimization_filtered = _filter_too_broad_annotated_time_spans(edb, filter_margin)
    date_optimization_edb_with_labels = _assign_label_and_drop_dates(date_optimization_filtered, label_margin)
    return date_optimization_edb_with_labels.dropna(axis='rows')


def _get_edb_with_combined_link_columns(edb):
    link_columns = [column for column in edb.columns.tolist() if 'link' in column.lower()]
    edb_with_any_link = edb[link_columns].dropna(how='all')
    urls = edb_with_any_link.apply(lambda x: list(split_and_flatten_list(x)), axis=1)
    edb_links_combined = edb.drop(link_columns, axis=1)
    edb_links_combined['links'] = pd.Series(list(map(lambda x: split_and_flatten_list(x), urls)))
    return _remove_invalid_urls(edb_links_combined)


def _get_optimization_edb(edb):
    # Data Frame for date or count?

    edb = edb[edb['Fälle gesamt*'].notna()]
    edb = edb[['Fälle gesamt*', 'links']]
    return edb


def _extract_text_from_edb_urls(edb, disable_tqdm=False):
    # TODO: think about a test for this function

    edb_with_text = pd.DataFrame(columns=['Fälle gesamt*', 'links', 'text'])
    for i, urls in enumerate(tqdm(edb['links'], disable=disable_tqdm, postfix='Extract text from link')):
        for j, url in enumerate(urls):
            edb_with_text = _try_to_extract_text_from_url_and_fill_edb_with_text(edb, edb_with_text, url, i)
    return edb_with_text


def _annotate_text_in_edb(edb):
    annos = pd.Series(np.zeros(len(edb)))
    for i, text in enumerate(tqdm(edb['text'], postfix='Annotate text')):
        annotated = annotate(text, tiers='CountAnnotator()')
        annos.loc[i] = annotated
    edb['annotated'] = annos
    return edb


def _extract_sentences_from_spans(edb, drop_annotated=True):
    print(edb.columns)
    edb_with_sentences = pd.DataFrame(columns=['Fälle gesamt*',
                                               'sentence',
                                               'count'])
    for i, row in tqdm(edb[['Fälle gesamt*', 'annotated']].iterrows(),
                       total=edb.shape[0],
                       postfix='Extract sentences and dates from spans'):
        target_count, anno = row
        for span in anno.tiers['counts'].spans:
            sentence = _try_extract_sentences(span, anno)

            to_append = pd.Series({'Fälle gesamt*': target_count,
                                   'sentence': sentence,
                                   'count': span.metadata['count']})
            edb_with_sentences = edb_with_sentences.append(to_append, ignore_index=True)
    if not drop_annotated:
        edb_with_sentences['annotated'] = edb['annotated']  # AnnoDocs cannot be pickled, so I drop them
    return edb_with_sentences


def _filter_too_broad_annotated_time_spans(edb, allowed_margin):
    return edb[edb['to'] - edb['from'] <= pd.Timedelta(allowed_margin)]


def _assign_label_and_drop_dates(edb, allowed_margin):
    is_in_time_range = (((edb['from'] - pd.Timedelta(allowed_margin)) <= edb['Fälle gesamt*'])
                        & ((edb['to'] + pd.Timedelta(allowed_margin)) >= edb['Fälle gesamt*']))
    edb = edb.assign(is_label=is_in_time_range)
    return edb[['sentence', 'is_label']]


def _try_extract_sentences(span, anno):
    try:
        date_sentences = get_sentence_and_date_from_annotated_span(span, anno)
    except AttributeError:
        date_sentences = ([], [np.nan, np.nan])  # Occurs when sentences is empty
    return date_sentences


def _try_to_extract_text_from_url_and_fill_edb_with_text(edb, edb_with_text, url, i):
    try:
        text_extracted = extract_cleaned_text_from_url(url)
        count = edb['Fälle gesamt*'].iloc[i]
        to_append = pd.Series({'Fälle gesamt*': count,
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


def _create_new_row(count, link, text_extracted):
    return pd.Series({'Fälle gesamt*': count,
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
