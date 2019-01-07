import numpy as np
import pandas as pd
import pickle
import os
from tqdm import tqdm_notebook as tqdm

from .utils.my_utils import remove_nans, check_url_validity, remove_guillemets, split_and_flatten_list
from .utils.text_from_url import extract_cleaned_text_from_url
from .edb_clean import get_cleaned_edb


def get_date_optimization_edb(edb=None):
    if edb is None:
        edb = get_cleaned_edb()
    edb_links_combined = _get_edb_with_combined_link_columns(edb)
    edb_without_invalid = _remove_invalid_entries(edb_links_combined, to_optimize='date')
    date_optimization_edb = edb_without_invalid[['Datenstand für Fallzahlen gesamt*', 'links']].copy(deep=True)
    return _extract_text_from_edb_urls(date_optimization_edb)


def _get_edb_with_combined_link_columns(edb):
    link_columns = [column for column in edb.columns.tolist() if 'link' in column.lower()]
    edb_with_any_link = edb[link_columns].dropna(how='all')
    urls = edb_with_any_link.apply(lambda x: list(split_and_flatten_list(x)), axis=1)
    edb_links_combined = edb.drop(link_columns, axis=1)
    edb_links_combined['links'] = pd.Series(list(map(lambda x: split_and_flatten_list(x), urls)))
    return edb_links_combined


def _remove_invalid_entries(edb, to_optimize):
    # Data Frame for date or count?
    if to_optimize == 'date':
        valid_target_edb = edb[edb['Datenstand für Fallzahlen gesamt*'].notna()]
        valid_target_edb = valid_target_edb[['Datenstand für Fallzahlen gesamt*', 'links']]
    else:
        valid_target_edb = edb

    # Valid URL
    valid_target_edb['links'] = valid_target_edb['links'].apply(remove_nans)
    valid_target_edb['links'] = valid_target_edb['links'].apply(_only_keep_valid_urls)
    valid_target_and_example_edb = valid_target_edb.reset_index(drop=True)
    return valid_target_and_example_edb


def _extract_text_from_edb_urls(edb, disable_tqdm=False):
    path = os.path.join('pickles', 'extracted_text.p')
    if not os.path.exists(path):
        extracted_text = pd.Series(np.zeros(len(edb)))
        for i, links in enumerate(tqdm(edb['links'], disable=disable_tqdm)):
            text = ''
            for link in links:
                try:
                    text_extracted = extract_cleaned_text_from_url(link)
                    text += text_extracted
                except Exception as e:
                    text += ''
                    print(e, 'during extraction of text form url')

            extracted_text.iloc[i] = text
        pickle.dump(extracted_text, open('extracted_text.p', 'wb'))
    else:
        extracted_text = pickle.load(open(path, 'rb'))
    edb['text'] = extracted_text
    return edb


def _only_keep_valid_urls(list_of_urls):
    removed_guillemets = map(remove_guillemets, list_of_urls)
    valid_urls = filter(check_url_validity, removed_guillemets)
    return list(valid_urls)
