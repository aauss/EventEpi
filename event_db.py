import os
import re
import warnings
import numpy as np
import pandas as pd
from pytz.exceptions import UnknownTimeZoneError


def read_cleaned(path=None):
    event_db = _read_unprocessed(path=path)
    event_db_dropped = _rename_and_drop_unused_columns(event_db)
    event_db_nans = _format_missing_data(event_db_dropped)
    event_db_time_format = _to_datetime(event_db_nans)
    event_db_count_format = _clean_count(event_db_time_format)
    return event_db_count_format


def _read_unprocessed(path=None):
    if path is None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'data', 'event_db', 'edb.csv')
    event_db = pd.read_csv(path, sep=';')
    event_db.columns = list(map(lambda x: x.strip(), event_db))
    return event_db


def _rename_and_drop_unused_columns(event_db):
    URLs = list(filter(lambda x: 'Link zur Quelle' in x, event_db.columns))
    columns_to_keep = ['Ausgangs- bzw. Ausbruchsland',
                       'Krankheitsbild(er)',
                       'Datenstand für Fallzahlen gesamt*',
                       'Fälle gesamt*']
    columns_to_keep.extend(URLs)
    event_db = event_db[columns_to_keep]
    # Rename columns
    event_db.columns = ['country_edb', 'disease_edb', 'date_of_data', 'count_edb', 'URL_1', 'URL_2', 'URL_3', 'URL_4']
    return event_db


def _format_missing_data(event_db):
    str_columns = ['country_edb', 'disease_edb', 'URL_1', 'URL_2', 'URL_3', 'URL_4']
    event_db[str_columns] = event_db[str_columns].copy().replace(['nan', '-', np.nan], [None] * 3)

    numerical_columns = ['count_edb', 'date_of_data']
    event_db[numerical_columns] = event_db[numerical_columns].copy().replace(['nan', '-'], [np.nan] * 2)

    event_db = event_db.dropna(how='all')
    return event_db


def _to_datetime(event_db):
    event_db.date_of_data = event_db.date_of_data.apply(lambda x:
                                                        pd.to_datetime(x,
                                                                       dayfirst=True,
                                                                       errors='coerce'))
    return event_db


def _clean_count(event_db):
    event_db.count_edb = event_db.count_edb.replace(['.', '.'], ['', ''])
    event_db.count_edb = event_db.count_edb.apply(_keep_only_integers)
    return event_db


def _keep_only_integers(string_with_int):
    if isinstance(string_with_int, str):
        int_as_string = re.search(r'(\d)+', string_with_int)[0]
        return int(int_as_string)
    else:
        return string_with_int



