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
    event_db_nans['date_of_data'] = event_db_nans['date_of_data'].apply(_to_timestamp)
    return event_db_nans


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
    event_db.columns = ['country_edb', 'disease_edb', 'date_of_data', 'count_edb', 'URL_1', 'URL_2', 'URL_3', 'URL_4']
    return event_db


def _format_missing_data(event_db):
    str_columns = ['country_edb', 'disease_edb', 'URL_1', 'URL_2', 'URL_3', 'URL_4']
    event_db[str_columns] = event_db[str_columns].copy().replace(['nan', '-', np.nan], [None] * 3)

    numerical_columns = ['count_edb', 'date_of_data']
    event_db[numerical_columns] = event_db[numerical_columns].copy().replace(['nan', '-'], [np.nan] * 2)

    event_db = event_db.dropna(how='all')
    return event_db


def _to_timestamp(date):
    date_matched = re.match(r"(\d{1,2})\D(\d{1,2})\D(\d{4})", str(date))
    if date_matched:
        day = int(date_matched[1])
        month = int(date_matched[2])
        year = int(date_matched[3])

        try:
            time_stamp = pd.Timestamp(year=int(year), month=int(month), day=int(day))
        except (TypeError, UnknownTimeZoneError):
            time_stamp = pd.NaT
            warnings.warn(f'{date} is not an existing date. Please change it')
    else:
        time_stamp = pd.NaT
    return time_stamp


def _to_count(count):
    pass
