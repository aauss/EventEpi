import os
import numpy as np
import pandas as pd

from ._clean_dates import to_datetime
from ._clean_counts import clean_counts
from ._clean_countries import clean_countries


def read_cleaned(path=None):
    event_db = _read_unprocessed(path=path)
    preprocessed_event_db = (event_db
                             .pipe(_rename_and_drop_unused_columns)
                             .pipe(_format_missing_data)
                             .pipe(to_datetime)
                             .pipe(clean_counts)
                             .pipe(clean_countries))
    return preprocessed_event_db


def _read_unprocessed(path=None):
    if path is None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, '..', '..', 'data', 'rki', 'edb.csv')
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
    event_db = event_db.loc[:, columns_to_keep]
    # Rename columns
    event_db.columns = ['country_edb', 'disease_edb', 'date_of_data', 'count_edb', 'URL_1', 'URL_2', 'URL_3', 'URL_4']
    return event_db


def _format_missing_data(event_db):
    str_columns = ['country_edb', 'disease_edb', 'URL_1', 'URL_2', 'URL_3', 'URL_4']
    event_db.loc[:, str_columns] = event_db.loc[:, str_columns].replace(['nan', '-', np.nan], [None] * 3)

    numerical_columns = ['count_edb', 'date_of_data']
    event_db.loc[:, numerical_columns] = event_db.loc[:, numerical_columns].replace(['nan', '-'], [np.nan] * 2)

    event_db = event_db.dropna(how='all')
    return event_db
