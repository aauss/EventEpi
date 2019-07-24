import os
import numpy as np
import pandas as pd

from .clean_dates import to_datetime
from .clean_counts import clean_counts
from .clean_countries import clean_countries
from .clean_diseases import clean_diseases
from .clean_urls import clean_urls


def read_cleaned(path: str = None) -> pd.DataFrame:
    """A method to apply all preprocessing steps for the incident database

    Args:
        path: Path of unprocessed incident database

    Returns:
        Cleaned/preprocessed incident database
    """
    event_db = _read_unprocessed(path=path)
    preprocessed_event_db = (event_db
                             .pipe(_rename_and_drop_unused_columns)
                             .pipe(_format_missing_data)
                             .pipe(to_datetime)
                             .pipe(clean_counts)
                             .pipe(clean_countries)
                             .pipe(clean_diseases)
                             .pipe(clean_urls))
    return preprocessed_event_db


def _read_unprocessed(path=None):
    if path is None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, '..', '..', 'data', 'rki', 'idb.csv')
    event_db = pd.read_csv(path, sep=';')
    event_db.columns = list(map(lambda x: x.strip(), event_db))
    return event_db


def _rename_and_drop_unused_columns(event_db):
    urls = list(filter(lambda x: 'Link zur Quelle' in x, event_db.columns))
    columns_to_keep = ['Ausgangs- bzw. Ausbruchsland',
                       'Krankheitsbild(er)',
                       'Datenstand für Fallzahlen gesamt*',
                       'Fälle gesamt*']
    columns_to_keep.extend(urls)
    event_db = event_db.loc[:, columns_to_keep]
    # Rename columns
    event_db.columns = ['country_edb', 'disease_edb', 'date_of_data', 'count_edb', 'URL_1', 'URL_2', 'URL_3', 'URL_4']
    return event_db


def _format_missing_data(event_db):
    text_columns = ['country_edb', 'disease_edb', 'URL_1', 'URL_2', 'URL_3', 'URL_4']
    event_db.loc[:, text_columns] = event_db.loc[:, text_columns].replace(['nan', '-', np.nan, '', '?', 'keine'],
                                                                          [None] * 6)

    numerical_columns = ['count_edb', 'date_of_data']
    event_db.loc[:, numerical_columns] = event_db.loc[:, numerical_columns].replace(['nan', '-'],
                                                                                    [np.nan] * 2)

    event_db = event_db.dropna(how='all')
    return event_db
