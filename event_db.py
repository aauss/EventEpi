import os
import re
import warnings
import numpy as np
import pandas as pd
from pytz.exceptions import UnknownTimeZoneError


def read_cleaned(path=None):
    event_db = _read_unprocessed(path=path)
    preprocessed_event_db = (event_db
                             .pipe(_rename_and_drop_unused_columns)
                             .pipe(_format_missing_data)
                             .pipe(_to_datetime)
                             .pipe(_clean_counts)
                             .pipe(_clean_country))
    return preprocessed_event_db


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


def _clean_counts(event_db):
    event_db.count_edb = event_db.count_edb.replace(['.', '.'], ['', ''])
    event_db.count_edb = event_db.count_edb.apply(_keep_only_integers)
    return event_db


def _clean_country(event_db):
    event_db.country_edb = event_db.country_edb.apply(_clean_country_str)
    # TODO: Denke über das auffächern nach, wenn ein eintrag ein komma hat
    return event_db


def _clean_country_str(country):
    if isinstance(country, str):
        cardinal_dir = re.compile(r"(Süd|Nord|West|Ost)\s(\S*)")
        country = re.sub(r'\n', ', ', country)
        country = re.sub(r',,', ',', country)  # Because the line above adds one comma to much
        country = re.sub(r'\(.*\)', "", country)
        country = country.replace("&", "und")
        country = country.replace("_", " ")
        country = country.strip(" ")
        if cardinal_dir.match(country) and country.lower() != 'korea':
            # Except for korea, in German, cardinal direction and country name are written separately
            try:
                country = cardinal_dir.match(country)[1] + cardinal_dir.match(country)[2].lower()
            except IndexError:
                warnings.warn('Problems with processing country string with cardinal direction in name')
    return country


def _keep_only_integers(string_with_int):
    if isinstance(string_with_int, str):
        int_as_string = re.search(r'(\d)+', string_with_int)[0]
        return int(int_as_string)
    else:
        return string_with_int



