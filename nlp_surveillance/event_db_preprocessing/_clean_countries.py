import re
import warnings
import pandas as pd
import numpy as np


def clean_countries(event_db):
    event_db.country_edb = event_db.country_edb.apply(_clean_country_str)
    event_db = _split_country_by_comma(event_db)
    return event_db


def _clean_country_str(country):
    if isinstance(country, str):
        country = re.sub(r'\n', ', ', country)
        country = re.sub(r',,', ',', country)  # Because the line above adds one comma to much
        country = re.sub(r'\(.*\)', "", country)
        country = country.replace("&", "und")
        country = country.replace("_", " ")
        country = country.strip(" ")
        country = _correct_wrong_use_of_cardinal_directions(country)
    return country


def _split_country_by_comma(event_db):
    split, length = _split_by_comma_and_then_calc_len_of_split(event_db)
    repeated_entries_dict = _repeat_entries_as_dict(event_db, length)
    repeated_entries_dict['country_edb'] = np.concatenate(split.values)
    split_event_db = pd.DataFrame(repeated_entries_dict, columns=repeated_entries_dict.keys())
    split_event_db.country_edb = split_event_db.country_edb.str.strip()
    return split_event_db


def _correct_wrong_use_of_cardinal_directions(country):
    cardinal_dir = re.compile(r"(Süd|Nord|West|Ost)\s(\S*)")
    if cardinal_dir.match(country) and country.lower() != 'korea':
        # Except for korea, in German, cardinal direction and country name are written separately
        try:
            country = cardinal_dir.match(country)[1] + cardinal_dir.match(country)[2].lower()
        except IndexError:
            warnings.warn('Problems with processing country string with cardinal direction in name')
    return country


def _split_by_comma_and_then_calc_len_of_split(event_db):
    split = event_db.country_edb.str.split(',')
    # Nones in country column cause non-list entries that need to be transformed to list
    split = pd.Series([country if isinstance(country, list) else [country]
                       for country in split])

    length = split.str.len()
    length = [1 if np.isnan(i) else int(i) for i in length]  # Also caused by Nones in country column
    return split, length


def _repeat_entries_as_dict(event_db, length):
    columns = event_db.columns
    return {column: np.repeat(event_db[column].values, length) for column in columns
            if column != 'country_edb'}


