import re
import warnings

import utils


def clean_countries(event_db):
    event_db.country_edb = event_db.country_edb.apply(_clean_country_str)
    event_db = utils.split_strings_at_comma_and_distribute_to_new_rows(event_db, 'country_edb')
    return event_db


def _clean_country_str(country):
    if isinstance(country, str):
        country = re.sub(r'\n', ', ', country)  # Comma instead of new line
        country = re.sub(r',,', ',', country)  # Because the line above adds one comma to much
        country = re.sub(r'\(.*\)', "", country)  # Remove parentheses and their content
        country = country.replace("&", "und")
        country = country.replace("_", " ")
        country = country.strip()
        country = _correct_wrong_use_of_cardinal_directions(country)
    return country


def _correct_wrong_use_of_cardinal_directions(country):
    cardinal_dir = re.compile(r"(Süd|Nord|West|Ost)\s(\S*)")
    if cardinal_dir.match(country) and country.lower() != 'korea':
        # Except for korea, in German, cardinal direction and country name are written separately
        try:
            country = cardinal_dir.match(country)[1] + cardinal_dir.match(country)[2].lower()
        except IndexError:
            warnings.warn('Problems with processing country string with cardinal direction in name')
    return country


