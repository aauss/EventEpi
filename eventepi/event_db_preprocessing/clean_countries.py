import re
import warnings

from eventepi import my_utils


def clean_countries(event_db):
    """Cleans country names in incident database

    Removes format errors for country names that were found during
    exploration of incident database. Also, splits single rows with
    several country names to several rows with a single country entry
    and where every other entry of the row is duplicated form the
    former row.

    Args (pd.DataFrame):
        event_db: Incident database without cleaned country names

    Returns (pd.DataFrame):
        Incident database with cleaned country names

    """
    event_db["country_edb"] = event_db["country_edb"].apply(_clean_country_str)
    event_db = my_utils.split_strings_at_comma_and_distribute_to_new_rows(event_db, 'country_edb')
    return event_db


def _clean_country_str(country):
    if isinstance(country, str):
        country = re.sub(r'\s*\n\s*', ', ', country)  # Comma instead of new line
        country = re.sub(r',,', ',', country)  # Because the line above adds one comma to much
        country = re.sub(r'\(.*\)', "", country)  # Remove parentheses and their content
        country = country.replace("&", "und")
        country = country.replace("_", " ")
        country = country.strip()
        country = _correct_wrong_use_of_cardinal_directions(country)
    return country


def _correct_wrong_use_of_cardinal_directions(country):
    if "korea" in country.lower():
        return _handle_korea(country)
    else:
        cardinal_dir = re.compile(r"(Süd|Nord|West|Ost)\s(\S*)")
        if cardinal_dir.match(country):
            try:
                country = cardinal_dir.match(country)[1] + cardinal_dir.match(country)[2].lower()
            except IndexError:
                print(country)
                warnings.warn('Problems with processing country string with cardinal direction in name')
        return country


def _handle_korea(country):
    # Except for Korea, in https://de.wikipedia.org/wiki/Liste_der_Staaten_der_Erde,
    # cardinal direction and country name are written separately
    if "nord" in country.lower():
        return "Nord Korea"
    elif "süd" in country.lower():
        return "Süd Korea"
    else:
        return country


