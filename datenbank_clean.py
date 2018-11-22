"""This python file should contain, if possible, function that are passed to pandas' apply function to clean
 the Ereignisdatenbank"""

import re
import warnings
from datetime import datetime
from wiki_country_parser import get_wiki_countries_df


def edb_to_timestamp(date):
    """Transforms a list of unconverted string to timestamp"""

    date = str(date)
    date = date.replace('.', ' ')
    if re.match(r"\d\d\W\d\d\W\d\d\d\d", date) and "-" not in date:
        try:
            date = datetime.strptime(date, '%d %m %Y').strftime("%Y-%m-%d")
        except ValueError:
            warnings.warn('"{}" is not an existing date. Please change it'.format(date))
    return date


def clean_country_names(country):
    """Takes a list of countries (from Ereignisdatenbank) and returns a set of cleaned country names"""

    country = str(country)
    card_dir = re.compile(r"(Süd|Nord|West|Ost)\s(\S*)")  # Matches cardinal directions and the string after it
    country = str(country).strip(" ")
    country = re.sub(r'\n', ', ', country)
    country = re.sub(r',,', ',', country)  # Because the line above adds one comma to much
    country = re.sub(r'\(.*\)', "", country)
    country = country.replace("&", "und")
    country = country.replace("_", " ")
    if card_dir.match(country):
        try:
            country = card_dir.match(country)[1] + card_dir.match(country)[2].lower()
        except IndexError:
            print(card_dir.match, " has a cardinal direction but is not of the form 'Süd Sudan'")
    if "," in country:
        return [clean_country_names(entry) for entry in country.split(",")]
    else:
        return country


def translate_abbreviation(to_translate, to_clean=True):
    # TODO: Continue here after weekendgi
    """Takes a string or a list of countries and/or abbreviations and translates it to the full state name"""

    wikipedia_country_list = get_wiki_countries_df()
    to_return = []
    if to_translate != list:
        to_translate = [to_translate]
    if to_clean and type(to):
        to_translate = [clean_country_names(entry) for entry in to_translate]
    for potential_abbreviation in to_translate:
        if type(potential_abbreviation) == str and not re.findall(r"([^A-Z]+)", potential_abbreviation):

            # First check the official abrev. than the self created ones e.g. VAE for the Emirates
            for column in ["wiki_abbreviations", "inoff_abbreviations"]:
                for i, abbreviation in enumerate(wikipedia_country_list[column]):
                    if potential_abbreviation in abbreviation:
                        to_return.append(wikipedia_country_list["state_name_de"].tolist()[i])

        elif type(potential_abbreviation) == list:
            list_entry = [translate_abbreviation(nested_entry) for nested_entry in potential_abbreviation]
            flattened = [entry for sublist in list_entry for entry in sublist]
            to_return.append(flattened)
        else:
            to_return.append(potential_abbreviation)
    return to_return
