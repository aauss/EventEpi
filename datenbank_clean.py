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
    """Takes a list of countries (from Ereginsdatenbank) and returns a set of cleaned country names"""

    country = str(country).strip(" ")
    card_dir = re.compile(r"(Süd|Nord|West|Ost)\s(\S*)")  # Matches cardinal directions and the string after it

    # Because someone used new lines in entries instead of comma to list countries
    country = re.sub(r'\n', ', ', country)

    # Because the line above adds one comma to much
    country = re.sub(r',,', ',', country)
    country = re.sub(r'\(.*\)', "", country)
    country.replace("&", "und")
    if "," in country:
        country.split(",")
    if type(country) != list:
        country.replace("_", " ")
    if type(country) != list and card_dir.match(country):
        try:
            country = card_dir.match(country)[1] + card_dir.match(country)[2].lower()
        except IndexError:
            print(card_dir.match, " has a cardinal direction but is not of the form 'Süd Sudan'")
    return country


def translate_abbreviation(to_translate, clean=False):
    """Takes a list of countries and/or abbreviations and translates the abbreviations to the full state name"""

    wikipedia_country_list = get_wiki_countries_df()
    to_return = []
    if clean:
        to_translate = clean_country_names(to_translate)
    if to_translate != list:
        to_translate = [to_translate]
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
