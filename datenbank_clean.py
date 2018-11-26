"""This python file should contain, if possible, function that are passed to pandas' apply function to clean
 the Ereignisdatenbank"""

import re
import warnings
import pandas as pd
from didyoumean import didyoumean
from datetime import datetime
from wiki_country_parser import get_wiki_countries_df


def edb_to_timestamp(date):
    """Transforms an unconverted string of a date to a timestamp"""

    date = str(date)
    date = date.replace('.', ' ')
    if re.match(r"\d\d\W\d\d\W\d\d\d\d", date) and "-" not in date:
        try:
            date = datetime.strptime(date, '%d %m %Y').strftime("%Y-%m-%d")
        except ValueError:
            warnings.warn('"{}" is not an existing date. Please change it'.format(date))
    return date


def clean_country_name(country):
    """

    Args:
        country:

    Returns:

    """
    """Takes a string of a country/ies (from Ereignisdatenbank) and returns cleaned country names"""

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
        return [clean_country_name(entry) for entry in country.split(",")]  # make a list out of many countr. entries
    else:
        return country


def translate_abbreviation(to_translate, to_clean=True, look_up=get_wiki_countries_df()):
    # TODO: Continue here after weekend
    """Takes a string or a list of countries and/or abbreviations and translates it to the full state name"""
    if isinstance(look_up, pd.DataFrame):
        wiki_countries_df = look_up
    else:
        wiki_countries_df = get_wiki_countries_df()
    if to_clean:
        if isinstance(to_translate, str):
            to_translate = clean_country_name(to_translate)
        elif isinstance(to_translate, list):
            to_translate = [clean_country_name(country) for country in to_translate]
    if isinstance(to_translate, str) and not re.findall(r"([^A-Z]+)", to_translate):
        for column in ["wiki_abbreviations", "inoff_abbreviations"]:
            for i, abbreviation in enumerate(wiki_countries_df[column]):
                if to_translate in abbreviation:
                    return wiki_countries_df["state_name_de"].tolist()[i]
                    break
    if type(to_translate) == list:
        return [translate_abbreviation(country) for country in to_translate]
    else:
        return to_translate


def match_country(country, look_up, translation):
    escaped = re.escape(country)
    found = list(filter(lambda x: re.findall(escaped, x), look_up))
    if len(found) == 1:
        return translation[look_up.index(found[0])]
    elif len(found) > 1:
        # Check if identity in ambiguous e.g Niger --> (Niger, Nigeria)
        identical = [found_name for found_name in found if found_name == country]
        if len(identical) == 1:
            return translation[look_up.index(identical[0])]
        else:
            return [translation[look_up.index(similar)] for similar in found]


def translate(to_translate, look_up=get_wiki_countries_df()):
    if isinstance(look_up, pd.DataFrame):
        wiki_countries_df = look_up
    else:
        wiki_countries_df = get_wiki_countries_df()

    continents = ["europa", "africa", "america", "australien", "asia"]
    state_name_de = wiki_countries_df["state_name_de"].tolist()
    full_state_name_de = wiki_countries_df["full_state_name_de"].tolist()
    translation = wiki_countries_df["translation_state_name"].tolist()
    match = None

    if isinstance(to_translate, str):
        match = match_country(to_translate, state_name_de, translation)
        if not match:
            match = match_country(to_translate, full_state_name_de, translation)
        if not match:
            match = match_country(to_translate, translation, translation)
        # If still no match
        if not match:
            did_u_mean = didyoumean.didYouMean(to_translate, state_name_de)
            if did_u_mean and (did_u_mean not in continents):
                match = translate(did_u_mean)
            else:
                match = to_translate
    elif isinstance(to_translate, list):
        match = [translate(country) for country in to_translate]
    return match

example_to_translate = ["Deutschland", "Delaware", ["Kongo", "China"], "Niger"]



