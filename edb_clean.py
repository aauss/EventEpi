"""This python file should contain, if possible, function that are passed to pandas' apply function to clean
 the Ereignisdatenbank"""

import re
import warnings
import pandas as pd
from didyoumean import didyoumean
from datetime import datetime
from wiki_country_parser import get_wiki_countries_df


def _add_zero(x):
    """Adds a zero if day or month only have a single value

    Args:
        x: a day or month as string

    Returns:
        day/month as string in the format dd/mm
    """
    if len(x) == 1:
        x = "0" + x
    return x


def edb_to_timestamp(date):
    """Transforms an unconverted string of a date to a timestamp"""

    date = str(date)
    date_matched = re.match(r"(\d{1,2})\D(\d{1,2})\D(\d{4})", date)
    if date_matched and "-" not in date:
        day = _add_zero(date_matched[1])
        month = _add_zero(date_matched[2])
        year = date_matched[3]
        try:
            date_try_format = day + " " + month + " " + year
            date = datetime.strptime(date_try_format, '%d %m %Y').strftime("%Y-%m-%d")
        except ValueError:
            warnings.warn('"{}" is not an existing date. Please change it'.format(date))
    return date


def clean_country_name(country):
    """Takes a string of a country/ies (from edb) and returns cleaned country names

    Args:
        country:

    Returns:

    """

    if isinstance(country, str):
        card_dir = re.compile(r"(Süd|Nord|West|Ost)\s(\S*)")  # Matches cardinal directions and the string after it
        country = str(country).strip(" ")
        country = re.sub(r'\n', ', ', country)
        country = re.sub(r',,', ',', country)  # Because the line above adds one comma to much
        country = re.sub(r'\(.*\)', "", country)
        country = country.replace("&", "und")
        country = country.replace("_", " ")
        if card_dir.match(country):
            try:
                if "korea" not in country.lower():
                    country = card_dir.match(country)[1] + card_dir.match(country)[2].lower()
            except IndexError:
                print(card_dir.match, " has a cardinal direction but is not of the form 'Süd Sudan'")
        if "," in country:
            return [clean_country_name(entry) for entry in country.split(",")]  # make a list out of many countr. entries
        else:
            return country
    elif isinstance(country, list):
        return [clean_country_name(entry) for entry in country]


def translate_abbreviation(to_translate, to_clean=True, look_up=get_wiki_countries_df()):
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


def _match_country(country, look_up, translation):
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
    to_translate = translate_abbreviation(clean_country_name(to_translate))
    continents = ["europa", "afrika", "amerika", "australien", "asien"]
    state_name_de = wiki_countries_df["state_name_de"].tolist()
    full_state_name_de = wiki_countries_df["full_state_name_de"].tolist()
    translation = wiki_countries_df["translation_state_name"].tolist()
    match = None

    if isinstance(to_translate, str):
        match = _match_country(to_translate, state_name_de, translation)
        if not match:
            match = _match_country(to_translate, full_state_name_de, translation)
        elif not match:
            match = _match_country(to_translate, translation, translation)
        # If still no match
        if not match:
            did_u_mean = didyoumean.didYouMean(to_translate, state_name_de)
            if did_u_mean and (did_u_mean.lower() not in continents):
                try:
                    match = translate(did_u_mean)
                except RecursionError:
                    print(did_u_mean, "caused recursion error")
            else:
                match = to_translate
    elif isinstance(to_translate, list):
        match = [translate(country) for country in to_translate]
    return match
