"""This python file should contain, if possible, function that are passed to pandas' apply function to clean
 the Ereignisdatenbank"""

import re
import os
import warnings
import pandas as pd
from didyoumean import didyoumean
from datetime import datetime
from .wiki_country_parser import get_wiki_countries_df
from .wiki_disease_parser import get_wiki_disease_df


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


def _complete_partial_words(to_complete, list_of_possible_corrections_de, list_of_possible_corrections_en):
    # Takes a list of correctly written en/ger disease names and tries to complete them: Ebola -> Ebolavirus
    match = [s for s in list_of_possible_corrections_de if s.lower().startswith(to_complete.lower())]
    if not match:
        match = [s for s in list_of_possible_corrections_en if s.lower().startswith(to_complete.lower())]
    if match:
        return match[0]
    else:
        return to_complete


def translate_disease_name(disease):
    # TODO: Write this into a wiki_disease_parser
    # Initialization of databases
    disease_translation_df, disease_code_df = get_wiki_disease_df()
    disease_db_en = disease_translation_df['itemLabel_EN']
    disease_db_de = [d for d in disease_translation_df['itemLabel_DE'] if isinstance(d, str)]

    # Translate
    disease = str(disease)
    disease = disease.strip()
    match_for_abbreviation = re.match(r'[A-Z]{2,}', disease)

    if match_for_abbreviation:
        index_for_matched_abbreviation = disease_code_df[disease_code_df['Code'] == match_for_abbreviation[0]]

        if not index_for_matched_abbreviation.empty:
            disease = index_for_matched_abbreviation['TypeName'].tolist()[0]

    if disease in disease_db_de:
        return disease_translation_df['itemLabel_EN'].loc[disease_translation_df['itemLabel_DE'] == disease].tolist()[0]
    elif disease in disease_db_en:
        return disease
    elif ',' in disease:
        # Did not translate because it is a concatenation of disease names
        disease = disease.split(',')
        disease = [entry.strip() for entry in disease]
        return [translate_disease_name(entry) for entry in disease]

    else:
        # If typo occurred
        did_u_mean = didyoumean.didYouMean(disease, disease_db_de)
        if did_u_mean in disease_db_en:
            return did_u_mean
        elif did_u_mean in disease_db_de:
            return disease_translation_df['itemLabel_EN'].loc[disease_translation_df['itemLabel_DE']
                                                              == did_u_mean].tolist()[0]
        else:
            complete_disease_name = _complete_partial_words(disease, disease_db_de, disease_db_en)
            if disease != complete_disease_name:
                return translate_disease_name(complete_disease_name)
            else:
                return disease


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
            return [clean_country_name(entry) for entry in country.split(",")]
        else:
            return country
    elif isinstance(country, list):
        return [clean_country_name(entry) for entry in country]


def _translate_abbreviation(to_translate, to_clean=True, look_up=get_wiki_countries_df()):
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
    if type(to_translate) == list:
        return [_translate_abbreviation(country) for country in to_translate]
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


def translate_geonames(to_translate, look_up=get_wiki_countries_df()):
    if isinstance(look_up, pd.DataFrame):
        wiki_countries_df = look_up
    else:
        wiki_countries_df = get_wiki_countries_df()
    to_translate = _translate_abbreviation(clean_country_name(to_translate))
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
                    match = translate_geonames(did_u_mean)
                except RecursionError:
                    print(did_u_mean, "caused recursion error")
            else:
                match = to_translate
    elif isinstance(to_translate, list):
        match = [translate_geonames(country) for country in to_translate]
    return match


def get_cleaned_edb(clean=[(edb_to_timestamp, [7, 8, 10, 16, 19, 22, 25, 34]),
                           (translate_geonames, [3, 4]),
                           (translate_disease_name, [6])], reduced=True, condensed=False, unprocessed=False):
    """

    Returns:
        pd.DataFrame: formatted edb
        if reduce drop columns unnecessary for analysis
    """
    if not isinstance(clean, list):
        clean = [clean]
    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, 'data', 'edb.csv')
    edb = pd.read_csv(path, sep=";")
    edb = edb.dropna(how="all").reset_index(drop=True)
    edb.columns = list(map(lambda x: x.strip(), edb.columns))
    if unprocessed:
        return edb
    for funct, columns in clean:
        edb.iloc[:, columns] = edb.iloc[:, columns].applymap(funct)
    if reduced or condensed:
        to_drop = edb.iloc[:, [0, 1, 2, 5, 7, 8, 27, 28, 29, 30, 31, 32, 33, 34, 35]].columns.tolist()
        return edb.drop(to_drop, axis=1)
    else:
        return edb
