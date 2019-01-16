import requests
import re
import pandas as pd
import sys

from bs4 import BeautifulSoup


def scrape_wiki_countries():
    """Scrapes German Wikipedia article of list of states of the earth and returns dict with entries."""
    soup = _get_soup()
    wiki_dict = {"state_name_de": [],
                 "full_state_name_de": [],
                 "translation_state_name": [],
                 "iso_three_abbreviation": [],
                 "iso_two_abbreviation": []}
    for i in range(len(soup)):
        try:
            state_name_de = _extract_column_from_soup(soup[i][0], True)
            wiki_dict["state_name_de"].append(state_name_de)

            full_state_name_de = _extract_column_from_soup(soup[i][1])
            wiki_dict["full_state_name_de"].append(full_state_name_de)

            translation_state_name = _extract_column_from_soup(soup[i][10])
            wiki_dict["translation_state_name"].append(translation_state_name)

            wiki_dict['iso_three_abbreviation'].append(_extract_abbreviation_from_soup(soup[i][7]))
            wiki_dict['iso_two_abbreviation'].append(_extract_abbreviation_from_soup(soup[i][8]))
        except IndexError as e:  # Because header and footer are part of the table, soup operations don't work
            # Except that the first and last entry fail
            if i not in [0, 213]:
                print("Entry {} failed unexpected because of {}".format(i, e))
    wiki_df = pd.DataFrame.from_dict(wiki_dict)
    return wiki_df


def _get_soup():
    req = requests.get("https://de.wikipedia.org/wiki/Liste_der_Staaten_der_Erde")
    soup = BeautifulSoup(req.content, "html.parser")

    # Find table with of all countries
    table_soup = soup.find("table", class_="wikitable sortable zebra").find("tbody")
    country_soup = table_soup.find_all("tr")  # Get entries of countries form table

    # Extract table entries from country entry
    country_entry_soup = [country_soup[i].find_all('td') for i in range(len(country_soup))]
    return country_entry_soup


def _extract_column_from_soup(soup, is_state_name_de=False):
    extracted_column = soup.text.replace("\n", "")
    if is_state_name_de:
        extracted_column = re.sub(r"((mit)|(ohne)).*", "", extracted_column)
        extracted_column = extracted_column.replace("\xad", "")  # Remove soft hyphen used in "Zentralafr. Rep".
    extracted_column = _reorder_words_in_names_with_comma(extracted_column)
    return extracted_column


def _extract_abbreviation_from_soup(soup):
    abbreviation = soup.text.replace("\n", "")
    return abbreviation


def _remove__footnotes_dashes_and_brackets(wiki_df):
    wiki_df_without_brackets = wiki_df.applymap(lambda x: re.sub(r"\(.*\)", " ", str(x)))
    wiki_df_wihout_footnotes = wiki_df_without_brackets.applymap(lambda x: re.sub(r'\[\d*\]', "", x))
    wiki_df_without_dash = wiki_df_wihout_footnotes.applymap(_replace_wiki_dash_with_none)
    return wiki_df_without_dash


def _replace_wiki_dash_with_none(string):
    dash = u"\u2014"  # Used dash for missing entry in Wikipedia table
    if dash == string:
        return None
    else:
        return string


def _reorder_words_in_names_with_comma(country_name):
    """Formats such that Congo, Republik of (Brazzaville) --> Republik of Congo
    """
    if "," in country_name:
        # If there is a comma, switch order to yield a more common abbreviation: Korea, Nord --> Nord Korea
        matched = re.match(r"([A-Za-z]*), (.*)", country_name)  # Extract capital letters
        try:
            country_name = matched[2] + " " + matched[1]  # Patch capital letters together
            if country_name[0].islower():
                country_name = [word.capitalize() if word.islower() else word
                                for word in country_name.split(" ")]  # the Gambia -> The Gambia
                country_name = " ".join(country_name)
        except TypeError:
            print(country_name, " could not be formatted by {}()".format(sys._getframe().f_code.co_name))
    country_name = re.sub(r'(\s{2,})', ' ', country_name)  # The lines above sometimes includes unnecessary spaces
    return country_name


