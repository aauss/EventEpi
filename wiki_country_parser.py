import requests
import re
import pickle
import os
import pandas as pd
import sys
from bs4 import BeautifulSoup


def format_country_with_comma_and_parathesis(country_name):
    """
    Formats such that Congo, Republik of (Brazzaville) --> Republik of Congo
    Returns:
        formatted country name
    """
    country_name = re.sub(r"\(.*\)", " ", country_name)  # Delete content in parenthesis since not relevant here
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


def scrape_wiki_countries():
    """Scrapes German Wikipedia article of list of states of the earth and returns dict with entries."""

    req = requests.get("https://de.wikipedia.org/wiki/Liste_der_Staaten_der_Erde")
    soup = BeautifulSoup(req.content, "html.parser")

    # Find table with of all countries
    parsed_soup = soup.find("table", class_="wikitable sortable zebra").find("tbody")
    parsed_soup = parsed_soup.find_all("tr")  # Get entries of countries form table
    amount_countries = len(parsed_soup)

    # Extract table entries from country entry
    parsed_soup = [parsed_soup[i].find_all('td')for i in range(amount_countries)]

    wiki_dict = {"state_name_de": [],
                 "full_state_name_de": [],
                 "capital_de": [], "translation_state_name": [],
                 "wiki_abbreviations": []}
    dash = u"\u2014"  # Used dash for missing entry in Wikipedia table
    regex = re.compile(r"\[\d*\]")  # To remove footnotes in the names
    for i in range(amount_countries):
        try:
            # state_name_de
            state_name_de = regex.sub("", parsed_soup[i][0].text.replace("\n", "")
                                      .replace("\xad", ""))  # Remove soft hyphen used in "Zentralafr. Rep".
            state_name_de = re.sub(r"((mit)|(ohne)).*", "", state_name_de)
            state_name_de = format_country_with_comma_and_parathesis(state_name_de)
            # Remove additional information that are note the state name

            wiki_dict["state_name_de"].append(state_name_de)

            # full_state_name_de
            full_state_name_de = regex.sub("", parsed_soup[i][1].text).replace("\n", "")
            full_state_name_de = format_country_with_comma_and_parathesis(full_state_name_de)
            wiki_dict["full_state_name_de"].append(full_state_name_de)

            # capital_de
            wiki_dict["capital_de"].append(regex.sub("", parsed_soup[i][2].text).replace("\n", ""))

            # translation_state_name
            translation_state_name = regex.sub("", parsed_soup[i][10].text).replace("\n", "")
            translation_state_name = format_country_with_comma_and_parathesis(translation_state_name)
            wiki_dict["translation_state_name"].append(translation_state_name)

            # Also removes new lines. Column 7 and 8 are long and short official abbreviations for the countries
            # TODO: Take out references
            list_abbreviation = [parsed_soup[i][7].text.replace("\n", ""), parsed_soup[i][8].text.replace("\n", "")]

            # Remove empty abbrev. E.g. ["Abchasien", ["ABC", "-"], "Abkhazia"] --> ["Abchasien", ABC", "Abhkazia"]
            list_abbreviation = list(filter(lambda x: x not in ["", dash], list_abbreviation))
            if len(list_abbreviation) > 1:
                wiki_dict["wiki_abbreviations"].append(list_abbreviation)
            else:
                # When after removal of empty entries no abbrev. remains, enter a single dash
                wiki_dict["wiki_abbreviations"].append(dash)
        except IndexError as e:  # Because header and footer are part of the table, soup operations don't work
            # Except that the first and last entry fail
            if i not in [0, 213]:
                print("Entry {} failed unexpected because of {}".format(i, e))
    return pd.DataFrame.from_dict(wiki_dict)


def abbreviate_country(country_name):
    """Abbreviates entries of list of country names

    Example: United Kingdom --> UK
    """
    abbreviation = None
    if len(re.findall(r"([A-Z|Ä|Ö|Ü])", country_name)) > 1:
        abbreviation = "".join(re.findall(r"([A-Z|Ä|Ö|Ü])", country_name))
    return abbreviation


def abbreviate_df(wikipedia_country_df, columns=["state_name_de", "full_state_name_de", "translation_state_name"]):
    """Search for names that might have abbreviations. If they consist of two or more words that start with a capital
    letter, it makes an abbreviation out of it
    """
    abbreviations = [list(map(abbreviate_country, wikipedia_country_df[column].tolist())) for column in columns]
    abbreviations = [list(a) for a in zip(*abbreviations)]
    abbreviations = [list(filter(None, abb)) for abb in abbreviations if str(abb) != 'None']  # Removes Nones
    abbreviations = list(map(lambda x: list(set(x)) if len(x) > 0 else "-", abbreviations))  # Removes redundancy
    wikipedia_country_df["inoff_abbreviations"] = abbreviations
    return wikipedia_country_df


def get_wiki_countries_df(use_pickle=True):
    # scripts_path = inspect.getfile(inspect.currentframe())
    pickle_path = os.path.join("pickles", "wiki_countries_df.p")
    if os.path.exists(pickle_path) and use_pickle:
        wiki_countries_df = pickle.load(open(pickle_path, "rb"))
    else:
        print("Pickle not found. Start scraping..")
        wiki_countries_df = scrape_wiki_countries()
        wiki_countries_df = abbreviate_df(wiki_countries_df)
        print("...scraping complete.")
        try:
            pickle.dump(wiki_countries_df, open(pickle_path, "wb"))
        except FileNotFoundError:
            print("Wikipedia country data frame needed to be scraped unexpectedly. Don't execute to often")
    return wiki_countries_df
