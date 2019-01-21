import requests
import pandas as pd
from bs4 import BeautifulSoup


def scrape_wikipedia_countries():
    """Scrapes German Wikipedia article of list of states of the earth and returns dict with entries."""
    soup = _get_soup()
    wiki_dict = {"state_name_de": [],
                 "full_state_name_de": [],
                 "translation_state_name": [],
                 "iso_three_abbreviation": [],
                 "iso_two_abbreviation": []}
    for i in range(len(soup)):
        try:
            state_name_de = soup[i][0].text.replace("\n", "")
            wiki_dict["state_name_de"].append(state_name_de)

            full_state_name_de = soup[i][1].text.replace("\n", "")
            wiki_dict["full_state_name_de"].append(full_state_name_de)

            translation_state_name = soup[i][10].text.replace("\n", "")
            wiki_dict["translation_state_name"].append(translation_state_name)

            wiki_dict['iso_three_abbreviation'].append(soup[i][7].text.replace("\n", ""))
            wiki_dict['iso_two_abbreviation'].append(soup[i][8].text.replace("\n", ""))
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
