import requests
import os
import pickle
import pandas as pd
from bs4 import BeautifulSoup

from .utils.my_utils import try_if_connection_is_possible
from .utils.text_from_url import extract_cleaned_text_from_html_webpage
from .annotator import geonames, dates, keywords, cases, create_annotated_database


def get_annotated_2018_whos(entity_funcs_and_params=None):
    path = os.path.join(os.path.dirname(__file__), "pickles", "parsed_who_df.p")
    if not os.path.isfile(path):
        all_links = scrape(years=['2018'])
        extracted = list(map(lambda x: extract_cleaned_text_from_html_webpage(x), all_links))
        if entity_funcs_and_params is None:
            entity_funcs_and_params = [(geonames, {"raw": True}),
                                       (cases, {"raw": True}),
                                       (dates, {"raw": True}),
                                       keywords]
        parsed_whos_df = pd.DataFrame.from_dict(
            create_annotated_database(extracted, entity_funcs_and_params=entity_funcs_and_params))
        pickle.dump(parsed_whos_df, open(path, "wb"))
    else:
        return pickle.load(open(path, "rb"))


def scrape(years=None, months=None, headers=None, proxies=None):
    # Scrapes the WHO DONs using the WHO DON scraping functions and returns the links to these DONs

    if not try_if_connection_is_possible() and proxies is not None:
        headers = load_params()["headers"]
        proxies = load_params()["proxies"]

    years = get_links_by_year(list_of_years=years, proxies=proxies, headers=headers)
    all_links = get_links_per_year(years, list_of_months=months, proxies=proxies, headers=headers)
    return all_links


def get_links_by_year(list_of_years=None, proxies=None, headers=None):
    # Returns (all) the anual links of the WHO DONs

    page = requests.get('http://www.who.int/csr/don/archive/year/en/', proxies=proxies, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    archiv_years = soup.find('ul', attrs={'class': 'list'})
    years_links_html = archiv_years.find_all('a')
    if list_of_years:
        return ['http://www.who.int' + link.get('href') for link in years_links_html if
                any(year in link for year in list_of_years)]
    else:
        return ['http://www.who.int' + link.get('href') for link in years_links_html]


def get_links_per_year(years_links, list_of_months=None, proxies=None, headers=None):
    # Take a list of links to the annual archive and return a list of DON links of these years

    all_links = []

    for year_link in years_links:
        page_year = requests.get(year_link, proxies=proxies, headers=headers)
        soup_year = BeautifulSoup(page_year.content, 'html.parser')
        archive_year = soup_year.find('ul', attrs={'class': 'auto_archive'})
        daily_links = ['http://www.who.int' + link.get('href') for link in archive_year.find_all('a')]
        all_links.extend(daily_links)

    if list_of_months:
        all_links = [link for link in all_links if
                     any(month in link for month in map(lambda s: s.lower(), list_of_months))]
    return all_links


def load_params():
    path = os.path.join("pickles", "scraping_params.p")
    return pickle.load(open(path, "rb"))

