import requests
import urllib.request
import pandas as pd
from bs4 import BeautifulSoup

from utils import my_utils


def scrape(list_of_years=None, months=None, headers=None, proxy=None):
    # Scrapes the WHO DONs using the WHO DON scraping functions and returns the links to these DONs
    if not isinstance(list_of_years, list):
        list_of_years = [list_of_years]

    if not _connection_is_possible() and proxy is not None:
        headers = my_utils.load_rki_header_and_proxy_dict()['headers']
        proxy = my_utils.load_rki_header_and_proxy_dict()['proxy']

    list_of_years = _get_links_by_year(list_of_years=list_of_years, proxy=proxy, headers=headers)
    all_links = _get_links_per_year(list_of_years, list_of_months=months, proxy=proxy, headers=headers)
    urls = pd.DataFrame({'URL': all_links})
    return urls


def _get_links_by_year(list_of_years=None, proxy=None, headers=None):
    # Returns (all) the anual links of the WHO DONs

    page = requests.get('http://www.who.int/csr/don/archive/year/en/', proxies=proxy, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    archive_years = soup.find('ul', attrs={'class': 'list'})
    years_links_html = archive_years.find_all('a')
    if list_of_years:
        return ['http://www.who.int' + link.get('href') for link in years_links_html if
                any(year in link for year in list_of_years)]
    else:
        return ['http://www.who.int' + link.get('href') for link in years_links_html]


def _get_links_per_year(years_links, list_of_months=None, proxy=None, headers=None):
    # Take a list of links to the annual archive and return a list of DON links of these years

    all_links = []

    for year_link in years_links:
        page_year = requests.get(year_link, proxies=proxy, headers=headers)
        soup_year = BeautifulSoup(page_year.content, 'html.parser')
        archive_year = soup_year.find('ul', attrs={'class': 'auto_archive'})
        daily_links = ['http://www.who.int' + link.get('href') for link in archive_year.find_all('a')]
        all_links.extend(daily_links)

    if list_of_months:
        all_links = [link for link in all_links if
                     any(month in link for month in map(lambda s: s.lower(), list_of_months))]
    return all_links


def _connection_is_possible():
    try:
        urllib.request.urlopen('https://www.rki.de/DE/Home/homepage_node.html', timeout=4)
        return True
    except urllib.request.URLError:
        return False

