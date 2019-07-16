import requests
import pandas as pd

from typing import Union, List
from bs4 import BeautifulSoup
from itertools import product
from operator import itemgetter


def scrape(list_of_years: Union[int, List[int]] = None,
           months: list = None,
           headers: dict = None,
           proxy: dict = None) -> pd.DataFrame:
    """Scrapes WHO DONs given time range

    Args:
        list_of_years: An int or a list of years as int that shall be scraped
        months (optional): A list of months as str that shall be scraped
        headers (optional): Dictionary of headers for requests.get()
        proxy (optional): Dictionary of proxy settings for requests.get()

    Returns:
        DataFrame with all URLs of WHO DONs given specified time range

    """

    if list_of_years and not isinstance(list_of_years, list):
        list_of_years = [list_of_years]

    list_of_years = _get_urls_to_archives_per_year(list_of_years=list_of_years, proxy=proxy, headers=headers)
    all_links = _get_article_urls_per_years(list_of_years, list_of_months=months, proxy=proxy, headers=headers)
    urls = pd.DataFrame({'URL': all_links})
    return urls


def _get_urls_to_archives_per_year(list_of_years=None, proxy=None, headers=None):
    page = requests.get('http://www.who.int/csr/don/archive/year/en/', proxies=proxy, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    archive_years = soup.find('ul', attrs={'class': 'list'})
    years_links_html = archive_years.find_all('a')
    years_as_str = ['http://www.who.int' + link.get('href') for link in years_links_html]
    if list_of_years:
        list_of_years = list(map(str, list_of_years))
        return [link for link in years_as_str if _get_year_in_url(link) in list_of_years]
    else:
        return years_as_str


def _get_article_urls_per_years(years_links, list_of_months=None, proxy=None, headers=None):
    # Take a list of links to the annual archive and return a list of DON links of these years
    if list_of_months and not isinstance(list_of_months, list):
        list_of_months = [list_of_months]

    all_links = []

    for year_link in years_links:
        page_year = requests.get(year_link, proxies=proxy, headers=headers)
        soup_year = BeautifulSoup(page_year.content, 'html.parser')
        archive_year = soup_year.find('ul', attrs={'class': 'auto_archive'})
        daily_links = ['http://www.who.int' + link.get('href') for link in archive_year.find_all('a')]
        all_links.extend(daily_links)

    if list_of_months:
        cartesian_product = list(product(list_of_months, all_links))
        where_month_in_link = list(filter(lambda month_link_tuple:
                                          month_link_tuple[0] in month_link_tuple[1], cartesian_product))
        only_links_kept = list(map(itemgetter(1), where_month_in_link))
        all_links = only_links_kept
    return all_links


def _get_year_in_url(url):
    return ''.join(list(filter(str.isdigit, url)))
