import requests
from bs4 import BeautifulSoup


def get_links_by_year(list_of_years=None, proxies={'http': 'http://fw-bln.rki.local:8020'}):
    """Returns (all) the anual links of the WHO DONs

    list_of_years -- a list of years (YYYY format) you want to parse (default None)
    proxies -- the proxy to use while scraping (default {'http': 'http://fw-bln.rki.local:8020'})
    """
    page = requests.get('http://www.who.int/csr/don/archive/year/en/', proxies=proxies)
    soup = BeautifulSoup(page.content, 'html.parser')
    archiv_years = soup.find('ul', attrs={'class': 'list'})
    years_links_html = archiv_years.find_all('a')
    if list_of_years:
        return ['http://www.who.int' + link.get('href') for link in years_links_html if
                any(year in link for year in list_of_years)]
    else:
        return ['http://www.who.int' + link.get('href') for link in years_links_html]


def get_links_per_year(years_links, list_of_months=None, proxies={'http': 'http://fw-bln.rki.local:8020'}):
    """Take a list of links to the annual archive and return a list of DON links of these years

    years_links -- a list of links of the anual archive to parse
    list_of_months -- a list of months (MMM* format) you want to parse (default None)
    proxies -- the proxy to use while scraping (default {'http': 'http://fw-bln.rki.local:8020'})
    """
    all_links = []

    for year_link in years_links:
        page_year = requests.get(year_link, proxies=proxies)
        soup_year = BeautifulSoup(page_year.content, 'html.parser')
        archive_year = soup_year.find('ul', attrs={'class': 'auto_archive'})
        daily_links = ['http://www.who.int' + link.get('href') for link in archive_year.find_all('a')]
        all_links.extend(daily_links)

    if list_of_months:
        all_links = [link for link in all_links if
                     any(month in link for month in map(lambda s: s.lower(), list_of_months))]
    return all_links


headers = {
    'User-Agent': 'Auss Abbood, www.rki.de',
    'From': 'abbooda@rki.de'
}


def scrape(years=None,
           months=None,
           num_last_reports=None,
           headers=None,
           proxies={'http': 'http://fw-bln.rki.local:8020'}):
    """Scrapes the WHO DONs using the WHO DON scraping functions and returns the links to these DONs

    years -- a list of strings of years in the format YYYY to be scraped
    months -- a list of strings of months in the format MMM* to be scraped
    num_list_reports -- an integer to specify how many of the last reports should be scraped.
    can be combined with the specification of year and/or month
    headers -- use a header for scraping
    proxies -- the proxy to use while scraping (default {'http': 'http://fw-bln.rki.local:8020'})
    """
    years = get_links_by_year(list_of_years=years, proxies=proxies)
    all_links = get_links_per_year(years, list_of_months=months, proxies=proxies)
    return all_links
