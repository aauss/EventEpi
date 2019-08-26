import requests
import re
import pandas as pd

from functools import partial
from typing import Union, Tuple
from tqdm import tqdm_notebook as tqdm

from nlp_surveillance.scraper.text_extractor import get_html_from_promed_url


def scrape(year_range: Union[Tuple[int, int], int] = None,
           date_range: Tuple[str, str] = None,
           proxy=None) -> pd.DataFrame:
    """Scrapes ProMED Mail articles given a time range

    Args:
        year_range (optional): Years to scrape as int. Either one year or year range given tuple of two ints
        date_range (optional): Date range for scraping. Tuple contains from- and to-date as string (mm/dd/YYYY)
        proxy (optional): A dict for proxy values. E.g. {'http_proxy': '<YOUR_PROXY>'}

    Returns:
        DataFrame with all ProMED Mail URLs for the given year/date range
    """

    if isinstance(year_range, int):
        from_date = f"01/01/{year_range}"
        to_date = f"12/31/{year_range}"
    elif isinstance(year_range, tuple):
        from_date = f"01/01/{year_range[0]}"
        to_date = f"12/31/{year_range[1]}"
    else:
        from_date = date_range[0]
        to_date = date_range[1]
    from_date = _correct_to_earliest_allowed_date(from_date)
    ids = _get_article_ids_per_year(from_date=from_date,
                                    to_date=to_date,
                                    proxy=proxy)
    parsed = [f'https://www.promedmail.org/post/{str(id_)}' for id_ in ids]
    urls = pd.DataFrame({'URL': parsed})
    return urls


def _correct_to_earliest_allowed_date(from_date: str) -> str:
    corrected_date = max(pd.Timestamp(day=int(from_date[3:5]), month=int(from_date[0:2]), year=int(from_date[6:11])),
                         pd.Timestamp(day=20, month=8, year=1994))
    return corrected_date.strftime('%m/%d/%Y')


def _get_article_ids_per_year(from_date, to_date, proxy=None) -> list:
    get_content_of_search_page = partial(_get_content_of_search_page,
                                         from_date=from_date,
                                         to_date=to_date,
                                         proxy=proxy)

    content_first_page = get_content_of_search_page(page_num=2)
    try:
        max_page_num = re.search(r'Page -?\d+ of (\d+)', content_first_page)[1]
    except TypeError:
        # If there is only one result page, we won't find the regex pattern above
        max_page_num = 1
    ids = []
    for i in tqdm(range(2, int(max_page_num) + 2)):
        ids_of_pages = re.findall(r'id(\d+)', get_content_of_search_page(page_num=i))
        if ids_of_pages:
            ids.extend(ids_of_pages)
        else:
            # After around 200 pages, ProMED is returning an error message instead of the next page of URLs
            break
    if ids and int(max_page_num) > 199:
        last_id_before_error = ids[-1]
        ids.extend(_recursively_call_with_last_scraped_date_as_new_to_date(last_id_before_error,
                                                                           from_date,
                                                                           proxy))
    return ids


def _get_content_of_search_page(from_date: str, to_date: str, page_num: int, proxy: dict) -> str:
    return (requests
            .get(f'https://www.promedmail.org/ajax/runSearch.php?'
                 f'pagenum={page_num}&'
                 f'search=&date1={from_date}&'
                 f'date2={to_date}',
                 proxies=proxy)
            .content
            .decode('utf-8')
            )


def _recursively_call_with_last_scraped_date_as_new_to_date(last_id_before_error: str,
                                                            from_date: str,
                                                            proxy: dict):
    html = get_html_from_promed_url(last_id_before_error, proxy=proxy)

    published_date = re.search(r'Published Date:.* (\d\d\d\d-\d\d-\d\d)', html)[1]
    last_date_as_mm_dd_yyyy = f'{published_date[5:7]}/{published_date[8:10]}/{published_date[0:4]}'
    return _get_article_ids_per_year(from_date=from_date,
                                     to_date=last_date_as_mm_dd_yyyy,
                                     proxy=proxy)

