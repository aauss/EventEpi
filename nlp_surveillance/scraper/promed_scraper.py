import requests
import re
import pandas as pd
from functools import partial
from tqdm import tqdm_notebook as tqdm


from nlp_surveillance.scraper.text_extractor import get_html_from_promed_url


def scrape(from_date: str, to_date: str, proxy=None) -> pd.DataFrame:
    # Date in the form mm/dd/YYYY
    ids = _get_article_ids_per_year(from_date=from_date,
                                    to_date=to_date,
                                    proxy=proxy)
    parsed = [f'https://www.promedmail.org/post/{str(id_)}' for id_ in ids]
    urls = pd.DataFrame({'URL': parsed})
    return urls


def _get_article_ids_per_year(from_date='01/01/2018', to_date='12/31/2018', proxy=None) -> list:
    # date in the format mm/dd/YYYY
    # adjust from_date to valid smallest date
    from_date = _correct_to_earliest_allowed_date(from_date)
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
            # After around 200 pages, promed is returning an error message instead of the next page of URLs
            break
    if ids and int(max_page_num) > 199:
        last_id_before_error = ids[-1]
        ids.extend(_recursively_call_with_last_scraped_date_as_new_to_date(last_id_before_error, from_date))
    return ids


def _correct_to_earliest_allowed_date(from_date: str) -> str:
    corrected_date = max(pd.Timestamp(day=int(from_date[3:5]), month=int(from_date[0:2]), year=int(from_date[6:11])),
                         pd.Timestamp(day=20, month=8, year=1994))
    return corrected_date.strftime('%m/%d/%Y')


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


def _recursively_call_with_last_scraped_date_as_new_to_date(last_id_before_error, from_date: str):
    html = get_html_from_promed_url(last_id_before_error)
    published_date = re.search(r'Published Date:.* (\d\d\d\d-\d\d-\d\d)', html)[1]
    last_date_as_mm_dd_yyyy = f'{published_date[5:7]}/{published_date[8:10]}/{published_date[0:4]}'
    return _get_article_ids_per_year(from_date=from_date, to_date=last_date_as_mm_dd_yyyy)

