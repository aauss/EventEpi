import requests
import re
import pandas as pd

from utils import my_utils


def scrape(list_of_years, proxy=None):
    # If from_year and to_year are the same, all URLs of of all articles of this year are scraped
    ids = _get_article_ids_per_year(from_year=min(list_of_years),
                                    to_year=max(list_of_years),
                                    proxy=proxy)
    parsed = [f'https://www.promedmail.org/post/{str(id_)}' for id_ in ids]
    return parsed


def _get_article_ids_per_year(from_year='2018', to_year='2018', proxy=None):
    _get_content_of_search_page = lambda x: (requests.get(f'https://www.promedmail.org/ajax/runSearch.php?'
                                                          f'pagenum={x}&kwby1=summary&'
                                                          f'search=&date1=01/01/{from_year}&'
                                                          f'date2=01/01/2019&feed_id=1&submit=next',
                                                          proxies=proxy)
                                             .content
                                             .decode('utf-8'))

    content = _get_content_of_search_page(0)
    max_page_num = re.search(r'Page \d+ of (\d+)', content)[1]
    ids_of_pages = [re.findall(r'id\d+', _get_content_of_search_page(i))
                    for i in range(int(max_page_num))]
    urls_with_ids = my_utils.flatten_list(ids_of_pages)
    urls = pd.DataFrame({'URL': urls_with_ids})
    return urls
