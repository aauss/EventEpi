import os

from eventepi.scraper import ProMedScraper, WhoDonScraper


def test_who_don_scraper():
    scraper = WhoDonScraper(headers={"test": "header"})
    scraper.find_urls()
    assert len(scraper.scrapable_urls) > 2800


def test_promed_scraper():
    scraper = ProMedScraper(headers={"test": "header"})
    scraper.find_scrapable_ids(start="01/01/1994", end="01/01/1995")
    assert len(scraper.ids) == 1
    post = scraper.request_post(scraper.ids[0])
    assert post.startswith(
        '<span class="printable"><p><img src="https://promedmail.org/wp-content/plugins/promed-latest-post/assets/images/printer.gif"/>'
    )
