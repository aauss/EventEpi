# coding=utf8
import unittest
import scraper
import process_resources
import logging
import translation
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('scraper')
logger.setLevel(logging.INFO)

class TestScraper(unittest.TestCase):
    def test_promed_1(self):
        result = scraper.scrape("http://promedmail.org/direct.php?id=20140919.436908")
        # print process_resources.extract_clean_content(result['htmlContent'])
        self.assertFalse(result.get('unscrapable'))
    def test_promed_2(self):
        result = scraper.scrape("http://www.promedmail.org/direct.php?id=3041400")
        # print process_resources.extract_clean_content(result['htmlContent'])
        self.assertFalse(result.get('unscrapable'))
    def test_link_1(self):
        result = scraper.scrape("http://news.zing.vn/nhip-song-tre/thay-giao-gay-sot-tung-bo-luat-tinh-yeu/a291427.html")
        # print process_resources.extract_clean_content(result['htmlContent'])
        self.assertFalse(result.get('unscrapable'))
    def test_link_2(self):
        result = scraper.scrape("http://news.google.com/news/url?sa=t&fd=R&usg=AFQjCNErKUuDda2EHlPu0LwpUJ0dcdDY4g&url=http://focus.stockstar.com/SS2012101000003737.shtml")
        # print process_resources.extract_clean_content(result['htmlContent'])
        self.assertFalse(result.get('unscrapable'))
    def test_link_3(self):
        result = scraper.scrape("http://www.theargus.co.uk/news/9845086.Screening_follows_new_cases_of_TB_reported_in_Sussex/")
        print process_resources.extract_clean_content(result['htmlContent'])
        self.assertFalse(result.get('unscrapable'))
    def test_link_4(self):
        result = scraper.scrape("http://www.foodmate.net/news/yujing/2012/05/206413.html")
        # print process_resources.extract_clean_content(result['htmlContent'])
        self.assertFalse(result.get('unscrapable'))
    def test_link_5(self):
        # This article can be visited in my browser, but the server
        # sends back error messages in html comments when scraped.
        result = scraper.scrape("http://news.google.com/news/url?sa=t&fd=R&usg=AFQjCNHf5IPdc5RFjTgsO7TnHq_LW8l0-Q&url=http://www.eltribuno.info/Jujuy/218240-Suspendieron-las-clases-por-un-brote-de-influenza-B-en-pueblos-de-la-Puna-.note.aspx")
        if result.get('unscrapable'):
            print result
        self.assertFalse(result.get('unscrapable'))
        self.assertTrue(len(process_resources.extract_clean_content(result['htmlContent'])['content']) > 1)
    def test_english_detection(self):
        from translation import Translator
        my_translator = Translator(None)
        result = scraper.scrape("http://news.google.com/news/url?sa=t&fd=R&usg=AFQjCNFY1KzEAhaiZchzd5ulmoY4_4P8kA&url=http://vov.vn/Van-hoa/NSND-Thanh-Hoa-xuc-dong-hat-truoc-benh-nhan/228256.vov")
        self.assertFalse(result.get('unscrapable'))
        text_obj = process_resources.extract_clean_content(result['htmlContent'])
        self.assertFalse(my_translator.is_english(text_obj['content']))
    def test_english_translation(self):
        import config
        from translation import Translator
        my_translator = Translator(config)
        result = scraper.scrape("http://peninsulardigital.com/municipios/comondu/refuerzan-acciones-contra-el-dengue/155929")
        text_obj = process_resources.extract_clean_content(result['htmlContent'])
        translation_obj = my_translator.translate_to_english(text_obj['content'])
        self.assertFalse(translation_obj.get('error'))
    def test_cutoff(self):
        # This article is being cut off at "Tochter in die Kita bringen"
        # Goose is at fault. Using beautiful soup instead seems to avoid the
        # cutoff, however we need a method to determine which method we should
        # be using.
        result = scraper.scrape("http://www.haz.de/Hannover/Aus-der-Region/Wennigsen/Nachrichten/Kita-Kind-an-Ehec-erkrankt")
        self.assertTrue(
            process_resources.extract_clean_content(
                result['htmlContent'])['content']
                    .strip()
                    .endswith("Carsten Fricke"))
    def test_pdf_querystring(self):
        result = scraper.scrape(
            "http://apps.who.int/iris/bitstream/10665/136645/1/roadmapupdate17Oct14_eng.pdf?ua=1")
    def test_english_detection(self):
        import config
        from translation import Translator
        my_translator = Translator(config)
        self.assertFalse(my_translator.is_english("""
        Bệnh viêm não năm nay đã xuất hiện nhiều dấu hiệu bất thường...
        """))