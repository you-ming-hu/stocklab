from .. import Scraper

class OTCScraper(Scraper):
    
    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://hist.tpex.org.tw/"
        }
        return super().create_session(header)