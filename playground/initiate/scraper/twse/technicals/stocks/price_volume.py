from ...base import TWSEScraper

class URL_0(TWSEScraper):

    def create_request_info(self, date):
        url = 'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX'
        params = {
            "response": "json",
            "type": "ALLBUT0999", # 每日收盤行情(全部(不含權證、牛熊證、可展延牛熊證))
            "date": self.create_request_date(date),
            "_": self.create_cache_id()
        }
        return url, params

url_0 = URL_0('D', '.json')