import json

from ._stocks_stage_2 import STOCKS_STAGE_2

class STOCKS_STAGE_3(STOCKS_STAGE_2):
    
    def create_request_info(self, date):
        url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes'
        data = dict(
            date = self.create_request_date(date),
            id = '',
            response = 'json'
        )
        return url, data

    def save(self, res, filename):
        data = res.json()
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

stocks_stage_3 = STOCKS_STAGE_3('D', '.json')