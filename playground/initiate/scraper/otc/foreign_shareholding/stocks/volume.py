from ...base import OTCScraper

class URL_0(OTCScraper):
    
    def create_request_info(self, date):
        url = 'https://mopsov.twse.com.tw/server-java/t13sa150_otc'
        y,m,d = self.create_request_date(date).split('/')
        data = dict(
            years = y,
            months = m,
            days = d,
            bcode = '',
            step = '2'
        )
        return url, data
    
    def save(self, res, filename):
        self.old_api_save(res, filename)
        
url_0 = URL_0('D', '.html')