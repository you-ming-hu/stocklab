from ... import IndustryScraper

class URL_0(IndustryScraper):
    
    def get_company_ids(self, table):
        return [company['公司代號'] for company in table]
    
url_0 = URL_0()