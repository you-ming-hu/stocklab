from ... import IndustryScraper

class URL_0(IndustryScraper):
    
    def get_company_ids(self, table):
        return [company['SecuritiesCompanyCode'] for company in table]
    
url_0 = URL_0()