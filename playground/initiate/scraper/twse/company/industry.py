from ... import IndustryScraper

class VERSION_0(IndustryScraper):
    
    def get_company_ids(self, table):
        return [company['公司代號'] for company in table]
    
version_0 = VERSION_0()