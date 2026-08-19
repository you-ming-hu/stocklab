from ..base import Source
from ...schema.tables import CompanyInfo, FinancialStatement

import pandas as pd
import pathlib
import json

class BASE(Source):

    def check_empty(self, content):
        return False
        
    def to_df(self, content):
        return pd.DataFrame(content)

class OVERVIEW_V0(BASE):

    def open(self, file):
        data = []
        for company in sorted(pathlib.Path(file,'companies').glob('*.json')):
            with open(company) as f:
                content = json.load(f)
            data.extend(content)
        return data
    
    def to_df(self, content):
        df = pd.DataFrame(content)
        return df

    def add_other_columns(self, df):
        df[CompanyInfo.f_company_info.市場別] = self.__class__.market_type
        return df
    
    def format_dtype(self, df):
        str_cols = [
            CompanyInfo.f_company_info.代號,
            CompanyInfo.f_company_info.名稱,
            CompanyInfo.f_company_info.主要登記產業,
            CompanyInfo.f_company_info.營運產業,
            CompanyInfo.f_company_info.題材
        ]
        df = super().format_dtype(df, str_cols=str_cols)
        return df

class FINANCIAL_STATEMENT_V0(BASE):

    def open(self,file):
        return super().open(pathlib.Path(file,'overview.json'))

    def format_dtype(self, df):
        int_cols = [
            FinancialStatement.f_season.年度,
            FinancialStatement.f_season.季別,
            FinancialStatement.f_financial_statement.營業利益,
            FinancialStatement.f_financial_statement.營業外收入及支出,
            FinancialStatement.f_financial_statement.營業收入,
            FinancialStatement.f_financial_statement.稅後淨利,
        ]
        float_cols = [
            FinancialStatement.f_financial_statement.每股盈餘,
        ]
        df = super().format_dtype(df, int_cols=int_cols, float_cols=float_cols)
        return df

class TWSE:
    market_type = 'TWSE'

class OTC:
    market_type = 'OTC'