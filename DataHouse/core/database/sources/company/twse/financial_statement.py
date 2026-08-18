from ..base import FINANCIAL_STATEMENT_V0, TWSE
from ....schema.tables import FinancialStatement

class VERSION_0(TWSE, FINANCIAL_STATEMENT_V0):

    def to_df(self, content):
        df = super().to_df(content)
        df['年度'] = df['年度'].astype(int) + 1911
        return df

version_0 = VERSION_0(
    FinancialStatement,
    {
        '公司代號': FinancialStatement.f_index.代號,
        '年度': FinancialStatement.f_season.年度,
        '季別': FinancialStatement.f_season.季別,
        '基本每股盈餘(元)': FinancialStatement.f_financial_statement.每股盈餘,
        '營業收入': FinancialStatement.f_financial_statement.營業收入,
        '營業利益': FinancialStatement.f_financial_statement.營業利益,
        '營業外收入及支出': FinancialStatement.f_financial_statement.營業外收入及支出,
        '稅後淨利': FinancialStatement.f_financial_statement.稅後淨利,
    },
    True
)