from ..base import Source
from ...schema.tables import CompanyInfo

import pathlib
import json
import pandas as pd

class VERSION_0(Source):

    def open(self, file):
        data = []
        for company in sorted(pathlib.Path(file).glob('*.json')):
            with open(company) as f:
                content = json.load(f)
            for c in content:
                data.append([company.stem]+c)
        return data

    def to_df(self, content):
        df = pd.DataFrame(columns=['代號','營運產業','題材'], data=content)
        return df

version_0 = VERSION_0(
    CompanyInfo,
    {
        '代號': CompanyInfo.f_company_info.代號,
        '營運產業': CompanyInfo.f_company_info.營運產業,
        '題材': CompanyInfo.f_company_info.題材,
    },
    True
)
