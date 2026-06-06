import datetime

from . import sql
from .base import Field

class General(Field):
    __chinese__ = '通用欄位'
    日期 = 'date', (datetime.date, sql.dtype.str)
    添加時間 = 'add_time', (datetime.datetime, sql.dtype.str)

class CompanyInfo(Field):
    __chinese__ = '公司資訊'
    代號 = 'id', (str, sql.dtype.str)
    名稱 = 'name', (str, sql.dtype.str)

class Technicals(Field):
    __chinese__ = '技術分析'
    開盤價 = 'open', (float, sql.dtype.float)
    最高價 = 'high', (float, sql.dtype.float)
    最低價 = 'low', (float, sql.dtype.float)
    收盤價 = 'close', (float, sql.dtype.float)
    交易股數 = 'volume', (int, sql.dtype.int)
    交易金額 = 'turnover', (int, sql.dtype.int)
    交易筆數 = 'trades', (int, sql.dtype.int)

class Sharehold(Field):
    __chinese__ = '持股比例'
    外資比例 = 'foreign', (float, sql.dtype.float)
    投信比例 = 'trust', (float, sql.dtype.float)
    自行商比例 = 'investor', (float, sql.dtype.float)
