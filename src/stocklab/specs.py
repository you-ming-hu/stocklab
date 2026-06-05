import enum

class terms(enum.StrEnum):
    # general information
    證券代號 = 'stock_id'
    證券名稱 = 'stock_name'
    # technicals
    開盤價 = 'open'
    最高價 = 'high'
    最低價 = 'low'
    收盤價 = 'close'
    成交股數 = 'volume'
    成交金額 = 'turnover'
    成交筆數 = 'transaction'
    # others...