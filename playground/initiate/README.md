# Sources

- 技術分析
    - TWSE
        - 個股
            1. 每日收盤行情: https://www.twse.com.tw/zh/trading/historical/mi-index.html
                - 選取 **全部(不含大盤、指數、權證、牛熊證、可展延牛熊證)**
                - Content
                    - 開高低收
                    - 成交金額（元）、成交股數（股）、成交筆數（筆）
                - API
                    - https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX
                    - type='ALLBUT0999NOTIND'
                    - response='json'
                    - date='yyyymmdd'
        - 總體
            1. 發行量加權股價指數歷史資料: https://www.twse.com.tw/zh/indices/taiex/mi-5min-hist.html
                - Content
                    - 開高低收
                - API
                    - https://www.twse.com.tw/indicesReport/MI_5MINS_HIST
                    - response='json'
                    - date='yyyymmdd'
            2. 每日市場成交資訊: https://www.twse.com.tw/zh/trading/historical/fmtqik.html
                - Content
                    - 成交金額（元）、成交股數（股）、成交筆數（筆）
                - API
                    - https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK
                    - response='json'
                    - date='yyyymmdd'
        - 資料庫欄位結構
            - 個股
                - 開盤價，最高價，最低價，收盤價，成交金額（元），成交股數（股），成交筆數（筆）
            - 總體
                - 開盤價，最高價，最低價，收盤價，成交金額（元），成交股數（股），成交筆數（筆）
        
    - OTC
        - 個股
            1. 上櫃股票行情: https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/param_3104.html
                - 民國92年8月至95年12月資訊
                - Content
                    - 開高低收
                    - 成交金額（元）、成交股數（股）、成交筆數（筆）
                - API
                    - https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES
                    - '/RSTA3104_' + date + '.HTML'
            2. 上櫃股票行情: https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing_hist96.html
                - 民國96年1月2日至96年4月20日資訊
                - Content
                    - 開高低收
                    - 成交金額（元）、成交股數（股）、成交筆數（筆）
                - API
                    - https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis
                    - response='json'
                    - date='yyyymmdd'
            3. 上櫃股票行情: https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html
                - 民國96年1月起開始
                - Content
                    - 開高低收
                    - 成交金額（元）、成交股數（股）、成交筆數（筆）
                - API
                    - https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes'
                    - response='json'
                    - date='yyyymmdd'
        - 總體
        - 資料庫欄位結構
            - 個股
                - 開盤價，最高價，最低價，收盤價，成交金額（元），成交股數（股），成交筆數（筆）
- 融資融券
    - TWSE
        - 個股總體合併
            1. 融資融券餘額: https://www.twse.com.tw/zh/trading/margin/mi-margn.html
                - 選取 **全部**
                - Content
                    - 個股
                        - 成交量（張）：（融資、融券）Ｘ（買進、賣出、償還、餘額、次日限額）
                    - 總體
                        - 成交量（張）：（融資、融券）Ｘ（買進、賣出、償還、餘額）
                        - 成交金額（千元）：（融資）Ｘ（買進、賣出、償還、餘額）
                - API
                    - https://www.twse.com.tw/exchangeReport/MI_MARGN
                    - response='json',
                    - selectType='ALL',
                    - date='yyyymmdd'
        - 資料庫欄位結構
            - 個股
                - 成交量（張）：（融資、融券）Ｘ（買進、賣出、償還、餘額、次日限額）
            - 總體
                - 成交量（張）：（融資、融券）Ｘ（買進、賣出、償還、餘額）
                - 成交金額（千元）：（融資）Ｘ（買進、賣出、償還、餘額）
    - OTC
        - 個股
        - 總體
    
- 融券借券賣出
    - TWSE
        - 個股
            1. 融券借券賣出餘額: https://www.twse.com.tw/zh/trading/margin/twt93u.html
                - Content
                    - 成交量（股）
                        - （融券）Ｘ（買進、賣出、償還、餘額、次日限額）
                        - （借券賣出）Ｘ（買進、賣出、調整、餘額、次日限額）
                - API
                    - https://www.twse.com.tw/exchangeReport/TWT93U
                    - response='json'
                    - date='yyyymmdd'
            2. 當日融券賣出與借券賣出成交量值: https://www.twse.com.tw/zh/trading/historical/twtasu.html **NEW!**
                - Content
                    - 成交金額（元）
                        - 融券、借券賣出
                - API
        - 資料庫欄位結構
            - 個股
                - 成交量（股）
                    - （融券）Ｘ（買進、賣出、償還、餘額、次日限額）
                    - （借券賣出）Ｘ（買進、賣出、調整、餘額、次日限額）
                - 成交金額（元）
                    - 融券、借券賣出
            - 總體
                - 成交量（股）
                    - （融券）Ｘ（買進、賣出、償還、餘額）
                    - （借券賣出）Ｘ（買進、賣出、調整、餘額）
                - 成交金額（元）
                    - 融券、借券賣出
    - OTC
        - 總體
        - 個股
- 三大法人
    - TWSE
        - 個股
            1. 三大法人買賣超日報: https://www.twse.com.tw/zh/trading/foreign/t86.html
                - 選取 **全部(不含權證、牛熊證、可展延牛熊證)**
                - Content
                    - 交易量（股）
                        - （自營商(自行買賣)、自營商(避險)、投信、外資及陸資(不含外資自營商)、外資自營商）Ｘ（買進、賣出）
                - API:
                    - https://www.twse.com.tw/rwd/zh/fund/T86
                    - response='json'
                    - selectType='ALLBUT0999'
                    - date='yyyymmdd'
        - 總體
            1. 三大法人買賣金額統計表: https://www.twse.com.tw/zh/trading/foreign/bfi82u.html
                - 選取 **日報表**
                - Content
                    - 成交金額（元）
                        - （自營商(自行買賣)、自營商(避險)、投信、外資及陸資(不含外資自營商)、外資自營商）Ｘ（買進、賣出）
                - API
                    - https://www.twse.com.tw/rwd/zh/fund/BFI82U
                    - response='json'
                    - type='day'
                    - dayDate='yyyymmdd' 
        - 資料庫欄位結構
            - 個股
                - 交易量（股）
                    - （自營商(自行買賣)、自營商(避險)、投信、外資及陸資(不含外資自營商)、外資自營商）Ｘ（買進、賣出）
            - 總體
                - 交易量（股）
                    - （自營商(自行買賣)、自營商(避險)、投信、外資及陸資(不含外資自營商)、外資自營商）Ｘ（買進、賣出）
                - 成交金額（元）
                    - （自營商(自行買賣)、自營商(避險)、投信、外資及陸資(不含外資自營商)、外資自營商）Ｘ（買進、賣出）
                
    - OTC
        - 個股
        - 總體
- 外資持股
    - TWSE
        - 個股
            1. 外資及陸資投資持股統計: https://www.twse.com.tw/zh/trading/foreign/mi-qfiis.html
                - 選取 **全部(不含權證)**
                - Content
                    - 發行量（股）
                    - 全體外資及陸資持有量（股）
                    - 外資及陸資共用法令投資上限比率
                - API:
                    - https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS
                    - response='json'
                    - selectType='ALLBUT0999'
                    - date='yyyymmdd'
        - 資料庫欄位結構
            - 個股
                - 發行量（股）、全體外資及陸資持有量（股）、外資及陸資共用法令投資上限比率
    - OTC
        - 個股
- 股權分散表
    - BOTH


# schema
    - micro
        - StockDaily
            
            # 股票資訊
            - 資料日期: TWSE/個股/每日收盤行情
            - 代號: TWSE/個股/每日收盤行情
            - 名稱: TWSE/個股/每日收盤行情
            - 市場別: TWSE/個股/每日收盤行情
            - 可交易標的: TWSE/個股/每日收盤行情
            
            # 技術分析
            - 開盤價: TWSE/個股/每日收盤行情
            - 最高價: TWSE/個股/每日收盤行情
            - 最低價: TWSE/個股/每日收盤行情
            - 收盤價: TWSE/個股/每日收盤行情
            - 成交金額: TWSE/個股/每日收盤行情
            - 成交股數: TWSE/個股/每日收盤行情
            - 成交筆數: TWSE/個股/每日收盤行情

            # 融資
            - 融資買進股數: TWSE/個股總體合併/融資融券餘額（張）->（股）
            - 融資賣出股數: TWSE/個股總體合併/融資融券餘額（張）->（股）
            - 融資現金償還股數: TWSE/個股總體合併/融資融券餘額（張）->（股）
            - 融資餘額股數: TWSE/個股總體合併/融資融券餘額（張）->（股）
            - 融資次日限額股數: TWSE/個股總體合併/融資融券餘額（張）->（股）

            # 融券
            - 融券買進股數: TWSE/個股/融券借券賣出餘額
            - 融券賣出股數: TWSE/個股/融券借券賣出餘額
            - 融券償還股數: TWSE/個股/融券借券賣出餘額
            - 融券餘額股數: TWSE/個股/融券借券賣出餘額
            - 融券次日限額股數: TWSE/個股/融券借券賣出餘額
            - 融券成交金額: TWSE/個股/當日融券賣出與借券賣出成交量值

            # 借券賣出
            - 借券賣出買進股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出賣出股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出調整股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出_不含賣出_總異動股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出餘額股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出次日限額股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出成交金額: TWSE/個股/當日融券賣出與借券賣出成交量值
            
            # 三大法人
            - 自營商_自行買賣_買進股數: TWSE/個股/三大法人買賣超日報
            - 自營商_自行買賣_賣出股數: TWSE/個股/三大法人買賣超日報
            - 自營商_避險_買進股數: TWSE/個股/三大法人買賣超日報
            - 自營商_避險_賣出股數: TWSE/個股/三大法人買賣超日報
            - 自營商買進股數: TWSE/個股/三大法人買賣超日報
            - 自營商賣出股數: TWSE/個股/三大法人買賣超日報
            - 投信買進股數: TWSE/個股/三大法人買賣超日報
            - 投信賣出股數: TWSE/個股/三大法人買賣超日報
            - 外資及陸資_不含外資自營商_買進股數: TWSE/個股/三大法人買賣超日報
            - 外資及陸資_不含外資自營商_賣出股數: TWSE/個股/三大法人買賣超日報
            - 外資自營商買進股數: TWSE/個股/三大法人買賣超日報
            - 外資自營商賣出股數: TWSE/個股/三大法人買賣超日報
            - 外資買進賣出股數: TWSE/個股/三大法人買賣超日報
            - 外資賣出股數: TWSE/個股/三大法人買賣超日報

            # 外資持股
            - 發行股數: TWSE/個股/外資及陸資投資持股統計
            - 全體外資及陸資持有股數: TWSE/個股/外資及陸資投資持股統計
            - 外資及陸資共用法令投資上限比率: TWSE/個股/外資及陸資投資持股統計
    - macro
        - MarketDaily (TWSEDaily, OTCDaily)
            - 資料日期: TWSE/總體/發行量加權股價指數歷史資料
            
            # 技術分析
            - 開盤價: TWSE/總體/發行量加權股價指數歷史資料
            - 最高價: TWSE/總體/發行量加權股價指數歷史資料
            - 最低價: TWSE/總體/發行量加權股價指數歷史資料
            - 收盤價: TWSE/總體/發行量加權股價指數歷史資料
            - 成交金額: TWSE/總體/每日市場成交資訊
            - 成交股數: TWSE/總體/每日市場成交資訊
            - 成交筆數: TWSE/總體/每日市場成交資訊

            # 融資
            - 融資買進股數: TWSE/個股總體合併/融資融券餘額（張）->（股）
            - 融資賣出股數: TWSE/個股總體合併/融資融券餘額（張）->（股）
            - 融資現金償還股數: TWSE/個股總體合併/融資融券餘額（張）->（股）
            - 融資餘額股數: TWSE/個股總體合併/融資融券餘額（張）->（股）
            - 融資買進金額: TWSE/個股總體合併/融資融券餘額（千元）->（元）
            - 融資賣出金額: TWSE/個股總體合併/融資融券餘額（千元）->（元）
            - 融資償還金額: TWSE/個股總體合併/融資融券餘額（千元）->（元）
            - 融資餘額金額: TWSE/個股總體合併/融資融券餘額（千元）->（元）

            # 融券
            - 融券買進股數: TWSE/個股/融券借券賣出餘額
            - 融券賣出股數: TWSE/個股/融券借券賣出餘額
            - 融券償還股數: TWSE/個股/融券借券賣出餘額
            - 融券餘額股數: TWSE/個股/融券借券賣出餘額
            - 融券成交金額: TWSE/個股/當日融券賣出與借券賣出成交量值

            # 借券賣出
            - 借券賣出買進股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出賣出股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出調整股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出_不含賣出_總異動股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出餘額股數: TWSE/個股/融券借券賣出餘額
            - 借券賣出成交金額: TWSE/個股/當日融券賣出與借券賣出成交量值
            
            # 三大法人
            - 自營商_自行買賣_買進股數: TWSE/個股/三大法人買賣超日報
            - 自營商_自行買賣_賣出股數: TWSE/個股/三大法人買賣超日報
            - 自營商_避險_買進股數: TWSE/個股/三大法人買賣超日報
            - 自營商_避險_賣出股數: TWSE/個股/三大法人買賣超日報
            - 自營商買進股數: TWSE/個股/三大法人買賣超日報
            - 自營商賣出股數: TWSE/個股/三大法人買賣超日報
            - 投信買進股數: TWSE/個股/三大法人買賣超日報
            - 投信賣出股數: TWSE/個股/三大法人買賣超日報
            - 外資及陸資_不含外資自營商_買進股數: TWSE/個股/三大法人買賣超日報
            - 外資及陸資_不含外資自營商_賣出股數: TWSE/個股/三大法人買賣超日報
            - 外資自營商買進股數: TWSE/個股/三大法人買賣超日報
            - 外資自營商賣出股數: TWSE/個股/三大法人買賣超日報
            - 外資買進賣出股數: TWSE/個股/三大法人買賣超日報
            - 外資賣出股數: TWSE/個股/三大法人買賣超日報
            - 自營商_自行買賣_買進金額: TWSE/總體/三大法人買賣金額統計表
            - 自營商_自行買賣_賣出金額: TWSE/總體/三大法人買賣金額統計表
            - 自營商_避險_買進金額金額: TWSE/總體/三大法人買賣金額統計表
            - 自營商_避險_賣出金額: TWSE/總體/三大法人買賣金額統計表
            - 自營商買進金額: TWSE/總體/三大法人買賣金額統計表
            - 自營商賣出金額: TWSE/總體/三大法人買賣金額統計表
            - 投信買進金額: TWSE/總體/三大法人買賣金額統計表
            - 投信賣出金額: TWSE/總體/三大法人買賣金額統計表
            - 外資及陸資_不含外資自營商_買進金額: TWSE/總體/三大法人買賣金額統計表
            - 外資及陸資_不含外資自營商_賣出金額: TWSE/總體/三大法人買賣金額統計表
            - 外資自營商買進金額: TWSE/總體/三大法人買賣金額統計表
            - 外資自營商賣出金額: TWSE/總體/三大法人買賣金額統計表
            - 外資買進賣出金額: TWSE/總體/三大法人買賣金額統計表
            - 外資賣出金額: TWSE/總體/三大法人買賣金額統計表



























# Schema
    - micro
        - StockDaily
            - fields.DataTimestamp
                - 添加日期 - 
                - 資料日期 - 
            - fields.StockInfo
                - 代號 - 
                - 名稱 - 
                - 市場別 - 
            - fields.Technicals
                - 開盤價
                - 最高價
                - 最低價
                - 收盤價
                - 交易股數
                - 交易金額
                - 交易筆數
            - fields.Margin
                - 融資買進股數
                - 融資賣出股數
                - 融資現償股數
                - 融資餘額股數
            - fields.Short
                - 融券買進股數
                - 融券賣出股數
                - 融券現償股數
                - 融券餘額股數
                - 借券賣出賣出股數
                - 借券賣出還券股數
                - 借券賣出調整股數
                - 借券賣出不含賣出總異動股數
                - 借券賣出餘額股數
            - fields.ShortAdditional
                - 融券次日限額股數
                - 借券賣出次日限額股數
            - fields.InstitutionShareFlow
                - 外陸資_不含外資自營商_買進股數
                - 外陸資_不含外資自營商_賣出股數
                - 外資自營商買進股數
                - 外資自營商賣出股數
                - 外陸資買進股數
                - 外陸資賣出股數
                - 投信買進股數
                - 投信賣出股數
                - 自營商_自行買賣_買進股數
                - 自營商_自行買賣_賣出股數
                - 自營商_避險_買進股數
                - 自營商_避險_賣出股數
                - 自營商買進股數
                - 自營商賣出股數
            - fields.Ownership
                - 總發行股數
                - 外陸資持有股數
                - 外陸資投資上限比率
        - CompanyInfo
        - ShareholdingDistribution
    - macro
        - TWSEDaily
        - OTCDaily











# OpenAPI swagger
上市: https://openapi.twse.com.tw/
上櫃: https://www.tpex.org.tw/openapi/


# 公司完整資訊處理方式
1. 先從openapi下載總表 (這邊只有當下資料，沒有歷史資料)
    - 上市: https://openapi.twse.com.tw/v1/opendata/t187ap14_L
    - 上櫃: https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap14_O
2. 依據股票代號查詢:
    - 來源網站: https://ic.tpex.org.tw/index.php
    - 上市櫃共用
    - 如下
```
from bs4 import BeautifulSoup
import requests

stock_id = '1101'

url = f"https://ic.tpex.org.tw/company_chain.php?stk_code={stock_id}"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers, timeout=10)
response.encoding = 'utf-8'

soup = BeautifulSoup(response.text, 'html.parser')
table = soup.find('body').find('center').find('div', 'main-panel').find('div', 'content-panel-main').find('div', 'content').find_all('h4')

[l.text.replace('►','').replace('\xa0','').split('>') for l in table[1:]]
```


