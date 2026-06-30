# Sources
- 技術面-價量
    - 上市
        - 個股
            - 開高低收 交易量
                - https://www.twse.com.tw/zh/trading/historical/mi-index.html
                - 分類項目 選取 **每日收盤行情(全部(不含權證、牛熊證、可展延牛熊證))**
                - 這個選項可以保留大盤資訊 並且 移除不是標的的項目
                - 但是早期資料內容並不包含指數點數，只有交易量
                    - 但這邊只能取得收盤價，沒有完整開高低收
                - 後期交易量更新可以只依據此查詢進行，但前期資料應該要到**每日市場成交資訊查詢**
                - API: 
                    - https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX
                    - type=ALLBUT0999 (ALLBUT0999NOTIND**每日收盤行情(全部(不含大盤、指數、權證、牛熊證、可展延牛熊證))**)
                    - response=json
                    - date=yyyymmdd
        - 總體市場
            - 交易量
                - https://www.twse.com.tw/zh/trading/historical/fmtqik.html
                - API:
                    - https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK
                    - response=json
                    - date=yyyymmdd
            - 開高低收
                - https://www.twse.com.tw/zh/indices/taiex/mi-5min-hist.html
                - API:
                    - https://www.twse.com.tw/indicesReport/MI_5MINS_HIST
                    - response=json
                    - date=yyyymmdd
                
    - 上櫃
        - 個股
            1. 民國92年8月至95年12月資訊
                - https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/param_3104.html
                - API: 
                    - https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES
                    - '/RSTA3104_' + date + '.HTML'
            2. 民國96年1月2日至96年4月20日資訊
                - https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing_hist96.html
                - API:
                    - https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis
                    - id = ''
                    - response = 'json'
                    - date = 
            3. 民國96年1月起開始
                - https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html
                - API:
                    - 'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes'
                    - id = ''
                    - response = 'json'
                    - date = 
        - 總體市場
            1. Not start yet

- 籌碼面
    - 上市
        - 金流
            - 融券,借券賣出
                - https://www.twse.com.tw/zh/trading/margin/twt93u.html
                - 個股 (股)
                - 總體市場: 總體市場為合計列，並非獨立項目 (股)
                - API:
                    - https://www.twse.com.tw/exchangeReport/TWT93U
                    - response = 'json'
                    - date = 
            - 融資,融券
                - https://www.twse.com.tw/zh/trading/margin/mi-margn.html 
                - 總體市場加總、個股: 分類項目 選取 **全部** (張, 千元)
                - API:
                    - https://www.twse.com.tw/exchangeReport/MI_MARGN
                    - response = 'json',
                    - selectType = 'ALL',
                    - date = 
            - 三大法人
                - 總體市場
                    - https://www.twse.com.tw/zh/trading/foreign/bfi82u.html
                    - 選取 **日報表**
                    - API:
                        - https://www.twse.com.tw/rwd/zh/fund/BFI82U
                        - response = 'json'
                        - type = 'day'
                        - dayDate = 
                - 個股
                    - https://www.twse.com.tw/zh/trading/foreign/t86.html
                    - 分類項目 選取 **全部(不含權證、牛熊證、可展延牛熊證)**
                    - API:
                        - https://www.twse.com.tw/rwd/zh/fund/T86
                        - response = json
                        - selectType = 'ALLBUT0999'
                        - date = 
        - 持股比例
            - 外資及陸資
                - https://www.twse.com.tw/zh/trading/foreign/mi-qfiis.html
                - 選取 **全部(不含權證)**
                - API:
                    - https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS
                    - response = 'json'
                    - selectType = 'ALLBUT0999'
                    - date = 
            
    - 上櫃


# Schema
    - micro
        - StockDaily
                - fields.DataTimestamp
                    - 添加日期 - auto
                    - 資料日期 - 
                - fields.StockInfo
                    - 代號 - key
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


