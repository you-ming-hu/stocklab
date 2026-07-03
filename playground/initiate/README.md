# Sources


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


- 技術分析
    - TWSE
        - 總體
            - 開高低收
                - Website
                    - 發行量加權股價指數歷史資料
                    - https://www.twse.com.tw/zh/indices/taiex/mi-5min-hist.html
                - API
                    - https://www.twse.com.tw/indicesReport/MI_5MINS_HIST
                    - response='json'
                    - date='yyyymmdd'
            - 量（元股筆）
                - Website
                    - 每日市場成交資訊
                    - https://www.twse.com.tw/zh/trading/historical/fmtqik.html
                - API
                    - https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK
                    - response='json'
                    - date='yyyymmdd'
        - 個股
            - 開高低收+量（元股筆）
                - Website
                    - 每日收盤行情
                    - 選取 **全部(不含大盤、指數、權證、牛熊證、可展延牛熊證)**
                    - https://www.twse.com.tw/zh/trading/historical/mi-index.html
                - API
                    - https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX
                    - type='ALLBUT0999NOTIND'
                    - response='json'
                    - date='yyyymmdd'
    - OTC
        - 總體
        - 個股
            - 開高低收+量（元股筆）民國92年8月至95年12月資訊
                - Website
                    - 上櫃股票行情
                    - https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/param_3104.html
                - API
                    - https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES
                    - '/RSTA3104_' + date + '.HTML'
            - 開高低收+量（元股筆）民國96年1月2日至96年4月20日資訊
                - Website
                    - 上櫃股票行情
                    - https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing_hist96.html
                - API
                    - https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotesHis
                    - response='json'
                    - date='yyyymmdd'
            - 開高低收+量（元股筆）民國96年1月起開始
                - Website
                    - 上櫃股票行情
                    - https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html
                - API
                    - https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes'
                    - response='json'
                    - date='yyyymmdd'
- 融資融券
    - TWSE
        - 總體個股合併
            - 總體（融資融券張+融資千元）+ 個股（融資融券張）
                - Website
                    - 融資融券餘額
                    - 選取 **全部**
                    - https://www.twse.com.tw/zh/trading/margin/mi-margn.html 
                - API
                    - https://www.twse.com.tw/exchangeReport/MI_MARGN
                    - response='json',
                    - selectType='ALL',
                    - date='yyyymmdd'
    - OTC
        - 總體
        - 個股
- 融券借券賣出
    - TWSE
        - 總體
            - 無資料，但可由個股加總獲得
        - 個股
            - 融券借券賣出股
                - Website
                    - 融券借券賣出餘額
                    - https://www.twse.com.tw/zh/trading/margin/twt93u.html
                - API
                    - https://www.twse.com.tw/exchangeReport/TWT93U
                    - response='json'
                    - date='yyyymmdd'
            - 融券借券賣出元 **NEW!**
                - Website
                    - 當日融券賣出與借券賣出成交量值
                    - https://www.twse.com.tw/zh/trading/historical/twtasu.html
                - API
    - OTC
        - 總體
        - 個股
- 三大法人
    - TWSE
        - 總體
            - 金額 元
                - Website
                    - 三大法人買賣金額統計表
                    - 選取 **日報表**
                    - https://www.twse.com.tw/zh/trading/foreign/bfi82u.html
                - API
                    - https://www.twse.com.tw/rwd/zh/fund/BFI82U
                    - response='json'
                    - type='day'
                    - dayDate='yyyymmdd' 
        - 個股
            - 交易量 股
                - Website
                    - 三大法人買賣超日報
                    - 選取 **全部(不含權證、牛熊證、可展延牛熊證)**
                    - https://www.twse.com.tw/zh/trading/foreign/t86.html
                - API:
                    - https://www.twse.com.tw/rwd/zh/fund/T86
                    - response='json'
                    - selectType='ALLBUT0999'
                    - date='yyyymmdd'
    - OTC
        - 總體
        - 個股
- 外資持股
    - TWSE
        - 個股
            - 持股量 股
                - Website
                    - 外資及陸資投資持股統計
                    - 選取 **全部(不含權證)**
                    - https://www.twse.com.tw/zh/trading/foreign/mi-qfiis.html
                - API:
                    - https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS
                    - response='json'
                    - selectType='ALLBUT0999'
                    - date='yyyymmdd'
    - OTC
        - 個股
- 股權分散表
    - BOTH
