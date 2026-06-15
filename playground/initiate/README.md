
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
                    - type=ALLBUT0999
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
            1. https://hist.tpex.org.tw/Hist/STOCK/AFTERTRADING/DAILY_CLOSE_QUOTES/param_3104.html
                - 民國92年8月至95年12月資訊
            2. https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing_hist96.html
                - 民國96年1月2日至96年4月20日資訊
            3. https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html
                - 民國96年1月起開始
        - 總體市場
            1. Not start yet

- 籌碼面
    - 上市
        - 金流
            - 融資: https://www.twse.com.tw/zh/trading/margin/mi-margn.html 
                1. 總體市場(個股+ETF): 分類項目 選取 **全部** (張, 千元)
                2. 個股: 分類項目 選取 **股票** (張)
                3. ETF: 分類項目 選取 **ETF** (張)
            - 融券,借券賣出: https://www.twse.com.tw/zh/trading/margin/twt93u.html
                1. 個股 (股)
                2. ETF (股)
                3. 總體市場(個股+ETF): 總體市場為合計列，並非獨立項目 (股)
            - 三大法人
                1. 總體市場: https://www.twse.com.tw/zh/trading/foreign/bfi82u.html
                    - 選取 **日報表**
                2. 個股: https://www.twse.com.tw/zh/trading/foreign/t86.html
                    - 分類項目 選取 **全部(不含權證、牛熊證、可展延牛熊證)**
        - 持股比例
            1. 外資及陸資: https://www.twse.com.tw/zh/trading/foreign/mi-qfiis.html
                - 選取 **全部(不含權證)**
            
    - 上櫃


# 信用交易

## 融資融券
https://www.twse.com.tw/zh/trading/margin/mi-margn.html
- 信用交易統計
    - 融資(交易單位)
        - 買進
        - 賣出
        - 現金(券)償還
        - 前日餘額
        - 今日餘額
    - 融券(交易單位)
        - 買進
        - 賣出
        - 現金(券)償還
        - 前日餘額
        - 今日餘額
    - 融資金額(仟元)
        - 買進
        - 賣出
        - 現金(券)償還
        - 前日餘額
        - 今日餘額

- 融資融券彙總 (股票), 融資融券彙總 (ETF)
    - 融資(交易單位)
        - 買進
        - 賣出
        - 現金(券)償還
        - 今日餘額
    - 融券(交易單位)
        - 買進
        - 賣出
        - 現金(券)償還
        - 今日餘額


## 借券賣出
https://www.twse.com.tw/zh/trading/margin/twt93u.html		
- 信用額度總量管制餘額表
    - 融券
        - 前日餘額
        - 賣出
        - 買進
        - 現券
        - 今日餘額
        - 次一營業日限額
    - 借券賣出
        - 前日餘額
        - 當日賣出
        - 當日還券
        - 當日調整
        - 當日餘額
        - 次一營業日可限額

# 三大法人

- 外陸資買進股數
- 外陸資賣出股數
- 外陸資買賣超股數

- 外陸資買進股數(不含外資自營商)
- 外陸資賣出股數(不含外資自營商)
- 外陸資買賣超股數(不含外資自營商)

- 外資自營商買進股數
- 外資自營商賣出股數
- 外資自營商買賣超股數


- 投信買進股數
- 投信賣出股數
- 投信買賣超股數


- 自營商買進股數
- 自營商賣出股數
- 自營商買賣超股數

- 自營商買進股數(自行買賣)
- 自營商賣出股數(自行買賣)
- 自營商買賣超股數(自行買賣)

- 自營商買進股數(避險)
- 自營商賣出股數(避險)
- 自營商買賣超股數(避險)

## table格式
### Micro
- CompanyInfo = DataTimestampFree + CompanyInfo
- FinancialStatement = DataTimestamp + 
- StockDaily = DataTimestamp + StockInfo + Technicals + 
    Margin + Short + ShortAdditional
    ------------------ from 融資融券彙總 (股票), 融資融券彙總 (ETF) -----------------------
        融資買進 margin_buy,
        融資賣出 margin_sell,
        融資現償 margin_cash_repayment,
        融資餘額 margin_balance,
    ------------------ from 信用額度總量管制餘額表 -----------------------
        融券買進 short_cover,
        融券賣出 short_sell,
        融券現償 short_stock_repayment,
        融券餘額 short_balance,
        融券次日限額 short_next_day_limit,

        借券賣出賣出 slb_sell,
        借券賣出還券 slb_return,
        借券賣出調整 slb_adjustment,
        借券賣出餘額 slb_balance,
        借券賣出次日限額 slb_next_day_limit,


### Macro
- MarketDaily = DataTimestamp + Technicals + 
    Margin + MarginAdditional + Short
    ------------------ from 信用交易統計 -----------------------
        融資買進,
        融資賣出,
        融資現償,
        融資餘額,

        融資金額買進 margin_amount_buy,
        融資金額賣出 margin_amount_sell,
        融資金額現償 margin_amount_cash_repayment,
        融資金額餘額 margin_amount_balance,
    ------------------ from 信用額度總量管制餘額表 -----------------------
        融券買進,
        融券賣出,
        融券現償,
        融券餘額,

        借券賣出賣出,
        借券賣出還券,
        借券賣出調整,
        借券賣出餘額,

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


