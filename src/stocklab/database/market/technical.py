import pandas as pd
import requests
import time

def fetch(cate, which, date):
    date = pd.Timestamp(date)

    url = dict(
        price=dict(
            taiex='https://www.twse.com.tw/indicesReport/MI_5MINS_HIST',
            otc='https://www.tpex.org.tw/web/stock/index_info/Inxh/Inx_result.php'
        ),
        volume=dict(
            taiex='https://www.twse.com.tw/exchangeReport/FMTQIK',
            otc='https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41_result.php'
        )
    )

    param = dict(
        taiex=dict(response='html',date=date.strftime(f'%Y%m%d')),
        otc=dict(l='zh-tw',s='0,asc,0',o='htm',d=date.strftime(f'{date.year-1911}/%m'))
    )
    
    headers = dict(
        taiex = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
        },
        otc = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Host': 'www.tpex.org.tw',
            'Referer': url[cate]['otc']+'?l=zh-tw',
            'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="99", "Google Chrome";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': "Windows",
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
    )
        
    i = 0
    while True:
        try:
            assert i < 10, f'ConnectionError for the {i} time'
            res = requests.get(url[cate][which], param[which],headers=headers[which])
            break
        except requests.exceptions.ConnectionError:
            i += 1
            time.sleep(3)
    
    assert res.status_code == 200
    return res.text
