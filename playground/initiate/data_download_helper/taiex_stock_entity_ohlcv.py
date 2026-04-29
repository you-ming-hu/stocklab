import pandas as pd
import time
import json
import requests
import pathlib
import random

def create_session():
    HEADER = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.twse.com.tw/"
    }
    session = requests.Session()
    session.headers.update(HEADER)
    return session

def create_request_url(date):
    ROOT_URL = 'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX'
    CATEGORY = 'type=ALLBUT0999' # 每日收盤行情(全部(不含權證、牛熊證、可展延牛熊證))
    DATA_FORMAT = 'response=json'

    if isinstance(date, pd.Timestamp):
        date = date.strftime("%Y%m%d")
    elif isinstance(date, str):
        date = pd.Timestamp(date)
        date = date.strftime("%Y%m%d")
    else:
        assert False, 'not recognized date type'
    
    cache_id = f'_={int(time.time()*1000)}'
    date = f'date={date}'
    url = f'{ROOT_URL}?{date}&{CATEGORY}&{DATA_FORMAT}&{cache_id}'
    return url

def download(session, url, filename, timeout, cooldown_if_abnormal=False):
    COOLDOWN_TIME = 5 * 60
    try:
        print('Requesting', end='\t')
        res = session.get(url, timeout=timeout)

    except Exception as err:
        print(err)
        if cooldown_if_abnormal:
            print(f'cool down for: {COOLDOWN_TIME} secs')
            time.sleep(COOLDOWN_TIME)
        return False
        
    if res.status_code != 200:
        print('status_code != 200')
        if cooldown_if_abnormal:
            print(f'cool down for: {COOLDOWN_TIME} secs')
            time.sleep(COOLDOWN_TIME)
        return False

    try:
        data = res.json()
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    except Exception as err:
        print(err)
        if cooldown_if_abnormal:
            print(f'cool down for: {COOLDOWN_TIME} secs')
            time.sleep(COOLDOWN_TIME)
        return False

    print('Finisih', end='\t')
    return True

def download_single(session, url, filename, timeout, cooldown_if_abnormal=False):
    MIN_SLEEP_TIME = 2
    MAX_SLEEP_TIME = 4
    success = download(session, url, filename, timeout, cooldown_if_abnormal)
    sleep_time = random.uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME)
    print(f'Sleep: {sleep_time:.1f}')
    time.sleep(sleep_time)
    return success

def download_batch(start_date, end_date, save_dir, stage, timeout=10):
    RESTART_SESSION_COUNT = 1000

    save_dir = pathlib.Path(save_dir, stage)
    save_dir.mkdir(parents=True, exist_ok=True)

    dates = pd.date_range(start_date, end_date)
    request_count = 0
    for date in dates:
        print(date, end='\t')
        filename = save_dir.joinpath(date.strftime("%Y%m%d")).with_suffix('.json')
        if not filename.exists():
            if request_count % RESTART_SESSION_COUNT == 0:
                session = create_session()
            url = create_request_url(date)
            while not download_single(session, url, filename, timeout, True):
                request_count += 1
        else:
            print('Json Exist')
    return True