import requests
import pandas as pd
import time
import pathlib
import random
import json
from bs4 import BeautifulSoup

class Scraper:
    RESTART_SESSION_COUNT = 1000
    MIN_SLEEP_TIME = 2
    MAX_SLEEP_TIME = 4
    COOLDOWN_TIME = 5 * 60

    def __init__(self, freq, suffix):
        self.freq = freq
        self.suffix = suffix
    
    def create_request_info(self, date):
        raise NotImplementedError
    
    def request(self, session, request_info, timeout):
        raise NotImplementedError

    def save(self, res, filename):
        data = res.json()
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    def create_session(self, header):
        session = requests.Session()
        session.headers.update(header)
        return session

    def download(self, session, request_info, filename, timeout, cooldown_if_abnormal=False):
        COOLDOWN_TIME = self.COOLDOWN_TIME
        try:
            print('Requesting', end='\t')
            res = self.request(session, request_info, timeout)

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
            self.save(res, filename)
        
        except Exception as err:
            print(err)
            if cooldown_if_abnormal:
                print(f'cool down for: {COOLDOWN_TIME} secs')
                time.sleep(COOLDOWN_TIME)
            return False

        print('Finisih', end='\t')
        return True

    def download_single(self, session, request_info, filename, timeout, cooldown_if_abnormal=False):
        MIN_SLEEP_TIME = self.MIN_SLEEP_TIME
        MAX_SLEEP_TIME = self.MAX_SLEEP_TIME
        success = self.download(session, request_info, filename, timeout, cooldown_if_abnormal)
        sleep_time = random.uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME)
        print(f'Sleep: {sleep_time:.1f}')
        time.sleep(sleep_time)
        return success
    
    def download_batch(self, dates, save_dir, iteration, timeout=10):
        RESTART_SESSION_COUNT = self.RESTART_SESSION_COUNT
        save_dir = pathlib.Path(save_dir, iteration)
        save_dir.mkdir(parents=True, exist_ok=True)

        request_count = 0
        for date in dates:
            date = pd.Timestamp(date)
            print(date, end='\t')
            filename = save_dir.joinpath(date.strftime("%Y%m%d")).with_suffix(self.suffix)
            if not filename.exists():
                if request_count % RESTART_SESSION_COUNT == 0:
                    session = self.create_session()
                request_info = self.create_request_info(date)
                while not self.download_single(session, request_info, filename, timeout, True):
                    request_count += 1
            else:
                print(f'{self.suffix.replace('.','').upper()} Exist')
        return True

    def download_by_date_range(self, start_date, end_date, save_dir, iteration, timeout=10):
        if start_date is None:
            start_date = pd.Timestamp.today().date()
        if end_date is None:
            end_date = pd.Timestamp.today().date()
        dates = pd.date_range(start_date, end_date, freq=self.freq)
        self.download_batch(dates, save_dir, iteration, timeout)
        return True
    
class IndustryScraper(Scraper):

    def __init__(self, suffix='.json'):
        super().__init__('D', suffix)

    def create_session(self):
        header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        return super().create_session(header)
    
    def create_request_info(self, stock_id):
        url = f"https://ic.tpex.org.tw/company_chain.php?stk_code={stock_id}"
        return url
    
    def request(self, session, request_info, timeout):
        res = session.get(request_info, timeout=timeout)
        return res
    
    def download_batch(self, dates, save_dir, iteration, timeout=10):
        RESTART_SESSION_COUNT = self.RESTART_SESSION_COUNT
        save_dir = pathlib.Path(save_dir, iteration)
        save_dir.mkdir(parents=True, exist_ok=True)

        with open(self.company_table_path) as f:
            table = json.load(f)
        stocks = self.get_company_ids(table)

        request_count = 0
        for i, stock in enumerate(stocks):
            print(f'{stock}, {i}/{len(stocks)}', end='\t')
            filename = save_dir.joinpath(stock).with_suffix(self.suffix)
            if not filename.exists():
                if request_count % RESTART_SESSION_COUNT == 0:
                    session = self.create_session()
                request_info = self.create_request_info(stock)
                while not self.download_single(session, request_info, filename, timeout, True):
                    request_count += 1
            else:
                print(f'{self.suffix.replace('.','').upper()} Exist')
        return True
    
    def find_latest_company_table(self, path, suffix='.json'):
        self.company_table_path = sorted(pathlib.Path(path).glob('*'+suffix))[-1]

    def save(self, res, filename):
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, 'html.parser')
        table = soup.find('body').find('center').find('div', 'main-panel').find('div', 'content-panel-main').find('div', 'content').find_all('h4')
        data = [l.text.replace('►','').replace('\xa0','').split('>') for l in table[1:]]
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def get_company_ids(self, table):
        raise NotImplementedError