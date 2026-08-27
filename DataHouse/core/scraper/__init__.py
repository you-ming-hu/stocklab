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

    def create_request_date(self, date, is_taiwanese, sep):
        date = pd.Timestamp(date)
        if not is_taiwanese:
            date = date.strftime(sep.join(['%Y','%m','%d']))
        else:
            date = sep.join([
                f'{date.year-1911}',
                f'{date.month:0>2}',
                f'{date.day:0>2}'
            ])
        return date
    
    def create_request_info(self, date):
        raise NotImplementedError
    
    def request(self, session, request_info, method, timeout):
        if method == 'get':
            url, params = request_info
            res = getattr(session, method)(url, params=params, timeout=timeout)
        elif method == 'post':
            url, data = request_info
            res = getattr(session, method)(url, data=data, timeout=timeout)
        else:
            assert False, f'not recognizable request method: {method}'
        return res

    def save(self, res, filename, sup=None):
        data = res.json()
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    def create_session(self, header):
        session = requests.Session()
        session.headers.update(header)
        return session

    def download(self, session, request_info, filename, timeout, cooldown_if_abnormal=False, sup=None):
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
            self.save(res, filename, sup)
        
        except Exception as err:
            print(err)
            if cooldown_if_abnormal:
                print(f'cool down for: {COOLDOWN_TIME} secs')
                time.sleep(COOLDOWN_TIME)
            return False

        print('Finisih', end='\t')
        return True

    def download_single(self, session, request_info, filename, timeout, cooldown_if_abnormal=False, sup=None):
        MIN_SLEEP_TIME = self.MIN_SLEEP_TIME
        MAX_SLEEP_TIME = self.MAX_SLEEP_TIME
        success = self.download(session, request_info, filename, timeout, cooldown_if_abnormal, sup)
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
        if end_date is None:
            end_date = pd.Timestamp.today().date()
        else:
            end_date = pd.Timestamp(end_date)
        if start_date is None:
            start_date = pd.Timestamp.today().date()
        elif isinstance(start_date, int):
            start_date = end_date + pd.Timedelta(start_date, 'day')
        else:
            start_date = pd.Timestamp(start_date)        
        dates = pd.date_range(start_date, end_date, freq=self.freq)
        self.download_batch(dates, save_dir, iteration, timeout)
        return True

class CompanyInfoScraper(Scraper):
    MIN_SLEEP_TIME = 0
    MAX_SLEEP_TIME = 0.1

    def __init__(self, suffix='.json'):
        super().__init__('D', suffix)
        self.overview_file_name = 'overview.json'
        self.company_folder_name = 'companies'

    def create_overview_session(self):
        raise NotImplementedError

    def create_overview_request_info(self):
        raise NotImplementedError

    def overview_request(self, session, request_info, timeout):
        raise NotImplementedError

    def parse_company_table(self, table, mapping):
        df = pd.DataFrame(table)
        df = df.loc[:, [c for c in mapping.keys()]]
        df.columns = [mapping[c] for c in df.columns]
        for c in df.columns:
            df[c] = df[c].str.replace(' ','').str.replace('*','')
        return df

    def create_company_session(self):
        header = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        return super().create_session(header)
    
    def create_company_request_info(self, stock_id):
        url = 'https://ic.tpex.org.tw/company_chain.php'
        param = {'stk_code':stock_id}
        return url, param

    def request(self, session, request_info, method, timeout):
        return super().request(session, request_info, 'get', timeout)
    
    def download_batch(self, dates, save_dir, iteration, timeout=10):
        assert len(dates) == 1, 'the dates parameter is just a placeholder for formality, multiple dates is invalid'
        RESTART_SESSION_COUNT = self.RESTART_SESSION_COUNT
        save_dir = pathlib.Path(save_dir, iteration, dates[0].strftime("%Y%m%d"))
        company_dir = save_dir.joinpath(self.company_folder_name)
        company_dir.mkdir(parents=True, exist_ok=True)

        overview_path = save_dir.joinpath(self.overview_file_name)
        overview_session = self.create_overview_session()
        overview_request_info = self.create_overview_request_info()
        overview_res = self.overview_request(overview_session, overview_request_info, timeout)
        super().save(overview_res, overview_path)

        with open(overview_path) as f:
            company_table = json.load(f)

        company_table = self.parse_company_table(company_table)
        company_session = self.create_company_session()

        request_count = 0
        for i, row in company_table.iterrows():
            company_id = row['代號']
            print(f'{company_id}, {i}/{len(company_table)}', end='\t')
            filename = company_dir.joinpath(company_id).with_suffix(self.suffix)
            if not filename.exists():
                if request_count % RESTART_SESSION_COUNT == 0:
                    company_session = self.create_company_session()
                company_request_info = self.create_company_request_info(company_id)
                while not self.download_single(company_session, company_request_info, filename, timeout, True, sup=row):
                    request_count += 1
            else:
                print(f'{self.suffix.replace('.','').upper()} Exist')
        return True
    
    def save(self, res, filename, sup):
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, 'html.parser')
        table = soup.find('body').find('center').find('div', 'main-panel').find('div', 'content-panel-main').find('div', 'content').find_all('h4')
        data = [l.text.replace('►','').replace('\xa0','').split('>') for l in table[1:]]
        data = [{'營運產業':d[0], '題材':d[1]} for d in data]
        data = [sup.to_dict()|d for d in data]
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)