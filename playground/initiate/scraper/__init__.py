import requests
import pandas as pd
import time
import pathlib
import random

class Scraper:
    def __init__(self, freq, suffix):
        self.freq = freq
        self.suffix = suffix

    def create_session(self):
        raise NotImplementedError
    
    def create_request_info(self, date):
        raise NotImplementedError
    
    def request(self, session, request_info, timeout):
        raise NotImplementedError

    def save(self, res, filename):
        raise NotImplementedError
    
    def create_session_template(self, header):
        session = requests.Session()
        session.headers.update(header)
        return session

    def download(self, session, request_info, filename, timeout, cooldown_if_abnormal=False):
        COOLDOWN_TIME = 5 * 60
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
        MIN_SLEEP_TIME = 2
        MAX_SLEEP_TIME = 4
        success = self.download(session, request_info, filename, timeout, cooldown_if_abnormal)
        sleep_time = random.uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME)
        print(f'Sleep: {sleep_time:.1f}')
        time.sleep(sleep_time)
        return success
    
    def download_group(self, dates, save_dir, stage, timeout=10):
        save_dir = pathlib.Path(save_dir, stage)
        save_dir.mkdir(parents=True, exist_ok=True)

        RESTART_SESSION_COUNT = 1000
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

    def download_batch(self, start_date, end_date, save_dir, stage, timeout=10):
        dates = pd.date_range(start_date, end_date, freq=self.freq)
        self.download_group(dates, save_dir, stage, timeout)
        return True