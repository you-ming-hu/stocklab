import requests
import pandas as pd
import time
import json
import pathlib
import random

class Scraper:

    def create_session(self):
        raise NotImplementedError
    
    def create_request_url(self):
        raise NotImplementedError
    
    def create_session_template(self, header):
        session = requests.Session()
        session.headers.update(header)
        return session

    def download(self, session, url, filename, timeout, cooldown_if_abnormal=False):
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

    def download_single(self, session, url, filename, timeout, cooldown_if_abnormal=False):
        MIN_SLEEP_TIME = 2
        MAX_SLEEP_TIME = 4
        success = self.download(session, url, filename, timeout, cooldown_if_abnormal)
        sleep_time = random.uniform(MIN_SLEEP_TIME, MAX_SLEEP_TIME)
        print(f'Sleep: {sleep_time:.1f}')
        time.sleep(sleep_time)
        return success

    def download_batch(self, start_date, end_date, freq, save_dir, stage, timeout=10):
        RESTART_SESSION_COUNT = 1000

        save_dir = pathlib.Path(save_dir, stage)
        save_dir.mkdir(parents=True, exist_ok=True)

        dates = pd.date_range(start_date, end_date, freq=freq)
        request_count = 0
        for date in dates:
            print(date, end='\t')
            filename = save_dir.joinpath(date.strftime("%Y%m%d")).with_suffix('.json')
            if not filename.exists():
                if request_count % RESTART_SESSION_COUNT == 0:
                    session = self.create_session()
                url = self.create_request_url(date)
                while not self.download_single(session, url, filename, timeout, True):
                    request_count += 1
            else:
                print('Json Exist')
        return True
