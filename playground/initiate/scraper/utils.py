
import time

def download_raw_material(material, start_date, end_date, save_path, cooldown_time):
    stages = ['1st', '2nd']
    for stage in stages:
        finish = False
        while not finish:
            try:
                finish = material.download_batch(
                    start_date, end_date, save_path, stage
                )
            except Exception as err:
                print('some unexpected error occurred')
                print(err)
                print(f'cooldown for {cooldown_time} hrs')
                time.sleep(cooldown_time * 60 *60)
    print('Finished')