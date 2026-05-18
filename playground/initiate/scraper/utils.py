
import time
import json

def handle_exception(err, cooldown_time):
    print('some unexpected error occurred')
    print(err)
    print(f'cooldown for {cooldown_time} hrs')
    time.sleep(cooldown_time * 60 *60)

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
                handle_exception(err, cooldown_time)
                
    print('Finished')

def download_group_material(material, save_path, stage, keys, cooldown_time):
    with open(save_path.joinpath('diff.json')) as f:
        group = json.load(f)
    for k in keys:
        group = group[k]
    finish = False
    while not finish:
        try:
            finish = material.download_group(
                group, save_path, stage
            )
        except Exception as err:
            handle_exception(err, cooldown_time)
    print('Finished')