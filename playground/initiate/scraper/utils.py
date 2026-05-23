import time

def download_raw_material(material, start_date, end_date, save_path, iterations, cooldown_time):
    for iteration in iterations:
        finish = False
        while not finish:
            try:
                finish = material.download_by_date_range(
                    start_date, end_date, save_path, iteration
                )
            except Exception as err:
                print('some unexpected error occurred')
                print(err)
                print(f'cooldown for {cooldown_time} hrs')
                time.sleep(cooldown_time * 60 *60)
    print('Download Finished')