def check_raw_material(material, save_path, t1, t2):
    stop = material.check_batch(save_path, t1, t2)
    print('Check Finished')
    return stop

def remove_inconsistent_raw_material(material, save_path, t1, t2):
    material.remove_inconsistent_files(save_path, t1, t2)
    print('Remove Finished')