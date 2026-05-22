def check_raw_material(material, save_path, t1, t2):
    material.check_batch(save_path, t1, t2)
    print('Finished')

def check_intersection(material, save_path, t1, t2, skip=[]):
    material.check_intersection(save_path, t1, t2, skip)
    print('Finished')
