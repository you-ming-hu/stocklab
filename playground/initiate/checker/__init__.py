import json

class Checker:
    DIFF_FILENAME = 'inconsistent.json'
    DIFF_KEY = 'files'

    def equal(self, t1st, t2nd):
        raise NotImplementedError
    
    def read(self, path):
        raise NotImplementedError

    def compare(self, c1, c2):
        raise NotImplementedError
    
    def try_read_file(self, path):
        try:
            content = self.read(path)
        except:
            print(f'error file: {path}')
            raise Exception
        return content
    
    def equal(self, t1, t2):
        try:
            c1 = self.try_read_file(t1)
            c2 = self.try_read_file(t2)
        except:
            return False
        return self.compare(c1, c2)
    
    def read_diff(self, save_path):
        DIFF_FILENAME = self.DIFF_FILENAME
        DIFF_KEY = self.DIFF_KEY
        save_path = save_path.joinpath(DIFF_FILENAME)
        with open(save_path) as f:
            diff = json.load(f)[DIFF_KEY]
        return diff

    def save_diff(self, save_path, diff):
        DIFF_FILENAME = self.DIFF_FILENAME
        DIFF_KEY = self.DIFF_KEY
        save_path = save_path.joinpath(DIFF_FILENAME)
        diff = {DIFF_KEY: diff}
        with open(save_path, 'w') as f:
            json.dump(diff, f, indent=4)

    def get_targets(self, save_path, t1, t2):
        target1 = {x.stem:x for x in sorted(save_path.joinpath(t1).iterdir())}
        target2 = {x.stem:x for x in sorted(save_path.joinpath(t2).iterdir())}
        assert set(target1.keys()) == set(target2.keys())
        keys = sorted(target1.keys())
        return keys, target1, target2

    def check_batch(self, save_path, t1, t2):
        keys, target1, target2 = self.get_targets(save_path, t1, t2)
        try:
            keys = self.read_diff(save_path)
        except FileNotFoundError:
            pass
        print(f'check keys count: {len(keys)}')
        diff = []
        for i,k in enumerate(keys):
            print(f'{k}, {i+1}/{len(keys)}')
            if not self.equal(target1[k],target2[k]):
                diff.append(k)
        self.save_diff(save_path, diff)
        print(f'left diff count: {len(diff)}')
        stop = len(diff) == 0
        return stop

    def remove_inconsistent_files(self, save_path, t1, t2):
        keys, target1, target2 = self.get_targets(save_path, t1, t2)
        diff = self.read_diff(save_path)
        print(f'remove count: {len(diff)}')
        for d in diff:
            target1[d].unlink()
            print(f'removed: {target1[d]}')
            target2[d].unlink()
            print(f'removed: {target2[d]}')
        


