import json

class Checker:

    def equal(self, t1st, t2nd):
        raise NotImplementedError

    def save(self, save_path, t1, t2, diff):
        save_path = save_path.joinpath('diff.json')
        if save_path.exists():
            with open(save_path) as f:
                prev = json.load(f)
        else:
            prev = {}
        prev[f'{t1},{t2}'] = diff
        with open(save_path, 'w') as f:
            json.dump(prev, f, indent=4)

    def check_batch(self, save_path, t1, t2):
        target1 = sorted(save_path.joinpath(t1).iterdir())
        target2 = sorted(save_path.joinpath(t2).iterdir())
        assert set(x.name for x in target1) == set(x.name for x in target2)
        diff = []
        for t1st,t2nd in zip(target1, target2):
            if not self.equal(t1st,t2nd):
                diff.append(t1st.stem)
        self.save(save_path, t1, t2, diff)
    
    def check_intersection(self, save_path, t1, t2, skip=[]):
        target1 = {x.stem: x for x in save_path.joinpath(t1).iterdir()}
        target2 = {x.stem: x for x in save_path.joinpath(t2).iterdir()}
        intersection = set(target1.keys()).intersection(set(target2.keys()))
        print(f'check count before skip: {len(intersection)}')
        intersection = intersection - set(skip)
        print(f'check count after skip: {len(intersection)}')
        print(f'skip count: {len(skip)}')
        diff = []
        for i in sorted(intersection):
            if not self.equal(target1[i],target2[i]):
                diff.append(i)
        self.save(save_path, t1, t2, diff)

