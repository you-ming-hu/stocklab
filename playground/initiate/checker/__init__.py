import datetime
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
        date = str(datetime.date.today())
        key = f'{t1},{t2}'
        if date in prev:
            prev[date][key] = diff
        else:
            prev[date] = {key: diff}
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
        return diff