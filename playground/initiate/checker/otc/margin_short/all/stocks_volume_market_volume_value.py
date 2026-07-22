import re

from ...base import OTCChecker

class VERSION_0(OTCChecker):
    
    def read(self, path):
        return path.read_text(encoding='utf-8')
    
    def standardize(self, c):
        c = re.sub(r'<script>.*?</script>', '', c)
        return c
    
    def remove_inconsistent_files(self, save_path, t1, t2):
        keys, target1, target2 = self.get_targets(save_path, t1, t2)
        diff = self.read_diff(save_path)
        print(f'theoretical remove count: {len(diff)}')
        for d in diff:
            for target in [target1[d], target2[d]]:
                if '嚙' in target.read_text(encoding='utf-8'):
                    target.unlink()
                    print(f'removed: {target}')
    
version_0 = VERSION_0()

class VERSION_1(OTCChecker):
    pass

version_1 = VERSION_1()
