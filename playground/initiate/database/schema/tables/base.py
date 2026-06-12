from ..fields import Field

class MetaTable(type):
    def __new__(mcls, name, bases, namespace:dict):
        cols = {}
        fs = {}

        for b in bases:
            if isinstance(b, MetaTable):
                cols.update(b.cols)
                fs.update(b.fs)

        for k, v in namespace.items():
            if isinstance(v, type):
                if issubclass(v, Field):
                    cols.update(v._member_map_)
                    fs.update({k:v.__chinese__})
        namespace.update(cols)
        namespace.update({'cols':cols})
        namespace.update({'fs':fs})
        namespace.setdefault('__primary_keys__', [])
        namespace.setdefault('__additional_index__', [])
        cls = super().__new__(mcls, name, bases, namespace)
        cls.__name__ = cls.__name__.lower()
        return cls