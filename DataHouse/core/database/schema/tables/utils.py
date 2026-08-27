from ..fields.utils import FieldGroup

class MetaTable(type):
    def __new__(mcls, name, bases, namespace:dict):
        columns = {}

        for b in bases:
            if isinstance(b, MetaTable):
                columns.update(b.columns)

        for v in namespace.values():
            if isinstance(v, type):
                if issubclass(v, FieldGroup):
                    columns.update(v.items)
        
        namespace.update({'columns':columns})
        namespace.setdefault('__primary_keys__', [])
        namespace.setdefault('__additional_index__', [])

        cls = super().__new__(mcls, name, bases, namespace)
        cls.__name__ = cls.__name__.lower()
        return cls