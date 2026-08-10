import enum
import datetime

class SQLtype(enum.StrEnum):
    null = 'NULL'
    int = 'INTEGER'
    float = 'REAL'
    str = 'TEXT'
    binary = 'BLOB'

class Field(str):
    SQLTypeMapping = {
        str: SQLtype.str,
        datetime.date: SQLtype.str,
        datetime.datetime: SQLtype.str,
        int: SQLtype.int,
        bool: SQLtype.int,
        float: SQLtype.float,
        None: SQLtype.null
    }

    def __new__(cls, text, dtype, default=None):
        obj = super().__new__(cls, text)
        obj.pytype = dtype
        obj.sqltype = cls.SQLTypeMapping[dtype]
        if not default is None:
            obj.default = dtype(default)
        else:
            obj.default = None
        return obj
    
class FieldGroupMeta(type):
    def __new__(mcls, name, bases, namespace:dict):
        items = {}
        for k, v in namespace.items():
            if isinstance(v, Field):
                items[k] = v
        namespace['items'] = items
        cls = super().__new__(mcls, name, bases, namespace)
        return cls
    
class FieldGroup(metaclass=FieldGroupMeta):
    pass

class DisabledMeta(FieldGroupMeta):
    def __getattribute__(cls, name):
        if name in {
            "__name__", "__qualname__", "__module__",
            "__doc__", "__class__"
        }:  
            return super().__getattribute__(name)
        else:
            raise RuntimeError(f"{cls.__name__} 尚未啟用")

def disabled(cls):
    return DisabledMeta(
        cls.__name__,
        cls.__bases__,
        dict(cls.__dict__)
    )