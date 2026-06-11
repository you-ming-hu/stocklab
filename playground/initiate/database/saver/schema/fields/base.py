import enum

class Field(str, enum.Enum):
    def __new__(cls, value, dtype):
        print(value)
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.pytype, obj.sqltype = dtype
        return obj