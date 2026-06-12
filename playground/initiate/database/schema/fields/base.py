import enum

class Field(enum.StrEnum):
    def __new__(cls, value, dtype):
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.pytype, obj.sqltype = dtype
        return obj