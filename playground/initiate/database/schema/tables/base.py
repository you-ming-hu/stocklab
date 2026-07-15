from . import utils
from .. import fields

class Table(metaclass = utils.MetaTable):
    f_datatimestamp = fields.DataTimestamp
