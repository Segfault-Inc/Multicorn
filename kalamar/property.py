import decimal
import io
import kalamar

PROPERTY_TYPES = set([str, int, float, decimal.Decimal, io.IOBase, kalamar.item.Item])

class Property(object):
    def __init__(self,  property_type, identity=False, auto=False,
                 default=None, mandatory=False, relation=None, remote_ap=None,
                 remote_property=None):
        self.property_type = property_type
        self.identity = identity
        self.auto = auto
        self.default = default
        self.remote_ap = remote_ap
        self.mandatory = mandatory
        self.relation = relation
        self.remote_property = remote_property
        self.validate()

    def validate(self):
        if self.relation:
            if not (self.remote_ap):
                raise RuntimeError('Invalid property definition ')
            if self.relation == 'one-to-many':
                if not self.remote_property:
                    raise RuntimeError('Invalid property definition')


