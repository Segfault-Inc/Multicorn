from abc import ABCMeta, abstractmethod
from .request import RequestProperty, make_request_property
from .property import Property
import __builtin__
import operator


class base_func(object):

    __metaclass__ = ABCMeta

    def __init__(self, property):
        self.property = make_request_property(property)

    def return_property(self, properties):
        return properties.get(self.property.name, None)




class transform_func(base_func, RequestProperty):


    @property
    def child_property(self):
        if self.property.child_property:
            child = self._copy()
            child.property = self.property.child_property
            return child
        return None

    def _copy(self):
        return self.__class__(self.property)

    @property
    def name(self):
        return self.property.name

    def get_value(self, item):
        return self(self.property.get_value(item))


class base_aggregate(base_func):

    def initializer(self, properties):
        return properties[self.property.name].type()

    @abstractmethod
    def __call__(self, accumulator, value):
        """An aggregate function must be suitable to be called through a "reduce"
        call.
        """
        pass

class sum(base_aggregate):

    def __call__(self, accumulator, item):
        return accumulator + self.property.get_value(item)

class count(base_aggregate):

    def initializer(self, properties):
        return 0

    def return_property(self, properties):
        return Property(int)

    def __init__(self):
        pass

    def __call__(self, accumulator, value):
        return accumulator + 1

class max(base_aggregate):

    def initializer(self, property):
        return None

    def __call__(self, accumulator, item):
        if accumulator is None:
            return self.property.get_value(item)
        return __builtin__.max(accumulator, self.property.get_value(item))

class min(base_aggregate):

    def initializer(self, property):
        return None

    def __call__(self, accumulator, item):
        if accumulator is None:
            return self.property.get_value(item)
        return __builtin__.min(accumulator, self.property.get_value(item))

class slice(transform_func):

    def _copy(self):
        return self.__class__(self.property, [self.range.start, self.range.stop])

    def __init__(self, property_name, select_range):
        super(slice, self).__init__(property_name)
        if hasattr(select_range, "__iter__"):
            self.range = __builtin__.slice(*select_range)
        else:
            self.range= __builtin__.slice(select_range)

    def __call__(self, value):
        return value[self.range] if value else value

class coalesce(transform_func):

    def __init__(self, property_name, replacement):
        super(coalesce, self).__init__(property_name)
        self.replacement = replacement

    def _copy(self):
        return self.__class__(self.property, self.replacement)

    def __call__(self, value):
        if value is None:
            return self.replacement
        return value

class constant(transform_func):

    def __init__(self, constant):
        self.constant = constant

    def _copy(self):
        return self.__class__(self.constant)

    def get_value(self, Item):
        return self.constant

    def return_property(self, value):
        return Property(type(self.constant))

    @property
    def name(self):
        return "constant(%s)" % self.constant

    @property
    def child_property(self):
       return None


class upper(transform_func):

    def __call__(self, value):
        if value is None:
            return value
        return value.upper()


class lower(transform_func):

    def __call__(self, value):
        if value is None:
            return value
        return value.lower()



class extract(transform_func):

    def __init__(self, property_name, field):
        super(extract, self).__init__(property_name)
        self.field = field

    def return_property(self, properties):
        # TODO: manage various datatypes. Currently, only date is
        # supported, yielding integers
        return Property(int)

    def _copy(self):
        return self.__class__(self.property, self.field)

    def __call__(self, value):
        if value is None:
            return None
        else:
            return getattr(value, self.field)

class multi_column_transform(transform_func):

    def __init__(self, *args):
        self.properties = [make_request_property(prop) for prop in args]

    def _copy(self):
        return self.__class__(*self.properties)

    @property
    def name(self):
        return "%s(%s)" % (self.__class__, [prop.name for prop in self.properties])

    def return_property(self, properties):
        # A multi column property has the same type as the first property
        return Property(properties.get(self.properties[0].name).type)

    @property
    def child_property(self):
        return None

class standard_operator_func(multi_column_transform):

    def get_value(self, item):
        return reduce(self.OPERATOR, [property.get_value(item)
            for property in self.properties])


class product(standard_operator_func):

    OPERATOR = operator.__mul__

class addition(standard_operator_func):

    OPERATOR = operator.__add__

class substraction(standard_operator_func):

    OPERATOR = operator.__sub__

class division(standard_operator_func):

    OPERATOR = operator.__div__
