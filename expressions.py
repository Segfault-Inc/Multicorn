# coding: utf8
import operator

class AbstractExpression(object):
    pass

# Dynamically add methods to AbstractExpression.
# Include these? index, concat, contains
for name in '''lt le eq ne ge gt abs add and div floordiv invert lshift mod mul
               neg or pos pow rshift sub truediv xor'''.split():
    name = '__%s__' % name
    # Use a closure here to hold each `operator_function`
    def method_factory(operator_function):
        # Use *args as some methods take one parameter (other than self)
        # and others zero. The method’s `self` is also in *args
        def magic_method(*args):
            return Operation(operator_function, *args)
        magic_method.__name__ = operator_function.__name__
        return magic_method
    setattr(AbstractExpression, name, method_factory(getattr(operator, name)))

# Reversed operator: eg. 1 + r.foo => r.foo.__radd__(1)
# Include divmod?
for name in '''add sub mul floordiv div truediv mod pow lshift rshift and or
                xor'''.split():
    r_name = '__r%s__' % name
    name = '__%s__' % name
    # Use a closure here to hold each `operator_function`
    def method_factory(operator_function):
        def magic_method(self, other):
            # Reversed argument order
            return Operation(operator_function, other, self)
        magic_method.__name__ = operator_function.__name__
        return magic_method
    setattr(AbstractExpression, r_name, method_factory(getattr(operator, name)))


class Operation(AbstractExpression):
    def __init__(self, operator_function, *args):
        self.operator_function = operator_function
        self.args = args

    def __repr__(self):
        return '%s(%s, %r)' % (self.__class__.__name__, self.operator_function,
                                self.args)


class Variable(AbstractExpression):
    def __init__(self, name):
        self.name = name
        
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.name)


class Root(object):
    def __getattr__(self, name):
        return Variable(name)

r = Root()


def evaluate(expression, namespace):
    if isinstance(expression, Variable):
        return namespace[expression.name]
    elif isinstance(expression, Operation):
        args = [evaluate(arg, namespace) for arg in expression.args]
        return expression.operator_function(*args)
    else:
        # Literal value
        return expression

if __name__ == '__main__':
    x_max = 70
    y_max = 20
    fall = (-(20 - r.x)) ** 2 / 50. + 2.5
    world = [[' '] * x_max for i in xrange(y_max)]
    for x in xrange(x_max):
        y = int(evaluate(fall, {'x': x}))
        if 0 <= y < y_max:
            world[y][x] = '#'
    print '\n'.join(''.join(line) for line in world)

