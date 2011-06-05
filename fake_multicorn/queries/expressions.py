# coding: utf8
import operator


class Expression(object):
    """
    Abstract base class for Operation, Variable and Literal
    """
    
    @classmethod
    def _add_magic_method(cls, name, operator_function, reverse=False):
        name = '__%s%s__' % (('r' if reverse else ''), name)
        # Define the method inside a function so that the closure will hold
        # each value of `operator_function` and `reverse`
        def magic_method(*args):
            # `*args` here includes the method’s `self`
            if reverse:
                args = args[::-1]
            return Operation(operator_function, *(
                arg if isinstance(arg, Expression) else Literal(arg)
                for arg in args))
        magic_method.__name__ = name
        setattr(cls, name, magic_method)

# Dynamically add methods to AbstractExpression.
# Include these? index, concat, contains, divmod
for names, reverse in (
        ('''lt le eq ne ge gt abs add and div floordiv invert lshift mod mul
            neg or pos pow rshift sub truediv xor''', False),
        # Reversed operators: eg. 1 + r.foo => r.foo.__radd__(1)
        ('''add sub mul floordiv div truediv mod pow lshift rshift and or
            xor''', True)):
    for name in names.split():
        Expression._add_magic_method(
            name, getattr(operator, '__%s__' % name), reverse)



class Operation(Expression):
    def __init__(self, operator_function, *args):
        self.operator_function = operator_function
        self.args = args
        self._affected_variables = None # Not computed yet.

    def __repr__(self):
        return 'Op(%s, %r)' % (self.operator_function.__name__, self.args)

    def evaluate(self, namespace):
        # Some operators don’t like *generator
        args = tuple(arg.evaluate(namespace) for arg in self.args)
        return self.operator_function(*args)


class Variable(Expression):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'Var(%s)' % self.name

    def evaluate(self, namespace):
        return namespace[self.name]


class Literal(Expression):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return repr(self.value)
    
    def evaluate(self, namespace):
        return self.value


class Root(object):
    def __getattr__(self, name):
        return Variable(name)

r = Root()


if __name__ == '__main__':
    x_max = 70
    y_max = 20
    fall = (-(20 - r.x)) ** 2 / 50. + 2.5
    world = [[' '] * x_max for i in xrange(y_max)]
    for x in xrange(x_max):
        y = int(fall.evaluate({'x': x}))
        if 0 <= y < y_max:
            world[y][x] = '#'
    print '\n'.join(''.join(line) for line in world)

