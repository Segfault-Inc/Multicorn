# coding: utf8
import operator


def _ensure_expression(expression):
    """
    Ensure that the given parameter is an Expression. If it is not, wrap it
    in a Literal.
    """
    if isinstance(expression, Expression):
        return expression
    else:
        return Literal(expression)


class Expression(object):
    """
    Abstract base class for Operation, Variable and Literal
    """
    
    # Since bool inherits from int, `&` and `|` (bitwise `and` and `or`)
    # behave as expected on booleans

    # Magic method for `self & other`, not `self and other`
    def __and__(self, other):
        other = _ensure_expression(other)
        # Simplify logic when possible
        for a, b in ((self, other), (other, self)):
            if isinstance(a, Literal):
                if a.value:
                    return b # True is the neutral element of and
                else:
                    return Literal(False) # False is the absorbing element
        return Operation(operator.and_, self, other)
    
    # Magic method for `self | other`, not `self or other`
    def __or__(self, other):
        other = _ensure_expression(other)
        # Simplify logic when possible
        for a, b in ((self, other), (other, self)):
            if isinstance(a, Literal):
                if a.value:
                    return Literal(True) # True is the absorbing element of or
                else:
                    return b # False is the neutral element
        return Operation(operator.or_, self, other)

    # `&` and `|` are commutative
    __rand__ = __and__
    __ror__ = __or__
    
    # However `~` (bitwise invert) does not behave as a logical `not`
    # on booleans:
    #   (~True, ~False) == (~1, ~0) == (-2, -1)
    # We want to use `~` on expressions as the logical `not`.
    def __invert__(self):
        # Simplify logic when possible
        if isinstance(self, Literal):
            return Literal(not self.value)
        # Use `not_` instead of `invert` here:
        return Operation(operator.not_, self)
    
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
                _ensure_expression(arg) for arg in args))
        magic_method.__name__ = name
        setattr(cls, name, magic_method)

# Dynamically add methods to AbstractExpression.
# Include these? abs, index, concat, contains, divmod
for names, reverse in (
        ('''lt le eq ne ge gt add div floordiv lshift mod mul
            neg pos pow rshift sub truediv''', False),
        # Reversed operators: eg. 1 + r.foo => r.foo.__radd__(1)
        ('''add sub mul floordiv div truediv mod pow lshift rshift''', True)):
    for name in names.split():
        Expression._add_magic_method(
            name, getattr(operator, '__%s__' % name), reverse)


class Operation(Expression):
    def __init__(self, operator_function, *args):
        self.operator_function = operator_function
        self.args = args
        self._affected_variables = None # Not computed yet.

    @property
    def name(self):
        """
        The name of the operator function without leading or trailing
        underscores.
        """
        return self.operator_function.__name__.strip('_')

    def __repr__(self):
        # Make a list to avoid the trailing comma in one-element tuples.
        return 'Op(%s, %s)' % (self.name, repr(list(self.args))[1:-1])

    def evaluate(self, namespace):
        # Some operators don’t like *generator
        args = tuple(arg.evaluate(namespace) for arg in self.args)
        return self.operator_function(*args)

    def affected_variables(self):
        """
        Return the set of the variables affected by this expression:
        
            >>> sorted((r.foo + r.bar + 4).affected_variables())
            ['bar', 'foo']
        """
        if self._affected_variables is None:
            self._affected_variables = frozenset(
                name
                for arg in self.args
                for name in arg.affected_variables())
        return self._affected_variables


class Variable(Expression):
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'Var(%s)' % self.name

    def evaluate(self, namespace):
        return namespace[self.name]

    def affected_variables(self):
        return (self.name,)


class Literal(Expression):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return repr(self.value)
#        return 'Lit(%r)' % (self.value,)
    
    def evaluate(self, namespace):
        return self.value

    def affected_variables(self):
        return () # empty tuple


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

