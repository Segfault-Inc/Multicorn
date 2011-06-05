from decimal import Decimal as D
from attest import Tests, assert_hook

from .queries import Query, execute
from .queries import aggregates as a
from .queries.expressions import r, Expression, Literal
from .queries.isolate import isolate_expression, isolate_query
from . import access_point, Metadata


suite = Tests()


@suite.test
def test_logical_simplifications():
    for true, false in ((True, False), (1, 0), ('a', '')):
        assert repr(r.foo & true) == 'Var(foo)'
        assert repr(r.foo | false) == 'Var(foo)'
        assert repr(r.foo & false) == 'False'
        assert repr(r.foo | true) == 'True'

        assert repr(true & r.foo) == 'Var(foo)'
        assert repr(false | r.foo) == 'Var(foo)'
        assert repr(false & r.foo) == 'False'
        assert repr(true | r.foo) == 'True'

    assert repr(~r.foo) == 'Op(not, Var(foo))'
    assert repr(~Literal('hello')) == 'False'
    assert repr(~Literal('')) == 'True'
    
    # Augmented assignment doesn't need to be defined explicitly
    assert not hasattr(Expression, '__iadd__')
    a = b = r.foo
    a &= r.bar
    assert repr(a) == 'Op(and, Var(foo), Var(bar))'
    # No in-place mutation
    assert repr(b) == 'Var(foo)'


@suite.test
def test_queries():
    data = [
        dict(toto='foo', tata=42, price=10, tax=D('1.196')),
        dict(toto='foo', tata=6, price=12, tax=1),
        dict(toto='bar', tata=42, price=5, tax=D('1.196')),
    ]
    def exec_(query):
        return list(execute(data, query))

    # Computed column
    res = exec_(Query.select(toto=r.toto, price=r.price * r.tax))
    assert res == [
        dict(toto='foo', price=D('11.96')),
        dict(toto='foo', price=12),
        dict(toto='bar', price=D('5.98')),
    ]

    res = exec_(Query.select_also(price=r.price * r.tax))
    assert res == [
        dict(toto='foo', tata=42, price=D('11.96'), tax=D('1.196')),
        dict(toto='foo', tata=6, price=12, tax=1),
        dict(toto='bar', tata=42, price=D('5.98'), tax=D('1.196')),
    ]

    res = exec_(Query.where(r.toto == 'foo').select(price=r.price))
    assert res == [
        dict(price=10),
        dict(price=12),
    ]

    # Test chaining two Query objects
    res = exec_(Query.where(r.toto == 'foo') + Query.select(price=r.price))
    assert res == [
        dict(price=10),
        dict(price=12),
    ]

    try:
        exec_(Query.select(price=r.price).where(r.toto == 'foo'))
    except KeyError:
        pass
    else:
        assert False, 'Expected KeyError'

    # Bitwise `and` used as a boolean `and`, not the precedence we would like
    res = exec_(Query.where((r.toto == 'foo') & (r.price > 11))
                     .select(price=r.price))
    assert res == [
        dict(price=12),
    ]

    res1 = exec_(Query.where((r.toto == 'bar') | (r.price > 11))
                      .select(price=r.price))
    res2 = exec_(Query.where((r.toto == 'bar') | ~(r.price < 11))
                      .select(price=r.price))
    assert res1 == res2 == [
        dict(price=12),
        dict(price=5),
    ]

    res = exec_(Query.sort(-r.price).select(toto=r.toto, tata=r.tata))
    assert res == [
        dict(toto='foo', tata=6),
        dict(toto='foo', tata=42),
        dict(toto='bar', tata=42),
    ]

    res = exec_(Query[1:].select(toto=r.toto, tata=r.tata))
    assert res == [
        dict(toto='foo', tata=6),
        dict(toto='bar', tata=42),
    ]

    res = exec_(Query[-2:].select(toto=r.toto, tata=r.tata))
    assert res == [
        dict(toto='foo', tata=6),
        dict(toto='bar', tata=42),
    ]

    try:
        Query[1]
    except ValueError:
        pass
    else:
        assert False, 'Expected ValueError'

    res = exec_(Query.aggregate(price=a.Sum(r.price * 2)))
    assert res == [dict(price=54)]

    res = exec_(Query[1:].aggregate(price=a.Sum(r.price * 2)))
    assert res == [dict(price=34)]

    res = exec_(Query.aggregate(a.By(toto='_' + r.toto + '!'),
                                price=a.Sum(r.price * 2)))
    assert res == [
        dict(toto='_foo!', price=44),
        dict(toto='_bar!', price=10),
    ]

    res = exec_(Query.aggregate(a.By(tata=r.tata), nb=a.Count(None)).sort(r.nb))
    assert res == [
        dict(tata=6, nb=1),
        dict(tata=42, nb=2),
    ]


@suite.test
def test_isolate():
    assert (r.foo * 2 + r.bar).affected_variables() == set(['foo', 'bar'])
    
    assert repr(isolate_expression(
        (r.foo == 4) & (r.bar > 0) & r.baz, ['foo', 'baz']
    )) == repr(((r.foo == 4) & r.baz, r.bar > 0))
    assert repr(isolate_expression(r.foo + 2, ['foo'])) \
        == repr((r.foo + 2, Literal(True)))
    assert repr(isolate_expression(r.foo + 2, ['bar'])) \
        == repr((Literal(True), r.foo + 2))
    
    def test_isolate_query_failure(q1, names):
        """Test that isolate_query did not isolate anything."""
        e2, q2 = isolate_query(q1, names)
        assert repr(e2) == 'True'
        assert q2.operations == q1.operations
    
    # Can only isolate a where on the start of the query
    test_isolate_query_failure(Query[1:].where(r.foo == 4), ['foo'])
    
    # Can not isolate this name in a &
    test_isolate_query_failure(Query.where(r.foo | r.bar)[1:], ['bar'])
    
    e1 = r.foo > 7
    e2 = r.foo < 10
    q1 = Query.where(e1).where(e2)[1:]
    e3, q2 = isolate_query(q1, ['foo'])
    assert repr(e3) == repr(e1 & e2)
    assert q2.operations == q1.operations[2:]

    q1 = Query.where(e1 & r.bar).where((r.bar == 0) & e2)[1:]
    e3, q2 = isolate_query(q1, ['foo'])
    assert repr(e3) == repr(e1 & e2)
    assert q2.operations[0].kind == 'where'
    assert q2.operations[1].kind == 'where'
    cond1, = q2.operations[0].args
    cond2, = q2.operations[1].args
    assert repr(cond1 & cond2) == repr(r.bar & (r.bar == 0))
    assert q2.operations[2:] == q1.operations[2:]
    

@suite.test
def test_access_points():
    metadata = Metadata()
    
    @metadata.register
    @access_point.Memory.declarative
    class foos:
        properties = {'hello': unicode, 'buzziness': int}
        ids = ['hello']

    foos.create(dict(hello='World', buzziness=0), save=True)
    foos.create(dict(hello='Lipsum', buzziness=4), save=True)

    q = Query.sort(r.buzziness).select(fu=r.hello + '!')
    assert list(foos.search(q)) == [{'fu': 'World!'}, {'fu': 'Lipsum!'}]

    q = Query.where(r.hello == 'Lipsum')
    assert foos.get(q.select(b=r.buzziness)) == {'b': 4}

    item = foos.get(q)
    # This is an actual Item, not a dict.
    assert item.identity.conditions == {'hello': 'Lipsum'}

