from decimal import Decimal as D

from .queries import Query, execute
from .queries import aggregates as a
from .queries.expressions import r
from . import access_point, Metadata

def test_queries():
    data = [
        dict(toto='foo', tata=42, price=10, tax=D('1.196')),
        dict(toto='foo', tata=6, price=12, tax=1),
        dict(toto='bar', tata=42, price=5, tax=D('1.196')),
    ]
    def exec_(query):
        return list(execute(data, query))

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

    try:
        exec_(Query.select(price=r.price).where(r.toto == 'foo'))
    except KeyError:
        pass
    else:
        assert False, 'Expected KeyError'

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

def test():
    test_queries()
    test_access_points()
    print 'Tests ok'

