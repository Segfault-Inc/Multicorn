from expressions import r
from queries import Query, execute
import aggregates as a

def test():
    from decimal import Decimal as D
    from pprint import pprint
    data = [
        dict(toto='foo', tata=42, price=10, tax=D('1.196')),
        dict(toto='foo', tata=6, price=12, tax=1),
        dict(toto='bar', tata=42, price=5, tax=D('1.196')),
    ]
    def exec_(query):
        return list(execute(query, data))

    res = exec_(Query.select(toto=r.toto, price=r.price * r.tax))
    assert res == [
        dict(toto='foo', price=D('11.96')),
        dict(toto='foo', price=12),
        dict(toto='bar', price=D('5.98')),
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

if __name__ == '__main__':
    test()
    print 'Tests ok'
