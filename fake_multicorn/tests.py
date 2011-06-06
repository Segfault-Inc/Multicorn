from attest import Tests, assert_hook
import attest

from .queries import Query
from .queries.expressions import r
from . import access_point, Metadata


suite = Tests()


@suite.test
def test_access_points():
    metadata = Metadata()
    
    @metadata.register
    @access_point.Memory.declarative
    class foos:
        properties = {'hello': unicode, 'foo': int, 'buzziness': int}
        ids = ['hello', 'foo']

    foos.create(dict(hello='World', foo=1, buzziness=0), save=True)
    foos.create(dict(hello='Lipsum', foo=1, buzziness=4), save=True)

    q = Query.sort(r.buzziness).select(fu=r.hello + '!')
    assert list(foos.search(q)) == [{'fu': 'World!'}, {'fu': 'Lipsum!'}]

    q = Query.where(hello='Lipsum')
    assert foos.get(q.select(b=r.buzziness)) == {'b': 4}

    item = foos.get(q)
    # This is an actual Item, not a dict.
    assert item.identity.conditions == {'hello': 'Lipsum', 'foo': 1}
    
    
    # Make sure we're not doing a slow query
    class SlowQuery(Exception):
        pass
    def _all():
        # Fast queries should not go through here.
        raise SlowQuery()
    foos._all = _all
    
    with attest.raises(SlowQuery):
        # buzziness is not an identity property, this is a slow query
        foos.get(Query.where(buzziness=4))

    with attest.raises(SlowQuery):
        # foo is only part of the identity, this is a slow query
        foos.get(Query.where(foo=1))

    # Whole identity, this is a fast query
    assert foos.get(Query.where(hello='Lipsum', foo=1).select(b=r.buzziness)) \
        == {'b': 4}

