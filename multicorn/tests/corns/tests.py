def corntest(fun):
    fun._is_corn_test = True
    return fun

@corntest
def test_all(Corn):
    items = Corn.all.execute()
    assert hasattr(items, '__iter__')

