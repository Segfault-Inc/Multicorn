from multicorn import ForeignDataWrapper
import gc
import sys
import random

class MyClass(object):

    def __init__(self, num, rand):
        self.num = num
        self.rand = rand

class GCForeignDataWrapper(ForeignDataWrapper):

    def execute(self, quals, columns):
        gc.collect()
        result = []
        for obj in gc.get_objects():
            tobj = type(obj)
            if isinstance(obj, str):
                obj = obj.decode('utf8')
            elif isinstance(obj, unicode):
                pass
            else:
                try:
                    obj = str(obj).decode('utf8')
                except (UnicodeEncodeError, UnicodeDecodeError):
                    try:
                        obj = unicode(obj)
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        obj = u"<NA>"
            result.append({'object': obj,
                   'type': unicode(tobj),
                   'id': unicode(id(obj)),
                   'refcount': unicode(sys.getrefcount(obj))})
        return result

class MemStressFDW(ForeignDataWrapper):

    def __init__(self, options, columns):
        self.nb = int(options.get('nb', 100000))
        self.options = options
        self.columns = columns
        super(MemStressFDW, self).__init__(options, columns)

    def execute(self, quals, columns):
        for i in range(self.nb):
            num = i / 100.
            yield {'value': str(MyClass(i, num)),
                           'i': i,
                           'num': num}
