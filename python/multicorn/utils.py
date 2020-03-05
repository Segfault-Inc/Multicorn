
from logging import ERROR, INFO, DEBUG, WARNING, CRITICAL
try:
    from ._utils import _log_to_postgres
    from ._utils import check_interrupts
    from ._utils import _execute
    from ._utils import _prepare
    from ._utils import _execute_stmt
    from ._utils import _fetch
except ImportError as e:
    from warnings import warn
    warn("Not executed in a postgresql server,"
         " disabling log_to_postgres", ImportWarning)

    def _log_to_postgres(message, level=0, hint=None, detail=None):
        pass

    def _execute(sql, readonly, count):
        pass

    def _prepare(sql, *argv):
        """
        _prepare takes a sql statement and a
        list of converter objects.  The 
        converter objects define getOID
        to get the postgres oid for 
        that argument and a getdataum method.
        The getdatum method returns the
        takes two a single object as
        an argument (as well as self) and
        returns the value of that argument
        as something the sql converter
        can understand.  Currently this is limited
        to a string, long, int, or float.  So
        for example dates and time stamps would
        have to be passed in as strings and
        the sql would have to convert them.
        """
        pass

    def _execute_stmt(stmt, args, converters):
        pass

    def _fetch():
        pass


REPORT_CODES = {
    DEBUG: 0,
    INFO: 1,
    WARNING: 2,
    ERROR: 3,
    CRITICAL: 4
}

class StatementException(Exception):
    pass

def _booleanXForm(bl):
    if isinstance(bl, basestring):
        if bl.lower() in ('0', 'false', 'f', ''):
            return 0
    if bl:
        return 1

    return 0

class Statement(object):
    type_map = None

    simple_converters = {
        'int8': { 'func': lambda x: long(x) },
        'int4': { 'func': lambda x: int(x) },
        'float8':{ 'func': lambda x: float(x) },
        'text': { 'lob': True },
        'bool': { 'func': _booleanXForm },
    }
    
    class Converter(object):
        converter_map = {}
        
        def __init__(self, pgtype, **kwargs):
            self.pgtype = pgtype
            self.lob = kwargs.get('lob', False)
            

        def isLob(self):
            return self.lob
        
        def getdatum(self, obj):
            return obj

        def getOID(self):
            oid = Statement.getOID(self.pgtype);
            return oid

    @staticmethod
    def converter(*args):
        def decorator(cls):
            for name in args:
                Converter.converter_map[name]=cls
            return cls
        return decorator

    for c,p in simple_converters.items():
        name="SimpleConverter%s" %c
        
        d = {
            '__init__': lambda s, n=c, p=p: Statement.Converter.__init__(s, n, **p)
        }

        if 'func' in p:
            d['getdatum'] = lambda s, o, f=p['func']: f(o)

        Converter.converter_map[c] = type(name, (Converter,), d)
            

    def __init__(self, sql, pytypes):
        self.converters = []
        self.results = None
        for t in pytypes:
            if t not in Statement.Converter.converter_map:
                raise StatementException("Unhandled Data Type %s" %t)
            self.converters.append(Statement.Converter.converter_map[t]())
        self.stmt = prepare(sql, self.converters)

    def execute(self, data):
        if not isinstance(data, tuple):
            data = tuple(data)
            
        ret = execute_stmt(self.stmt, data, tuple(self.converters))
        self.results = fetch()
        return ret

    def getResults(self):
        return self.results
            

    @staticmethod
    def getOID(typname):
        # not thread safe.
        # XXXXX FIXME, need to consider namespaces.
        if Statement.type_map is None:
            sql = "select typname, oid::int4 from pg_type;"
            res = execute(sql)
            types = fetch()
            Statement.type_map = {}
            for t in types:
                Statement.type_map[t['typname']] = t['oid']

        return Statement.type_map.get(typname, None)

            


def log_to_postgres(message, level=INFO, hint=None, detail=None):
    code = REPORT_CODES.get(level, None)
    if code is None:
        raise KeyError("Not a valid log level")
    _log_to_postgres(message, code, hint=hint, detail=detail)


def execute(sql, read_only=False, count=0):
    return _execute(sql, None, count)

def prepare(sql, converters):
    return _prepare(sql, *converters)

def execute_stmt(plan, args, converters):
    if not isinstance(args, tuple):
        args = tuple( a for a in args )

    if not isinstance(converters, tuple):
        converters = tuple (c for c in converters );
    import logging
    logging.debug("execute_stmt: about to call _execute_stmt")
    return _execute_stmt(plan, args, converters)

def fetch():
    return _fetch()
            

