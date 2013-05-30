import csv, collections, re

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

import syapse_client

############################################################################

def create_fdw(options,columns):
    """simple factory to make different FDW depending on source"""
    cl = ClassTable
    if options.get('source').startswith('fdw:'):
        cl = SavedQueryTable
    fdw = cl(options,columns)
    log_to_postgres(message = 'created '+str(type(fdw)))
    return fdw

class SyapseFDWFactory(object):
    """Attempting to create an instance of this class actually returns
    objects of another class that is chosen based on the provided
    arguments.  Unlike traditional factories, this factory creates new
    instances of other classes through its constructor rather than through
    a dedicated method.  This class exists because multicorn knows only
    how to call classes (rather than, say, factory functions), but it's
    convenient to be able to instantiate subclasses based on arguments."""
    def __new__(cls,options,columns):
        return create_fdw(options,columns)
    def __init__(self):
        pass

############################################################################

class SyapseFDW(ForeignDataWrapper):
    """A Table is an abstract object that represents a source of data from
    Syapse. It includes the properties-to-column mapping. Subclasses
    implement the essential `execute' method for FDWs that depend on
    source.  Subclasses also generate the DDL used to declare foreign
    tables in PostgreSQL."""
    def __init__(self, options, columns={}):
        super(SyapseFDW, self).__init__(options, columns)
        self.options = options
        self.columns = columns
        self.conn = connection_pool.get(self.options['syapse_hostname'],
                                        self.options['syapse_email'],
                                        self.options['syapse_password'])

    def execute(self, quals, columns):
        raise NotImplemented('execute method must be subclassed')


class ClassTable(SyapseFDW):
    def __init__(self,options,columns):
        super(ClassTable, self).__init__(options,columns)
        self.syapse_class = options['source']
        self._build_coldef_table()

    def execute(self, quals, columns):
        airs = self.conn.kb.listAppIndividualRecords(kb_class_id=self.syapse_class)
        for air in airs:
            ai = self.conn.kb.retrieveAppIndividual(air.app_ind_id)
            triples_items = ai.triplesItems(full=True)
            row = dict([ (self.coldefs[k].column, _transform_value(self.coldefs[k],_map_values(vals)))
                         for k,vals in triples_items ])
            yield row

    def _build_coldef_table(self):
        """returns a dict of syapse property name => ColDef record"""
        def __syapse_type(ps):
            return ps.prop.range if isinstance(ps.prop.range,unicode) else None
        def _make_coldef_record(ps):
            return ColDef(syapse_property = ps.prop.id, 
                          syapse_type = __syapse_type(ps),
                          syapse_cardinality = ps.cardinality,
                          column = camelcase_to_underscore(ps.prop.id),
                          type = 'TEXT',
                        )
        self.coldefs = dict(
            (ps.prop.id, _make_coldef_record(ps))
            for ps in self.conn.kb.getForm(self.syapse_class).props.values()
            )

    def table_ddl(self):
        cols = [ '{cd.column:30} {cd.type:10} -- {cd.syapse_cardinality:10}; {cd.syapse_type:10}; {cd.syapse_property}'.format(cd=cd)
                 for cd in self.coldefs.values() ]
        return """DROP FOREIGN TABLE {tablename};
CREATE FOREIGN TABLE {tablename} (
    {cols}
) server syapse options (
  syapse_hostname '{self.options[syapse_hostname]}',
  syapse_email    '{self.options[syapse_email]}',
  syapse_password '{self.options[syapse_password]}',
  syapse_class	  '{self.options[source]}'
);
""".format( tablename = camelcase_to_underscore(self.syapse_class),
            cols = "\n  , ".join(cols),
            self=self )


class SavedQueryTable(SyapseFDW):
    def table_ddl(self):
        raise NotImplemented()
    def execute(self, quals, columns):
        raise NotImplemented()

############################################################################

ColDef = collections.namedtuple('ColDef', [
        'syapse_property', 'syapse_type', 'syapse_cardinality', 'column', 'type'])

############################################################################
## CONNECTION POOL

class Singleton(type):
    def __init__(cls, name, bases, dict):
        super(Singleton, cls).__init__(name, bases, dict)
        cls.instance = None 

    def __call__(cls,*args,**kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance


class ConnectionPool(object):
    """Connection pool for Syapse connections"""
    __metaclass__ = Singleton

    def __init__(self):
        self.pool = dict()

    def get(self,hostname,email,password):
        hep = (hostname,email,password)
        if hep not in self.pool:
            self.pool[hep] = syapse_client.SyapseConnection(hostname,email,password)
            log_to_postgres(message = 'connected to Syapse ({hostname}/{email}:***)'.format(
                    hostname = hostname, email = email))
        return self.pool[hep]


############################################################################
## UTILITIES

def _map_values(vals):
    def _map_value(v):
        """transform values in triples items to unicode strings"""
        if isinstance(v,syapse_client.lobj.User):
            return v.email
        if isinstance(v,syapse_client.lobj.Project):
            return v.short_name
        return v
    return [ _map_value(v) for v in vals ]

def _transform_value(coldef,v):
    # if cardinality is 'Any', return set
    # else cadinality is 'ExactlyOne' or 'AtMostOne', return None or value
    if coldef.syapse_cardinality == 'Any':
        return v
    return v[0] if len(v)>0 else None

def camelcase_to_underscore(t):
    """e.g., camelCase -> camel_case, SyapseFDW -> syapse_fdw"""
    return re.sub(r'(\w)([A-Z]+)(?=[a-z]*)', r'\1_\2', t).lower()


############################################################################

connection_pool = ConnectionPool()

