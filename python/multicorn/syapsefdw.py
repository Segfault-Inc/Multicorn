import collections, re

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

import syapse_client

############################################################################

# TODO: column types (rule-based?); date_, has_->aii (array), 
# TODO: column name mapping
# TODO: rule-based column name mapping (has_, cardinality -> +s, etc)
# TODO: explicit prop -> col mapping (instead of relying on zip order)
# TODO: has_ -> list, always

# TODO: reconnect syapse on failure?
# TODO: config-based setup

############################################################################

class SyapseFDWFactory(object):
    """This class creates new subclasses of SyapseFDW based on the
    provided arguments to the constructor.

    Unlike traditional factories, this factory creates new instances of
    other classes through its constructor rather than through a dedicated
    method.  You will never get an instance of SyapseFDWFactory itself.

    Why?  Multicorn setup requires calling a class constructor for the
    FDW.  When multiple subclasses are used to handle FDWs for a single
    server, it's convenient to be able to instantiate subclasses based on
    arguments."""
    def __new__(cls,options,columns):
        return create_fdw(options,columns)
    def __init__(self):
        pass

def create_fdw(options,columns):
    """simple factory to make different FDW depending on source"""
    cl = ClassTable
    if options.get('source').startswith('fdw:'):
        cl = SavedQueryTable
    fdw = cl(options,columns)
    log_to_postgres(message = 'created '+str(type(fdw))+' for '+options.get('source'))
    return fdw


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
        self.project = self.conn.retrieveProject("s:project/2")
        self.conn.current_project = self.project

    def execute(self, quals, columns):
        raise NotImplemented('execute method must be subclassed')

    def table_ddl(self):
        cols = [ '{cd.column:30} {cd.type:10} -- {cd.syapse_cardinality:10}; {cd.syapse_type:10}; {cd.syapse_property}'.format(cd=cd)
                 for cd in self.coldefs ]
        return """DROP FOREIGN TABLE {tablename};
CREATE FOREIGN TABLE {tablename} (
    {cols}
) server syapse options (
  syapse_hostname '{self.options[syapse_hostname]}',
  syapse_email    '{self.options[syapse_email]}',
  syapse_password '{self.options[syapse_password]}',
  source      	  '{self.options[source]}'
);
""".format( tablename = self.tablename,
            cols = "\n  , ".join(cols),
            self=self )

class ClassTable(SyapseFDW):
    """Represents a Syapse class-based data source.  The current Syapse
    API has no filtering or bulk fetch mechanism.  Therefore, records are
    returned by fetching them one-by-one.  This interface is extremely
    slow -- consider making a saved query instead."""

    def __init__(self,options,columns):
        super(ClassTable, self).__init__(options,columns)
        self.syapse_class = options['source']
        self.tablename = camelcase_to_underscore(self.syapse_class)
        self._build_coldefs()

    def execute(self, quals, columns):
        airs = self.conn.kb.listAppIndividualRecords(kb_class_id=self.syapse_class)
        for air in airs:
            ai = self.conn.kb.retrieveAppIndividual(air.app_ind_id)
            triples_items = ai.triplesItems(full=True)
            # BROKEN HERE: adapt to coldefs list rather than coldefs dict
            row = dict([ (self.coldefs[k].column, _transform_value(self.coldefs[k],_map_values(vals)))
                         for k,vals in triples_items ])
            yield row

    def _build_coldefs(self):
        """builds a dict of syapse property name => ColDef record"""
        def _syapse_type(ps):
            return ps.prop.range if isinstance(ps.prop.range,unicode) else None
        def _make_coldef(ps):
            return ColDef(syapse_property = ps.prop.id, 
                          syapse_type = _syapse_type(ps),
                          syapse_cardinality = ps.cardinality,
                          column = camelcase_to_underscore(ps.prop.id),
                          type = 'TEXT',
                        )
        self.coldefs = [ _make_coldef(ps)
                         for ps in self.conn.kb.getForm(self.syapse_class).props.values() ]


class SavedQueryTable(SyapseFDW):
    def __init__(self,options,columns):
        super(SavedQueryTable, self).__init__(options,columns)
        self.conn._sqs = dict( self.conn.getAllSavedQueries() ) # add to conn object, shared across conns
        self.syapse_saved_query = options['source']
        self.syapse_saved_query_id = self.conn._sqs[ self.syapse_saved_query ]
        self.tablename = savedquery_to_tablename(self.syapse_saved_query)
        if 'conf' in options:
            self.conf = SafeConfigParser()
            self.conf.read( open(options['conf'],'r') )
        self._build_coldefs()

    def execute(self, quals, columns):
        def _process_ds_row(coldefs,ds_row):
            # horrors. The AII is buried in "AnnotatedValue" records,
            # which are returned for system properties when the saved
            # query is executed with annotate_meta=True.  These AV
            # instances happen to contain the AII for the current
            # record. We fish the AII out of any of these AVs.
            avs = [ v for v in ds_row 
                    if isinstance(v,syapse_client.sem.advq.AnnotatedValue) ]
            aii = avs[0].app_ind_id if len(avs) > 0 else None
            vals = [ v.value if isinstance(v,syapse_client.sem.advq.AnnotatedValue) else v
                     for v in [aii] + ds_row ]
            row = dict( [ (cd.column, _transform_value(val,cd.syapse_cardinality))
                           for cd,val in zip(coldefs,vals) ])
            #if row['workflow_aii'] != u'ivld:LocusWorkflow_19588':
            #    return None
            #import IPython; IPython.embed()
            return row

        ds = self.conn.kb.executeSavedQuery( self.syapse_saved_query_id,
                                             annotate_meta = True)
        for ds_row in ds.rows:
            row = _process_ds_row(self.coldefs,ds_row)
            yield row

    def _build_coldefs(self):
        """builds a list ColDef records"""
        def _make_coldef(h):
            col = camelcase_to_underscore(h.split(':')[-1])
            type = 'TEXT'
            if col.startswith('date_'):
                type = 'TIMESTAMP'
            card = 'ExactlyOne'
            if col.startswith('has_'):
                card = 'Any'
                type += '[]'
            return ColDef(syapse_property = h,
                                     syapse_type = None,
                                     syapse_cardinality = card,
                                     column = col,
                                     type = type,
                                     )
        ds = self.conn.kb.executeSavedQuery( self.syapse_saved_query_id )
        self.coldefs = [ _make_coldef(h)
                         for h in [self.tablename+'_aii'] + ds.headers ]
        
                    
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

connection_pool = ConnectionPool()

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

def _transform_value(v,cardinality):
    # v may be a list or scalar, and cardinality may be any
    # if cardinality is 'Any', return set
    if cardinality == 'Any':
        return v if isinstance(v,list) else [v]
    # else cardinality is 'ExactlyOne' or 'AtMostOne', return None or value
    if not isinstance(v,list):
        return v
    return v[0] if len(v)>0 else None

def camelcase_to_underscore(t):
    """e.g., camelCase -> camel_case, SyapseFDW -> syapse_fdw"""
    return re.sub(r'([a-z])([A-Z]+)(?=[a-z]*)', r'\1_\2', t).lower()

def savedquery_to_tablename(t):
    """fdw:Blood -> fdw_blood"""
    if t.startswith('fdw:'):
        t = t[4:]
    return camelcase_to_underscore(t)

############################################################################

