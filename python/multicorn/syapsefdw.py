from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

import syapse_client

import collections, re

# TODO: create a singleton connection
# TODO: type maps by type, prop (date)
# TODO: flesh out flow
# TODO: add support for saved queries


ColDef = collections.namedtuple('ColDef', ['syapse_property', 'syapse_type', 'syapse_cardinality', 
                                           'column', 'type'])

class SyapseFDW(ForeignDataWrapper):
    def __init__(self, options, columns={}):
        assert options['syapse_hostname'] is not None
        assert options['syapse_email'] is not None
        assert options['syapse_password'] is not None
        assert options['syapse_class'] is not None

        super(SyapseFDW, self).__init__(options, columns)
        self.options = options
        self.columns = columns
        self.syapse_class = options.get('syapse_class')
        self.conn = syapse_client.SyapseConnection(options['syapse_hostname'],
                                                   options['syapse_email'],
                                                   options['syapse_password'])
        self.coldefs = self._build_coldef_table()
        log_to_postgres(message = 'connected to Syapse for class '+self.syapse_class)

    def execute(self, quals, columns):
        airs = self.conn.kb.listAppIndividualRecords(kb_class_id=self.syapse_class)
        #import IPython; IPython.embed()
        for air in airs:
            ai = self.conn.kb.retrieveAppIndividual(air.app_ind_id)
            triples_items = ai.triplesItems(full=True)
            row = dict([ (self.coldefs[k].column, _transform_value(self.coldefs[k],_map_values(vals)))
                         for k,vals in triples_items ])
            yield row

    def create_table_sql(self):
        cols = [ '{cd.column:30} {cd.type:10} -- {cd.syapse_cardinality:10}; {cd.syapse_type:10}; {cd.syapse_property}'.format(cd=cd)
                 for cd in self.coldefs.values() ]
        return """DROP FOREIGN TABLE {tablename};
CREATE FOREIGN TABLE {tablename} (
    {cols}
) server syapse options (
  syapse_hostname '{self.options[syapse_hostname]}',
  syapse_email    '{self.options[syapse_email]}',
  syapse_password '{self.options[syapse_password]}',
  syapse_class	  '{self.options[syapse_class]}'
);
""".format( tablename = _camelcase_to_underscore(self.syapse_class),
            cols = "\n  , ".join(cols),
            self=self )

    ############################################################

    def _build_coldef_table(self):
        """returns a dict of syapse property name => ColDef record"""
        def __syapse_type(ps):
            return ps.prop.range if isinstance(ps.prop.range,unicode) else None

        def _make_coldef_record(ps):
            return ColDef(syapse_property = ps.prop.id, 
                          syapse_type = __syapse_type(ps),
                          syapse_cardinality = ps.cardinality,
                          column = _camelcase_to_underscore(ps.prop.id),
                          type = 'TEXT',
                        )
        return dict(
            (ps.prop.id, _make_coldef_record(ps))
            for ps in self.conn.kb.getForm(self.syapse_class).props.values()
            )



############################################################################
## UTILITIES

def _camelcase_to_underscore(t):
    """e.g., camelCase -> camel_case, SyapseFDW -> syapse_fdw"""
    return re.sub(r'(\w)([A-Z]+)(?=[a-z]*)', r'\1_\2', t).lower()

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


