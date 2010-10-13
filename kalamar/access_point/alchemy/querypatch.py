from kalamar import query
import kalamar.access_point.alchemy
from kalamar.request import And, Or, Condition, Not
from sqlalchemy import sql

def query_chain_to_alchemy(self, alchemy_query, access_point, properties):
    """Monkey-patched method on QueryChain to convert to alchemy

    """
    for sub_query in self.queries:
        alchemy_query = sub_query.to_alchemy(alchemy_query, access_point, properties)
        properties = sub_query.validate(access_point.site, properties)
    return alchemy_query

def query_chain_validator(self, access_point, properties):
    cants = []
    cans = []
    for sub_query in self.queries:
        managed,  not_managed = sub_query.alchemy_validate(access_point, properties)
        if not_managed is not None:
            cants.append(not_managed)         
        if managed is not None:
            cans.append(managed)
        properties = sub_query.validate(access_point.site, properties)
    query_can = query.QueryChain(cans) if cans and not cants else None
    return query_can, query.QueryChain(cants)


def standard_validator(self, access_point, properties):
    return (self,None)

def query_filter_to_alchemy(self, alchemy_query, access_point, properties):
    """Monkey-patched method on QueryFilter to convert to alchemy

    """

    def to_alchemy_condition(condition):
        """Converts a kalamar condition to an sqlalchemy condition.

        """
        if isinstance(condition, And):
            return apply(sql.and_, [to_alchemy_condition(cond)
                for cond in condition.sub_requests])
        elif isinstance(condition, Or):
            return apply(sql.or_, [to_alchemy_condition(cond)
                for cond in condition.sub_requests])
        elif isinstance(condition, Not):
            return apply(sql.not_, [to_alchemy_condition(cond)
                for cond in condition.sub_requests])
        else:
            col = properties[condition.property.name].column
            if condition.operator == '=':
                return col == condition.value
            else:
                return col.op(condition.operator)(condition.value)
    cond_tree = self.condition.properties_tree
    def build_join(tree, properties, alchemy_query):
        for name, values in tree.items():
            property = properties[name]
            if property.remote_ap:
                remote_ap = access_points[property.remote_ap]
                join_col1 = alchemy_query.corresponding_column(property.column)
                if property.relation == "many-to-one":
                    join_col2 = alchemy_query.corresponding_column(
                    remote_ap.properties[remote_ap.identity_properties[0]])
                    alchemy_query = alchemy_query.join(remote_ap._table,
                            onclause = join_col1 == join_col2)
                    alchemy_query = build_join(values, remote_ap.properties, alchemy_query)
        return alchemy_query
    alchemy_query = build_join(cond_tree, properties, alchemy_query)
    alchemy_query = alchemy_query.where(to_alchemy_condition(self.condition))
    return alchemy_query

def query_filter_validator(self, access_point, properties):
    cond_tree = self.condition.properties_tree
    access_points = access_point.site.access_points
    def inner_manage(name, values, properties):
        if name not in properties:
            return False
        elif properties[name].remote_ap:
            remote_ap = access_points[properties[name].remote_ap]
            return isinstance(remote_ap, kalamar.access_point.alchemy.Alchemy) and \
                all([inner_manage(name, values, remote_ap.properties) 
                    for name, values in cond_tree.items()])
        else: 
            return True
    return (self, None) if all([inner_manage(name, values, properties) for name, values in
        cond_tree.items()]) else (None, self)


    

def query_select_to_alchemy(self, alchemy_query, access_point, properties,
        join=None, is_root=True):
    """Monkey-patched method on QuerySelect to convert to alchemy

    """
    access_points = access_point.site.access_points
    def build_join(select, properties, join):
        for name, sub_select in select.sub_selects.items():
            remote_ap = access_points[properties[name].remote_ap]
            remote_property = remote_ap.properties[properties[name].remote_property]
            col1 = properties[name].column
            col2 = remote_property.column
            join = join.outerjoin(remote_ap._table, 
                onclause = col1 == col2)
            join = build_join(sub_select, remote_ap.properties, join)
        return join
    def build_select(select, properties, alchemy_query):
        for name, value in select.mapping.items():
            alchemy_query.append_column(properties[value.name].column.label(name))
        for name, sub_select in select.sub_selects.items():
            remote_ap = access_points[properties[name].remote_ap]
            alchemy_query = build_select(sub_select, remote_ap.properties,
                    alchemy_query)
        return alchemy_query
    join = build_join(self, properties, access_point._table)
    alchemy_query = alchemy_query.select_from(join)
    build_select(self, properties, alchemy_query)
    return alchemy_query


def query_select_validator(self, access_point, properties):
    access_points = access_point.site.access_points
    def isvalid(select, properties):
        for name, sub_select in select.sub_selects.items():
            remote_ap = access_points[properties[name].remote_ap]
            if not isinstance(remote_ap, kalamar.access_point.alchemy.Alchemy):
                return False
            if not isvalid(sub_select, remote_ap.properties):
                return False
        return True
    if isvalid(self, properties):
        return self,None
    else:
        return None, self


def query_distinct_to_alchemy(self, alchemy_query, access_point, properties):
    return alchemy_query.distinct()
    
def query_range_to_alchemy(self, alchemy_query, access_point, properties):
    if self.range.start:
        alchemy_query = alchemy_query.offset(self.range.start)
    if self.range.stop:
        alchemy_query = alchemy_query.limit(self.range.stop - (self.range.start
            or 0))
    return alchemy_query


def query_order_to_alchemy(self, alchemy_query, access_point, properties):
    for key, order in self.orderbys:
        alchemy_query = alchemy_query.order_by(expression.asc(key) if order
                else expression.desc(key))
    return alchmey_query


for q in [query.QueryDistinct, query.QueryRange, query.QueryOrder]:
    q.alchemy_validate = standard_validator

query.QueryFilter.alchemy_validate = query_filter_validator
query.QueryChain.alchemy_validate = query_chain_validator
query.QuerySelect.alchemy_validate = query_select_validator
    
query.QueryChain.to_alchemy = query_chain_to_alchemy
query.QueryFilter.to_alchemy = query_filter_to_alchemy
query.QueryDistinct.to_alchemy = query_distinct_to_alchemy
query.QueryRange.to_alchemy = query_range_to_alchemy
query.QueryOrder.to_alchemy = query_order_to_alchemy
query.QuerySelect.to_alchemy = query_select_to_alchemy


