# -*- coding: utf-8 -*-
from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition
from multicorn.compat import unicode_
from .utils import log_to_postgres, WARNING, ERROR
from itertools import cycle
from datetime import datetime
from operator import itemgetter


class TestForeignDataWrapper(ForeignDataWrapper):

    _startup_cost = 10

    def __init__(self, options, columns):
        super(TestForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        self.test_type = options.get('test_type', None)
        self.tx_hook = options.get('tx_hook', False)
        self._row_id_column = options.get('row_id_column',
                                          list(self.columns.keys())[0])
        log_to_postgres(str(sorted(options.items())))
        log_to_postgres(str(sorted([(key, column.type_name) for key, column in
                                    columns.items()])))
        for column in columns.values():
            if column.options:
                log_to_postgres('Column %s options: %s' %
                                (column.column_name, column.options))
        if self.test_type == 'logger':
            log_to_postgres("An error is about to occur", WARNING)
            log_to_postgres("An error occured", ERROR)

    def _as_generator(self, quals, columns):
        random_thing = cycle([1, 2, 3])
        for index in range(20):
            if self.test_type == 'sequence':
                line = []
                for column_name in self.columns:
                    line.append('%s %s %s' % (column_name,
                                              next(random_thing), index))
            else:
                line = {}
                for column_name, column in self.columns.items():
                    if self.test_type == 'list':
                        line[column_name] = [
                            column_name, next(random_thing),
                            index, '%s,"%s"' % (column_name, index),
                            '{some value, \\" \' 2}']
                    elif self.test_type == 'dict':
                        line[column_name] = {
                            "column_name": column_name,
                            "repeater": next(random_thing),
                            "index": index,
                            "maybe_hstore": "a => b"}
                    elif self.test_type == 'date':
                        line[column_name] = datetime(2011, (index % 12) + 1,
                                                     next(random_thing), 14,
                                                     30, 25)
                    elif self.test_type == 'int':
                        line[column_name] = index
                    elif self.test_type == 'encoding':
                        line[column_name] = (b'\xc3\xa9\xc3\xa0\xc2\xa4'
                                             .decode('utf-8'))
                    elif self.test_type == 'nested_list':
                        line[column_name] = [
                            [column_name, column_name],
                            [next(random_thing), '{some value, \\" 2}'],
                            [index, '%s,"%s"' % (column_name, index)]]
                    else:
                        line[column_name] = '%s %s %s' % (column_name,
                                                          next(random_thing),
                                                          index)
            yield line

    def execute(self, quals, columns, pathkeys=[]):
        log_to_postgres(str(sorted(quals)))
        log_to_postgres(str(sorted(columns)))
        if (len(pathkeys)) > 0:
            log_to_postgres("requested sort(s): ")
            for k in pathkeys:
                log_to_postgres(k)
        if self.test_type == 'None':
            return None
        elif self.test_type == 'iter_none':
            return [None, None]
        else:
            if (len(pathkeys) > 0):
                # testfdw don't have tables with more than 2 fields, without
                # duplicates, so we only need to worry about sorting on 1st
                # asked column
                k = pathkeys[0];
                res = self._as_generator(quals, columns)
                if (type(res) is dict):
                    return sorted(res, key=itemgetter(k.attname),
                                  reverse=k.is_reversed)
                else:
                    return sorted(res, key=itemgetter(k.attnum - 1),
                                  reverse=k.is_reversed)
            return self._as_generator(quals, columns)

    def get_rel_size(self, quals, columns):
        if self.test_type == 'planner':
            return (10000000, len(columns) * 10)
        return (20, len(columns) * 10)

    def get_path_keys(self):
        if self.test_type == 'planner':
            return [(('test1',), 1)]
        return []

    def can_sort(self, pathkeys):
        # assume sort pushdown ok for all cols, in any order, any collation
        return pathkeys

    def update(self, rowid, newvalues):
        if self.test_type == 'nowrite':
            super(TestForeignDataWrapper, self).update(rowid, newvalues)
        log_to_postgres("UPDATING: %s with %s" % (
            rowid, sorted(newvalues.items())))
        if self.test_type == 'returning':
            for key in newvalues:
                newvalues[key] = "UPDATED: %s" % newvalues[key]
            return newvalues

    def delete(self, rowid):
        if self.test_type == 'nowrite':
            super(TestForeignDataWrapper, self).delete(rowid)
        log_to_postgres("DELETING: %s" % rowid)

    def insert(self, values):
        if self.test_type == 'nowrite':
            super(TestForeignDataWrapper, self).insert(values)
        log_to_postgres("INSERTING: %s" % sorted(values.items()))
        if self.test_type == 'returning':
            for key in self.columns:
                values[key] = "INSERTED: %s" % values.get(key, None)
            return values

    @property
    def rowid_column(self):
        return self._row_id_column

    def begin(self, serializable):
        if self.tx_hook:
            log_to_postgres('BEGIN')

    def sub_begin(self, level):
        if self.tx_hook:
            log_to_postgres('SUBBEGIN')

    def sub_rollback(self, level):
        if self.tx_hook:
            log_to_postgres('SUBROLLBACK')

    def sub_commit(self, level):
        if self.tx_hook:
            log_to_postgres('SUBCOMMIT')

    def commit(self):
        if self.tx_hook:
            log_to_postgres('COMMIT')

    def pre_commit(self):
        if self.tx_hook:
            log_to_postgres('PRECOMMIT')

    def rollback(self):
        if self.tx_hook:
            log_to_postgres('ROLLBACK')

    @classmethod
    def import_schema(self, schema, srv_options, options, restriction_type,
                      restricts):
        log_to_postgres("IMPORT %s FROM srv %s OPTIONS %s RESTRICTION: %s %s" %
                        (schema, srv_options, options, restriction_type,
                         restricts))
        tables = set([unicode_("imported_table_1"),
                      unicode_("imported_table_2"),
                      unicode_("imported_table_3")])
        if restriction_type == 'limit':
            tables = tables.intersection(set(restricts))
        elif restriction_type == 'except':
            tables = tables - set(restricts)
        rv = []
        for tname in sorted(list(tables)):
            table = TableDefinition(tname)
            nb_col = options.get('nb_col', 3)
            for col in range(nb_col):
                table.columns.append(
                    ColumnDefinition("col%s" % col,
                                     type_name="text",
                                     options={"option1": "value1"}))
            rv.append(table)
        return rv
