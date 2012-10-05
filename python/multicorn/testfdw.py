from multicorn import ForeignDataWrapper
from .utils import log_to_postgres
from itertools import cycle
from datetime import datetime


class TestForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(TestForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        self.test_type = options.get('test_type', None)
        log_to_postgres(str(options))
        log_to_postgres(str(dict([(key, column.type_name) for key, column in
                                  columns.items()])))

    def execute(self, quals, columns):
        log_to_postgres(str(quals))
        log_to_postgres(str(columns))
        random_thing = cycle([1, 2, 3])
        for index in range(20):
            if self.test_type == 'sequence':
                line = []
                for column_name in self.columns:
                    line.append('%s %s %s' % (column_name,
                                              next(random_thing), index))
            else:
                line = {}
                for column_name in self.columns:
                    if self.test_type == 'list':
                        line[column_name] = [column_name, next(random_thing),
                                             index]
                    elif self.test_type == 'dict':
                        line[column_name] = {"column_name": column_name,
                                             "repeater": next(random_thing),
                                             "index": index}
                    elif self.test_type == 'date':
                        line[column_name] = datetime(2011, (index % 12) + 1,
                                                     next(random_thing), 14,
                                                     30, 25)

                    else:
                        line[column_name] = '%s %s %s' % (column_name,
                                                          next(random_thing),
                                                          index)
            yield line

    def get_rel_size(self, quals, columns):
        if self.test_type == 'planner':
            return (10000000, len(columns) * 10)
        return (20, len(columns) * 10)

    def get_path_keys(self):
        if self.test_type == 'planner':
            return [(('test1',), 1)]
        return []
