from multicorn import ForeignDataWrapper
from .utils import log_to_postgres
from itertools import cycle

class TestForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(TestForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        self.test_type = options.get('test_type', None)
        log_to_postgres(str(options))
        log_to_postgres(str(columns))

    def execute(self, quals, columns):
        log_to_postgres(str(quals))
        log_to_postgres(str(columns))
        random_thing = cycle([1, 2, 3])
        for index in range(20):
            line = {}
            for column_name in self.columns:
                if self.test_type == 'list':
                    line[column_name] = [column_name, next(random_thing), index]
                elif self.test_type == 'dict':
                    line[column_name] = {"column_name": column_name, "repeater": next(random_thing), "index": index}
                else:
                    line[column_name] = '%s %s %s' % (column_name, next(random_thing), index)
            yield line
