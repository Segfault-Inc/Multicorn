"""
Purpose
-------

This foreign data wrapper can be to list processes according to the
psutil module.

The column names are mapped to the :py:class:`psutil.Process` attributes.

.. api_compat: :read:

Usage Example
-------------

.. code-block:: sql

    create foreign table processes (
        pid int,
        ppid int,
        name text,
        exe text,
        cmdline text,
        create_time timestamptz,
        status text,
        cwd text,
        uids int[],
        gids int[],
        terminal text,
        nice int,
        ionice float[],
        rlimit int[],
        num_ctx_switches bigint[],
        num_fds int,
        num_threads int,
        cpu_times interval[],
        cpu_percent float,
        cpu_affinity bigint[],
        memory_info bigint[],
        memory_percent float,
        open_files text[],
        connections text[],
        is_running bool
    ) server process_server


.. code-block:: bash

    ro= select name, cmdline, cpu_percent  from processes where name = 'postgres';
        name   |                   cmdline                    | cpu_percent
     ----------+----------------------------------------------+-------------
      postgres | ['/home/ro/pgdev/bin/postgres']              |           0
      postgres | ['postgres: checkpointer process   ']        |           0
      postgres | ['postgres: writer process   ']              |           0
      postgres | ['postgres: wal writer process   ']          |           0
      postgres | ['postgres: autovacuum launcher process   '] |           0
      postgres | ['postgres: stats collector process   ']     |           0
      postgres | ['postgres: ro ro [local] SELECT']           |           9
    (7 rows)



Options
-------

No options.

"""
from . import ForeignDataWrapper
from datetime import datetime
import psutil


DATE_COLUMNS = ['create_time']


class ProcessFdw(ForeignDataWrapper):
    """A foreign datawrapper for querying system stats.

    It accepts no options.
    You can define any column named after a statgrab column.
    See the statgrab documentation.

    """

    def _convert(self, key, value):
        if key in DATE_COLUMNS:
            if isinstance(value, (list, tuple)):
                return [datetime.fromtimestamp(v) for v in value]
            else:
                return datetime.fromtimestamp(value)
        return value

    def execute(self, quals, columns):
        for process in psutil.process_iter():
            yield dict([(key, self._convert(key, value))
                   for key, value in process.as_dict(columns).items()])
