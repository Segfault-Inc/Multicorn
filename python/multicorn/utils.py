from logging import ERROR, INFO, DEBUG, WARNING, CRITICAL
try:
    from ._utils import _log_to_postgres
    from ._utils import check_interrupts
    from ._utils import _plpy_trampoline
    from ._utils import _getInstanceByOid
except ImportError as e:
    from warnings import warn
    warn("Not executed in a postgresql server,"
         " disabling log_to_postgres", ImportWarning)

    def _log_to_postgres(message, level=0, hint=None, detail=None):
        pass

    def _plpy_trampoline(oid):
        raise Exception("utils.so not loaded")

    def _getInstanceByOid(oid):
        raise Exception("utils.so not loaded")


REPORT_CODES = {
    DEBUG: 0,
    INFO: 1,
    WARNING: 2,
    ERROR: 3,
    CRITICAL: 4
}


def log_to_postgres(message, level=INFO, hint=None, detail=None):
    code = REPORT_CODES.get(level, None)
    if code is None:
        raise KeyError("Not a valid log level")
    _log_to_postgres(message, code, hint=hint, detail=detail)


def trampoline(table_oid):
    _plpy_trampoline(table_oid)

def getInstanceByOid(table_oid):
    instance = _getInstanceByOid(table_oid)
    if instance is None:
        raise KeyError(table_oid)
    return instance
