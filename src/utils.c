/*-------------------------------------------------------------------------
 *
 * The Multicorn Foreign Data Wrapper allows you to fetch foreign data in
 * Python in your PostgreSQL
 *
 * This software is released under the postgresql licence
 *
 * author: Kozea
 *
 *
 *-------------------------------------------------------------------------
 */
#include <Python.h>
#include "postgres.h"

static PyObject* log_to_postgres(PyObject* self, PyObject *args, PyObject* kwargs)
{
    const char* message;
    const int level = 1;
    int severity;
    PyObject* hint;
    if(!PyArg_ParseTuple(args, "s|i", &message, &level)){
        return ;
    }
    switch(level)
    {
        case 0:
            severity = DEBUG1;
            break;
        case 1:
            severity = NOTICE;
            break;
        case 2:
            severity = WARNING;
            break;  
        case 3:
            severity = ERROR;
            break;
        case 4:
            severity = FATAL;
            break;
        default:
            severity = INFO;
            break;
    }
    hint = PyDict_GetItemString(kwargs, "hint");
    if(hint != NULL && hint != Py_None){
        ereport(severity, (errmsg(message), errhint(PyString_AsString(hint))));
    } else {
        ereport(severity, (errmsg(message)));
    }
    return Py_None;
}

static PyMethodDef UtilsMethods[] = {
    {"_log_to_postgres", log_to_postgres, METH_VARARGS | METH_KEYWORDS, "Log to postresql client"},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC
init_utils(void)
{
    (void) Py_InitModule("multicorn._utils", UtilsMethods);
}
