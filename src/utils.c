/*-------------------------------------------------------------------------
 *
 * The Multicorn Foreign Data Wrapper allows you to fetch foreign data in
 * Python in your PostgreSQL.
 *
 * This module contains helpers meant to be called from python code.
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


static PyObject *
log_to_postgres(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char	   *message;
	char	   *hintstr = NULL,
			   *detailstr = NULL;
	int			level = 1;
	int			severity;
	PyObject   *hint,
			   *detail;

	if (!PyArg_ParseTuple(args, "s|i", &message, &level))
	{
		Py_INCREF(Py_None);
		return Py_None;
	}
	switch (level)
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
	detail = PyDict_GetItemString(kwargs, "detail");
	if (errstart(severity, __FILE__, __LINE__, PG_FUNCNAME_MACRO, TEXTDOMAIN))
	{
		errmsg("%s", message);
		if (hint != NULL && hint != Py_None)
		{
			hintstr = PyString_AsString(hint);
			errhint("%s", hintstr);
			Py_DECREF(hint);
		}
		if (detail != NULL && detail != Py_None)
		{
			detailstr = PyString_AsString(detail);
			errdetail("%s", detailstr);
			Py_DECREF(detail);
		}
		errfinish(0);
	}
	Py_DECREF(args);
	Py_DECREF(kwargs);
	Py_INCREF(Py_None);
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
