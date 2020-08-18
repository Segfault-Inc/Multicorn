/*-------------------------------------------------------------------------
 *
 * The Multicorn Foreign Data Wrapper allows you to fetch foreign data in
 * Python in your PostgreSQL server.
 *
 * This module contains error handling functions.
 *
 * This software is released under the postgresql licence
 *
 * author: Kozea
 *
 *
 *-------------------------------------------------------------------------
 */
#include "multicorn.h"
#include "bytesobject.h"
#include "access/xact.h"

void reportException(PyObject *pErrType,
				PyObject *pErrValue,
				PyObject *pErrTraceback);


void
errorCheck()
{
	PyObject   *pErrType,
			   *pErrValue,
			   *pErrTraceback;

	PyErr_Fetch(&pErrType, &pErrValue, &pErrTraceback);
	if (pErrType)
	{
		reportException(pErrType, pErrValue, pErrTraceback);
	}
}

void
reportException(PyObject *pErrType, PyObject *pErrValue, PyObject *pErrTraceback)
{
	char	   *errName,
			   *errValue,
			   *errTraceback = "";
	PyObject   *traceback_list;
	PyObject   *pTemp;
	PyObject   *tracebackModule = PyImport_ImportModule("traceback");
	PyObject   *format_exception = PyObject_GetAttrString(tracebackModule, "format_exception");
	PyObject   *newline = PyString_FromString("\n");
	int			severity;

	PyErr_NormalizeException(&pErrType, &pErrValue, &pErrTraceback);
	pTemp = PyObject_GetAttrString(pErrType, "__name__");
	errName = PyString_AsString(pTemp);
	errValue = PyString_AsString(PyObject_Str(pErrValue));
	if (pErrTraceback != NULL)
	{
		traceback_list = PyObject_CallFunction(format_exception, "(O,O,O)", pErrType, pErrValue, pErrTraceback);
		errTraceback = PyString_AsString(PyObject_CallMethod(newline, "join", "(O)", traceback_list));
		Py_DECREF(pErrTraceback);
		Py_DECREF(traceback_list);
	}

	if (IsAbortedTransactionBlockState())
	{
		severity = WARNING;
	}
	else
	{
		severity = ERROR;
	}
#if PG_VERSION_NUM >= 130000
	if (errstart(severity, TEXTDOMAIN))
#else
	if (errstart(severity, __FILE__, __LINE__, PG_FUNCNAME_MACRO, TEXTDOMAIN))
#endif
	{
#if PG_VERSION_NUM >= 130000
		if (errstart(severity, TEXTDOMAIN))
#else
		if (errstart(severity, __FILE__, __LINE__, PG_FUNCNAME_MACRO, TEXTDOMAIN))
#endif
			errmsg("Error in python: %s", errName);
		errdetail("%s", errValue);
		errdetail_log("%s", errTraceback);
	}
	Py_DECREF(pErrType);
	Py_DECREF(pErrValue);
	Py_DECREF(format_exception);
	Py_DECREF(tracebackModule);
	Py_DECREF(newline);
	Py_DECREF(pTemp);
#if PG_VERSION_NUM >= 130000
		errfinish(__FILE__, __LINE__, PG_FUNCNAME_MACRO);
#else
		errfinish(0);
#endif
}
