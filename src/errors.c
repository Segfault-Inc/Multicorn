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

	PyErr_NormalizeException(&pErrType, &pErrValue, &pErrTraceback);
	pTemp = PyObject_GetAttrString(pErrType, "__name__");
	errName = PyString_AsString(pTemp);
	errValue = PyString_AsString(PyObject_Str(pErrValue));
	if (pErrTraceback)
	{
		traceback_list = PyObject_CallFunction(format_exception, "(O,O,O)", pErrType, pErrValue, pErrTraceback);
		errTraceback = PyString_AsString(PyObject_CallMethod(newline, "join", "(O)", traceback_list));
		Py_DECREF(pErrTraceback);
		Py_DECREF(traceback_list);
	}
	if (IsAbortedTransactionBlockState())
	{
		ereport(WARNING, (errmsg("Error in python: %s", errName),
						  errdetail("%s", errValue),
						  errdetail_log("%s", errTraceback)));
	}
	else
	{
		ereport(ERROR, (errmsg("Error in python: %s", errName),
						errdetail("%s", errValue),
						errdetail_log("%s", errTraceback)));
	}
	Py_DECREF(pErrType);
	Py_DECREF(pErrValue);
	Py_DECREF(format_exception);
	Py_DECREF(tracebackModule);
	Py_DECREF(newline);
	Py_DECREF(pTemp);
}

/*
 * Test if an exception of the given type has been raised. Returns: - false
 * if no exception has been raised, - true if an exception of the given class
 * has been raised - Abort with an error if another exception has been
 * raised.
 */
bool
try_except(char *exceptionname)
{
	PyObject   *p_errtype,
			   *p_errvalue,
			   *p_errtraceback,
			   *p_exceptionclass;
	bool		result = false;

	PyErr_Fetch(&p_errtype, &p_errvalue, &p_errtraceback);
	if (p_errtype)
	{
		p_exceptionclass = getClassString(exceptionname);
		if (PyErr_GivenExceptionMatches(p_errtype, p_exceptionclass))
		{
			result = true;
		}
		Py_DECREF(p_exceptionclass);
		if (!result)
		{
			reportException(p_errtype, p_errvalue, p_errtraceback);
		}
		Py_DECREF(p_errtraceback);
		Py_DECREF(p_errtype);
		Py_DECREF(p_errvalue);
	}
	return result;
}
