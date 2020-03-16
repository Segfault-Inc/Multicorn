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
#include "multicorn.h"
#include "miscadmin.h"


struct module_state
{
	PyObject   *error;
};

#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif

static PyObject *
log_to_postgres(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char	   *message = NULL;
	char	   *hintstr = NULL,
			   *detailstr = NULL;
	int			level = 1;
	int			severity;
	PyObject   *hint,
			   *p_message,
			   *detail;

	if (!PyArg_ParseTuple(args, "O|i", &p_message, &level))
	{
		errorCheck();
		Py_INCREF(Py_None);
		return Py_None;
	}
	if (PyBytes_Check(p_message))
	{
		message = PyBytes_AsString(p_message);
	}
	else if (PyUnicode_Check(p_message))
	{
		message = strdup(PyUnicode_AsPgString(p_message));
	}
	else
	{

		PyObject   *temp = PyObject_Str(p_message);

		errorCheck();
		message = strdup(PyString_AsString(temp));
		errorCheck();
		Py_DECREF(temp);
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
		}
		if (detail != NULL && detail != Py_None)
		{
			detailstr = PyString_AsString(detail);
			errdetail("%s", detailstr);
		}
		Py_DECREF(args);
		Py_DECREF(kwargs);
		errfinish(0);
	}
	else
	{
		Py_DECREF(args);
		Py_DECREF(kwargs);
	}
	Py_INCREF(Py_None);
	return Py_None;
}

/* 
 * Used to call an c-function from plpython
 * so we can get an appropriate execution
 * context for all of our FDW methods.
 */

static PyObject *
_getInstanceByOid(PyObject *self, PyObject *args)
{
	Oid foreigntableid;
	CacheEntry *entry = NULL;
	PyObject   *pobj = PyTuple_GetItem(args, 0);
	bool		found = false;

	if (pobj == NULL || !PyLong_Check(pobj))
	{
		errorCheck();
		Py_INCREF(Py_None);
		return Py_None;
	}

	foreigntableid = (Oid)PyLong_AsLong(pobj);
	entry = hash_search(InstancesHash, &foreigntableid, HASH_FIND,
			    &found);

	if (!found || entry->value == NULL)
	{
		Py_INCREF(Py_None);
		return Py_None;
	}

	Py_INCREF(entry->value);
	return entry->value;
	
}

#if MAX_TRAMPOLINE_ARGS != 5
#error MAX_TRAMPOLINE_ARGS must be 5 or the code below must change.
#endif
typedef void *(*TrampolineFuncInternal)(void *, void *, void *, void *, void *);
static PyObject *
_plpy_trampoline(PyObject *self, PyObject *args)
{
	TrampolineData *td = multicorn_trampoline_data;
	MemoryContext old_context;

	/* We don't want any args, so ignore them. */
	if (args != NULL)
	{
	  Py_DECREF(args);
	  args = NULL;
	}
	
	/* If we are re-entered, we may need to use
	 * another trampoline for this same process.
	 * So clear this to make sure the error
	 * check when setting up the trampoline
	 * doesn't complain.
	*/
	multicorn_trampoline_data = NULL;

	/* Make sure we are in the same
	 * memory context as when trampoline
	 *  was called.
	 */

	old_context = MemoryContextSwitchTo(td->target_context);

#if PG_VERSION_NUM < 100000
	{
	  /* Since we may have come through
	   * plpython to get here, we need to
	   * do an SPI_push to make sure
	   * we have a new spi context.
	   */
	  bool spi_pushed = SPI_push_conditional();

#endif

	  /* C Calling convention means we can just shove the
	   * maximum number of args into the function and it
	   * will just ignore the ones it doesn't want.
	   */
	  td->return_data = 
	    ((TrampolineFuncInternal)td->func)(td->args[0],
					       td->args[1],
					       td->args[2],
					       td->args[3],
					       td->args[4]);

#if PG_VERSION_NUM < 100000
	SPI_pop_conditional(spi_pushed);
	}
#endif
	MemoryContextSwitchTo(old_context);
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
py_check_interrupts(PyObject *self, PyObject *args, PyObject *kwargs)
{
	CHECK_FOR_INTERRUPTS();
	Py_INCREF(Py_None);
	return Py_None;
}


static PyMethodDef UtilsMethods[] = {
	{"_log_to_postgres", (PyCFunction) log_to_postgres, METH_VARARGS | METH_KEYWORDS, "Log to postresql client"},
	{"check_interrupts", (PyCFunction) py_check_interrupts, METH_VARARGS | METH_KEYWORDS, "Gives control back to PostgreSQL"},
	{"_plpy_trampoline", (PyCFunction) _plpy_trampoline, METH_NOARGS,
	 "Internal use only, call the trampline function."},
	{"_getInstanceByOid", (PyCFunction) _getInstanceByOid, METH_VARARGS,
	 "Get the multicorn FDW instance by the table oid."},
	{NULL, NULL, 0, NULL}
};

#if PY_MAJOR_VERSION >= 3

static struct PyModuleDef moduledef = {
	PyModuleDef_HEAD_INIT,
	"multicorn._utils",
	NULL,
	sizeof(struct module_state),
	UtilsMethods,
	NULL,
	NULL,
	NULL,
	NULL
};

#define INITERROR return NULL

PyObject *
PyInit__utils(void)
#else
#define INITERROR return

void
init_utils(void)
#endif
{
#if PY_MAJOR_VERSION >= 3
	PyObject   *module = PyModule_Create(&moduledef);
#else
	PyObject   *module = Py_InitModule("multicorn._utils", UtilsMethods);
#endif
	struct module_state *st;

	if (module == NULL)
		INITERROR;
	st = GETSTATE(module);

#if PY_MAJOR_VERSION >= 3
	return module;
#endif
}
