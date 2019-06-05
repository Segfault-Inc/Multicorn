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
#include <stdio.h>
#include <Python.h>

#include "postgres.h"
#include "multicorn.h"
#include "miscadmin.h"
#include "executor/spi.h"

struct module_state
{
	PyObject   *error;
};

/* Used to name the PyCapsule type */
#define STATEMENT_NAME "prepared statement"


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

static PyObject *
py_check_interrupts(PyObject *self, PyObject *args, PyObject *kwargs)
{
	CHECK_FOR_INTERRUPTS();
	Py_INCREF(Py_None);
	return Py_None;
}

static void mylog(const char *m, const char *h, const char *d,
		const char *f, int line,
		const char *fn) {

	FILE *fp = fopen("/tmp/mylog", "a");
	fprintf(fp, "mylog(%s, %s, %s, %s, %d, %s)\n", m, h, d, f, line, fn);
	fclose(fp);
	
	if (errstart(WARNING, f, line, fn, TEXTDOMAIN))
	{
	  errmsg("%s", m);
	  if (h != NULL)
	  {
	    errhint("%s", h);
	  }
	  if (d != NULL)
	  {
	    errdetail("%s", d);
	  }
	  errfinish(0);
	}
}

#define MYLOG(m, h, d) mylog(m, h, d, __FILE__, __LINE__, PG_FUNCNAME_MACRO)
#define VLOG(...) do { snprintf(logbuff, sizeof(logbuff), __VA_ARGS__); MYLOG(logbuff, NULL, NULL); } while (0)

static PyObject *
py_fetch(PyObject *self, PyObject *args)
{
	PyObject *pret = NULL;
	const size_t row_count = SPI_processed;
	const size_t natts = SPI_tuptable != NULL ? SPI_tuptable->tupdesc->natts:0;
	size_t i = 0;
	int j = 0;
	AttInMetadata *attinmeta = NULL;
	ConversionInfo **cinfos = NULL;

	/* We don't want any args, so ignore them. */
	if (args != NULL)
	{
	  Py_DECREF(args);
	  args = NULL;
	}
	/*
	  return none if there are no tuples.
	  This differs from returning an
	  empty list if no rows were
	  returned.
	*/
	
	if (SPI_tuptable == NULL)
	{
	  pret = Py_None;
	  Py_INCREF(pret);
	  return pret;
	}
	
	attinmeta = TupleDescGetAttInMetadata(SPI_tuptable->tupdesc);
	
	cinfos = SPI_palloc(sizeof(ConversionInfo *) * natts);
	initConversioninfo(cinfos, attinmeta);
	pret = PyTuple_New(row_count);
  
	if (pret == NULL)
	{
	  errorCheck();
	  Py_INCREF(Py_None);
	  pret = Py_None;
	  return pret;
	}

	for (i = 0; i < row_count; ++i)
	{
	  PyObject   *rdict = PyDict_New();
	  for (j = 0; j < natts; ++j) {
	    Form_pg_attribute attr = TupleDescAttr(SPI_tuptable->tupdesc,j);
	    PyObject *pobj = NULL;
	    bool isnull = false;
	    Datum d = SPI_getbinval(SPI_tuptable->vals[i],
				    SPI_tuptable->tupdesc,
				    j+1, &isnull);
	    if (isnull == true)
	    {
	      Py_INCREF(Py_None);
	      pobj = Py_None;
	    }
	    else
	    {
	      pobj = datumToPython(d, cinfos[attr->attnum - 1]->atttypoid,
				   cinfos[attr->attnum - 1]);
	      if (pobj == NULL)
		{
		  errorCheck();
		  
		  Py_DECREF(pret);
		  pret = NULL;
		  
		  Py_DECREF(rdict);
		  rdict = NULL;
		  
		  Py_INCREF(Py_None);
		  pret = Py_None;
		  return pret;
		    
		}
	    }
	    PyDict_SetItemString(rdict, cinfos[attr->attnum - 1]->attrname,
				 pobj);
	    Py_DECREF(pobj);
	    pobj = NULL;
	  }
	  
	  PyTuple_SetItem(pret, i, rdict);
	  /* PyTupe_SetItem steals the reference,
	     so we don't need to drop our reference
	     to rdict.
	  */
	  rdict = NULL;
	}
	return pret;
}

static PyObject *
py_execute_stmt(PyObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject   *stmt_object = NULL;
	SPIPlanPtr stmt = NULL;
	PyObject   *pret = NULL;
	PyObject   *pobj = NULL;
	PyObject   *converter = NULL;
	PyObject   *sqlargs_obj = NULL;
	PyObject   *converters_obj = NULL;
	Py_ssize_t arg_count = 0;
	int        i = 0;
	int	   iret = 0;
	bool       read_only = false;
	char       *nulls = NULL;
	Datum      *stmt_args = NULL;

	char logbuff[200];

	if (!PyArg_ParseTuple(args, "OO!O!", &stmt_object,
			      &PyTuple_Type, &sqlargs_obj,
			      &PyTuple_Type, &converters_obj))
	{
	  goto errout;
	}

	stmt = PyCapsule_GetPointer(stmt_object, STATEMENT_NAME);

	if (stmt == NULL)
	{
	  goto errout;
	}

	arg_count = PyTuple_Size(sqlargs_obj);

	multicorn_connect();
	
	if (arg_count != PyTuple_Size(converters_obj) ||
	    arg_count != SPI_getargcount(stmt))
	{
	  /* XXXXX FIXME: throw arg count mismatch exception. */
	  goto errout;
	}
	
	nulls = (char *)SPI_palloc(sizeof(*nulls)*(arg_count+1));
	memset (nulls, ' ', (arg_count+1)*sizeof(*nulls));
	nulls[arg_count] = 0;
	
	stmt_args = (Datum *)SPI_palloc(sizeof(*stmt_args)*arg_count);
	memset (stmt_args, 0, arg_count*sizeof(*stmt_args));

	for (i = 0; i < arg_count; i++)
	{
	  PyObject *str=NULL;
	  PyObject *islob_obj = NULL;
	  bool islob = false;

	  pobj = PyTuple_GetItem(sqlargs_obj, i);

	  if (pobj == NULL || pobj == Py_None)
	  {
	    VLOG("arg %d is NULL", i);
	    nulls[i] = 'n';
	    continue;
	  }

	  str = PyObject_Str(pobj);
	  
	  converter = PyTuple_GetItem(converters_obj, i);
	  if (converter == NULL || converter == Py_None)
	  {
	    VLOG("arg %d is No Converter", i);
	    goto errout;
	  }

	  Py_DECREF(str);
	  
	  /* Don't use the macro here since we
	     are in the middle of a pg call.
	  */
	  islob_obj = PyObject_CallMethod(converter, "isLob", "()");

	  if (islob_obj == NULL)
	  {
	    VLOG("arg %d is No islob", i);
	    goto errout;
	  }
	  
	  islob = PyObject_IsTrue(islob_obj);
	  Py_DECREF(islob_obj);
	    
	  pobj = PyObject_CallMethod(converter, "getdatum", "(O)", pobj);

	  if (PyString_Check(pobj))
	  {
	    VLOG("arg %d is string islob=%d", i, islob);
	    if (islob)
	      {
		stmt_args[i] = DirectFunctionCall1(textin,
						   CStringGetDatum(pstrdup(PyString_AsString(pobj))));
	      }
	      else
	      {
		stmt_args[i]=CStringGetDatum(pstrdup(PyString_AsString(pobj)));
	      }
	  }
	  else if (PyLong_Check(pobj))
	  {
	    VLOG("arg %d is long", i);
	    stmt_args[i] = Int64GetDatum(PyLong_AsLong(pobj));
	  }
	  else if (PyInt_Check(pobj))
	  {
	    VLOG("arg %d is int", i);
	    stmt_args[i] = Int32GetDatum(PyInt_AsLong(pobj));
	  }
	  else if (PyFloat_Check(pobj))
	  {
	    VLOG("arg %d is float", i);
	    stmt_args[i] = Float8GetDatum(PyFloat_AsDouble(pobj));
	  }
	  
	  Py_DECREF(pobj);

	}

	iret = SPI_execute_plan(stmt, stmt_args, nulls,
				read_only, 0);

	pret = PyInt_FromLong(iret);
	if (pret == NULL)
	{
	  goto errout;
	}
	
  out:
	if (args != NULL)
	{
	  Py_DECREF(args);
	}

	if (kwargs != NULL)
	{
	  Py_DECREF(kwargs);
	}

	return pret;
	
  errout:
	errorCheck();
	Py_INCREF(Py_None);
	pret = Py_None;
	goto out;
}

static PyObject *
py_execute(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char	   *sql = NULL;
	bool       read_only=false;
	PyObject   *read_only_object = NULL;
	int        count=0;
	PyObject   *pret = NULL;
	int        iret = 0;
	
	
	if (!PyArg_ParseTuple(args, "sOi", &sql, &read_only_object, &count))
	{
	  errorCheck();
	  if (args != NULL) {
	    Py_DECREF(args);
	  }

	  if (kwargs != NULL) {
	    Py_DECREF(kwargs);
	  }

	  Py_INCREF(Py_None);
	  return Py_None;
	}

	read_only = PyObject_IsTrue(read_only_object);

	multicorn_connect();
	
	iret = SPI_execute(sql, read_only, count);

	if (args != NULL)
	{
	  Py_DECREF(args);
	}

	if (kwargs != NULL)
	{
	  Py_DECREF(kwargs);
	}

	pret = PyInt_FromLong(iret);
	
	return pret;
}

static void
stmt_destructor(PyObject *po)
{
	SPIPlanPtr stmt = PyCapsule_GetPointer(po, STATEMENT_NAME);

	if (stmt == NULL)
	{
	  errorCheck();
	  return;
	}

	SPI_freeplan(stmt);
}

static PyObject *
py_prepare(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char	   *sql = NULL;
	PyObject   *pret = NULL;
	PyObject   *pobj = NULL;
	SPIPlanPtr stmt = NULL;
	Py_ssize_t arg_count = 0;
	Oid 	   *argtypes = NULL;
	Py_ssize_t i;

	arg_count = PyTuple_Size(args) - 1;
	pobj = PyTuple_GetItem(args, 0);
	if (pobj == NULL)
	{
	  goto errout;
	}

	sql = PyString_AsString(pobj);
	
	if (sql == NULL)
	{
	  goto errout;
	}

	multicorn_connect();

	argtypes = SPI_palloc(sizeof(*argtypes) * arg_count);
	if (argtypes == NULL) {
	  goto errout;
	}

	memset(argtypes, 0, sizeof(*argtypes) * arg_count);
	for (i = 0; i < arg_count; ++i)
	{
	  pobj = PyTuple_GetItem(args, i + 1);
	  /* Don't use the macro here since we
	     are in the middle of a pg call.
	  */
	  pobj = PyObject_CallMethod(pobj, "getOID", "()");
	  if (pobj == NULL) {
	    goto errout;
	  }
	  
	  if (!PyLong_Check(pobj))
	  {
	    /* XXXXX Fixme:
	       throw a type error.
	    */
	    Py_DECREF(pobj);
	    goto errout;
	  }
	  
	  argtypes[i] = PyLong_AsLong(pobj);
	  Py_DECREF(pobj);
	}

	/* 
	 * SPI_Prepare makes a copy
	 * of the argtypes in _SPI_make_plan_non_temp
	 * so we must still free it.
	 */

	
	stmt = SPI_prepare(sql, arg_count, argtypes);

	
	if (stmt == NULL)
	{
	  goto errout;
	}

	if (SPI_keepplan(stmt))
	{
	  goto errout;
	}

	pret = PyCapsule_New(stmt, STATEMENT_NAME, stmt_destructor);
	
	if (pret == NULL)
	{
	  goto errout;
	}

  out:
	if (args != NULL)
	{
	  Py_DECREF(args);
	}
	
	if (kwargs != NULL)
	{
	  Py_DECREF(kwargs);
	}

	if (argtypes != NULL)
	{
	  SPI_pfree(argtypes);
	}

	return pret;

  errout:
	errorCheck();
	pret = Py_None;
	Py_INCREF(Py_None);
	goto out;
	  

}



static PyMethodDef UtilsMethods[] = {
	{"_log_to_postgres", (PyCFunction) log_to_postgres,
	 METH_VARARGS | METH_KEYWORDS, "Log to postresql client"},
	{"_execute", (PyCFunction) py_execute, METH_VARARGS | METH_KEYWORDS,
	 "Execute SQL"},
	{"_fetch", (PyCFunction) py_fetch, METH_VARARGS,
	 "Fetch a tuple of results from last execute."},
	{"_prepare", (PyCFunction) py_prepare,  METH_VARARGS | METH_KEYWORDS,
	 "Prepare a statement to execute."},
	{"_execute_stmt", (PyCFunction) py_execute_stmt,
	 METH_VARARGS | METH_KEYWORDS,
	 "Execute a previously prepared statement."},
	{"check_interrupts", (PyCFunction) py_check_interrupts,
	 METH_VARARGS | METH_KEYWORDS, "Gives control back to PostgreSQL"},
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
