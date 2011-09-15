/*-------------------------------------------------------------------------
 *
 *          foreign-data wrapper for dummy
 *
 * this software is released under the postgresql licence
 *
 * author: Kozea
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include <Python.h>

PG_MODULE_MAGIC;

/*
 * Options for the dummy wrapper
 */

typedef struct DummyState
{
  AttInMetadata *attinmeta;
  int rownum;
  PyObject *pFunc;
  PyObject *pIterator;
} DummyState;

extern Datum dummy_fdw_handler(PG_FUNCTION_ARGS);
extern Datum dummy_fdw_validator(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(dummy_fdw_handler);
PG_FUNCTION_INFO_V1(dummy_fdw_validator);

/*
 * FDW functions declarations
 */
static FdwPlan *dummy_plan(Oid foreign_table_id, PlannerInfo *root, RelOptInfo *base_relation);
static void dummy_explain(ForeignScanState *node, ExplainState *es);
static void dummy_begin(ForeignScanState *node, int eflags);
static TupleTableSlot *dummy_iterate(ForeignScanState *node);
static void dummy_rescan(ForeignScanState *node);
static void dummy_end(ForeignScanState *node);

/*
  Helpers
*/
static void dummy_get_options(Oid foreign_table_id, PyObject *options_dict, char **module);
static void dummy_get_attributes_name(TupleDesc desc, PyObject* list);
static HeapTuple pysequence_to_postgres_tuple(TupleDesc desc, PyObject *pyseq);
static HeapTuple pydict_to_postgres_tuple(TupleDesc desc, PyObject *pydict);
static char* pyobject_to_cstring(PyObject *pyobject);

const char* DATE_FORMAT_STRING = "%Y-%m-%d";

Datum
dummy_fdw_handler(PG_FUNCTION_ARGS)
{
  FdwRoutine *fdw_routine = makeNode(FdwRoutine);

  fdw_routine->PlanForeignScan = dummy_plan;
  fdw_routine->ExplainForeignScan = dummy_explain;
  fdw_routine->BeginForeignScan = dummy_begin;
  fdw_routine->IterateForeignScan = dummy_iterate;
  fdw_routine->ReScanForeignScan = dummy_rescan;
  fdw_routine->EndForeignScan = dummy_end;

  PG_RETURN_POINTER(fdw_routine);
}

Datum
dummy_fdw_validator(PG_FUNCTION_ARGS)
{
  PG_RETURN_BOOL(true);
}

static FdwPlan *
dummy_plan( Oid foreign_table_id,
            PlannerInfo *root,
            RelOptInfo  *base_relation)
{
  FdwPlan *fdw_plan;

  fdw_plan = makeNode(FdwPlan);

  fdw_plan->startup_cost = 10;
  base_relation->rows = 1;
  fdw_plan->total_cost = 15;

  return fdw_plan;
}

static void
dummy_explain(ForeignScanState *node, ExplainState *es)
{
  /* TODO: calculate real values */
  ExplainPropertyText("Foreign dummy", "dummy", es);

  if (es->costs)
    {
      ExplainPropertyLong("Foreign dummy cost", 10.5, es);
    }
}


static void
dummy_begin(ForeignScanState *node, int eflags)
{
  /*  TODO: do things if necessary */
  AttInMetadata  *attinmeta;
  Relation        rel = node->ss.ss_currentRelation;
  DummyState      *state;
  PyObject *pName, *pModule, *pArgs, *pValue, *pOptions, *pFunc, *pClass, *pObj, *pMethod, *pColumns;
  char *module;

  attinmeta = TupleDescGetAttInMetadata(rel->rd_att);
  state = (DummyState *) palloc(sizeof(DummyState));
  state->rownum = 0;
  state->attinmeta = attinmeta;
  node->fdw_state = (void *) state;

  Py_Initialize();
  pOptions = PyDict_New();
  dummy_get_options(RelationGetRelid(node->ss.ss_currentRelation),
                    pOptions, &module);
  pName = PyUnicode_FromString("fdw");
  pModule = PyImport_Import(pName);
  if (PyErr_Occurred()) {
    PyErr_Print();
  }
  Py_DECREF(pName);

  if (pModule != NULL) {
    pFunc = PyObject_GetAttrString(pModule, "getClass");
    if (PyErr_Occurred()) {
      PyErr_Print();
      elog(ERROR, "Error in python, see the logs");
    }
    Py_DECREF(pModule);

    pArgs = PyTuple_New(1);
    pName = PyString_FromString(module);
    PyTuple_SetItem(pArgs, 0, pName);

    pClass = PyObject_CallObject(pFunc, pArgs);
    if (PyErr_Occurred()) {
      PyErr_Print();
      elog(ERROR, "Error in python, see the logs");
    }

    Py_DECREF(pArgs);
    Py_DECREF(pFunc);
    pArgs = PyTuple_New(2);
    pColumns = PyList_New(0);
    dummy_get_attributes_name(node->ss.ss_currentRelation->rd_att, pColumns);
    PyTuple_SetItem(pArgs, 0, pOptions);
    PyTuple_SetItem(pArgs, 1, pColumns);
    /* Py_DECREF(pName); -> Make the pg crash -> ??*/
    pObj = PyObject_CallObject(pClass, pArgs);
    if (PyErr_Occurred()) {
      PyErr_Print();
      elog(ERROR, "Error in python, see the logs");
    }
    Py_DECREF(pArgs);
    Py_DECREF(pOptions);
    Py_DECREF(pClass);

    pArgs = PyTuple_New(0);
    pMethod = PyObject_GetAttrString(pObj, "execute");
    pValue = PyObject_CallObject(pMethod, pArgs);
    if (PyErr_Occurred()) {
        PyErr_Print();
        elog(ERROR, "Error in python, see the logs");
    }
    state->pIterator = PyObject_GetIter(pValue);
    Py_DECREF(pValue);
    Py_DECREF(pObj);
    Py_DECREF(pMethod);
    Py_DECREF(pArgs);
  } else {
    PyErr_Print();
    elog(ERROR, "Failed to load module");
  }
}


static TupleTableSlot *
dummy_iterate(ForeignScanState *node)
{
  TupleTableSlot  *slot = node->ss.ss_ScanTupleSlot;
  DummyState      *state = (DummyState *) node->fdw_state;
  HeapTuple        tuple;
  MemoryContext    oldcontext;
  PyObject        *pValue, *pArgs, *pIterator;

  ExecClearTuple(slot);

  pArgs = PyTuple_New(0);
  pIterator = state->pIterator;
  Py_DECREF(pArgs);
  if (pIterator == NULL) {
      /* propagate error */
  }
  pValue = PyIter_Next(pIterator);
  if (PyErr_Occurred()) {
    /* Stop iteration */
    PyErr_Print();
    return slot;
  }
  if (pValue == NULL){
    return slot;
  }
  oldcontext = MemoryContextSwitchTo(node->ss.ps.ps_ExprContext->ecxt_per_query_memory);
  MemoryContextSwitchTo(oldcontext);
  if(PyMapping_Check(pValue)){
      tuple = pydict_to_postgres_tuple(node->ss.ss_currentRelation->rd_att
          , pValue);
  }else if (PySequence_Check(pValue)){
      tuple = pysequence_to_postgres_tuple(node->ss.ss_currentRelation->rd_att
          , pValue);
  }else{
    elog(ERROR, "Cannot transform anything else than mappings and sequences to rows");
  }
  Py_DECREF(pValue);
  ExecStoreTuple(tuple, slot, InvalidBuffer, false);
  state->rownum++;
  return slot;
}

static void
dummy_rescan(ForeignScanState *node)
{
  DummyState *state = (DummyState *) node->fdw_state;
  state->rownum = 0;
}

static void
dummy_end(ForeignScanState *node)
{
  DummyState *state = (DummyState *) node->fdw_state;
  Py_DECREF(state->pIterator);
  Py_Finalize();
}


static void
dummy_get_options(Oid foreign_table_id, PyObject *pOptions, char **module)
{
  ForeignTable    *f_table;
  ForeignServer   *f_server;
  List            *options;
  ListCell        *lc;
  bool             got_module = false;
  PyObject        *pStr;

  f_table = GetForeignTable(foreign_table_id);
  f_server = GetForeignServer(f_table->serverid);

  options = NIL;
  options = list_concat(options, f_table->options);
  options = list_concat(options, f_server->options);

  foreach(lc, options) {

    DefElem *def = (DefElem *) lfirst(lc);

    if (strcmp(def->defname, "wrapper") == 0) {
      *module = defGetString(def);
      got_module = true;
    } else {
      pStr = PyString_FromString(defGetString(def));
      PyDict_SetItemString(pOptions, def->defname, pStr);
      Py_DECREF(pStr);
    }
  }
  if (!got_module) {
    ereport(ERROR,
            (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
             errmsg("wrapper option not found"),
             errhint("You must set wrapper option to a ForeignDataWrapper python class, for example fdw.csv.CsvFdw")));
  }
}

static HeapTuple
pydict_to_postgres_tuple(TupleDesc desc, PyObject *pydict)
{
  HeapTuple      tuple;
  AttInMetadata *attinmeta = TupleDescGetAttInMetadata(desc);
  PyObject      *pStr;
  char          *key;
  char         **tup_values;
  int            i, natts;
  natts = desc->natts;
  tup_values = (char **) palloc(sizeof(char *) * natts);
  for(i = 0; i< natts; i++){
    key = NameStr(desc->attrs[i]->attname);

    pStr = PyMapping_GetItemString(pydict, key);
    tup_values[i] = pyobject_to_cstring(pStr);
    Py_DECREF(pStr);
  }
  tuple = BuildTupleFromCStrings(attinmeta, tup_values);
  return tuple;
}

static HeapTuple
pysequence_to_postgres_tuple(TupleDesc desc, PyObject *pyseq)
{
  HeapTuple      tuple;
  AttInMetadata *attinmeta = TupleDescGetAttInMetadata(desc);
  char         **tup_values;
  Py_ssize_t     i, natts;
  PyObject      *pStr;

  natts = desc->natts;
  if (PySequence_Size(pyseq) != natts) {
    elog(ERROR, "The python backend did not return a valid sequence");
  } else {
      tup_values = (char **) palloc(sizeof(char *) * natts);
      for(i = 0; i< natts; i++){
        pStr = PySequence_GetItem(pyseq, i);
        tup_values[i] = pyobject_to_cstring(pStr);
        Py_DECREF(pStr);
      }
      tuple = BuildTupleFromCStrings(attinmeta, tup_values);
  }
  return tuple;
}


static char* pyobject_to_cstring(PyObject *pyobject)
{
    PyObject * date_module = PyImport_Import(
                PyUnicode_FromString("datetime"));
    Py_ssize_t unicode_size;
    PyObject * date_cls = PyObject_GetAttrString(date_module, "date");
    PyObject *pStr;
    if(PyNumber_Check(pyobject)){
        return PyString_AsString(PyObject_Str(pyobject));
    }
    if(pyobject == Py_None){
        return NULL;
    }
    if(PyUnicode_Check(pyobject)){
        unicode_size = PyUnicode_GET_SIZE(pyobject);
        elog(INFO, "Unicode object");
        return PyString_AsString(PyUnicode_Encode(pyobject, unicode_size, "utf8", NULL));
    }
    if(PyObject_IsInstance(pyobject, date_cls)){
        PyObject * date_format_method = PyObject_GetAttrString(pyobject, "strftime");
        PyObject * pArgs = PyTuple_New(1);
        PyObject * formatted_date = PyObject_CallObject(date_format_method, pArgs);
        Py_DECREF(pArgs);
        Py_DECREF(date_format_method);
        pStr = PyString_FromString(DATE_FORMAT_STRING);
        PyTuple_SetItem(pArgs, 0, pStr);
        Py_DECREF(pStr);
        return PyString_AsString(formatted_date);
    }
    Py_DECREF(date_module);
    Py_DECREF(date_cls);
    return PyString_AsString(pyobject);
}

static void dummy_get_attributes_name(TupleDesc desc, PyObject * list)
{
    char * key;
    Py_ssize_t i, natts;
    natts = desc->natts;
    for(i = 0; i< natts; i++){
        key = NameStr(desc->attrs[i]->attname);
        PyList_Append(list, PyString_FromString(key));
    }
}
