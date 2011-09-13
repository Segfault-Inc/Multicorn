/*-------------------------------------------------------------------------
 *
 *          foreign-data wrapper for dummy
 *
 * copyright (c) 2011, postgresql global development group
 *
 * this software is released under the postgresql licence
 *
 * author: dickson s. guedes <guedes@guedesoft.net>
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


    attinmeta = TupleDescGetAttInMetadata(rel->rd_att);
    state = (DummyState *) palloc(sizeof(DummyState));
    state->rownum = 0;
    state->attinmeta = attinmeta;
    node->fdw_state = (void *) state;
    return;
}


static TupleTableSlot *
dummy_iterate(ForeignScanState *node)
{
    TupleTableSlot            *slot = node->ss.ss_ScanTupleSlot;
    Relation                relation = node->ss.ss_currentRelation;

    DummyState      *state = (DummyState *) node->fdw_state;

    HeapTuple        tuple;

    int                total_attributes, i;
    char            **tup_values;
    MemoryContext        oldcontext;
    PyObject *pName, *pModule, *pDict, *pFunc;
    PyObject *pArgs, *pValue;

    ExecClearTuple(slot);
    total_attributes = relation->rd_att->natts;
    tup_values = (char **) palloc(sizeof(char *) * total_attributes);

    if (state->rownum > 10) {
      return slot;
    }
    /* Python lol */

    Py_Initialize();
    pName = PyUnicode_FromString("lol_module");
    pModule = PyImport_Import(pName);
    Py_DECREF(pName);

    if (pModule != NULL) {
        pFunc = PyObject_GetAttrString(pModule, "lol");
        /* pFunc is a new reference */

        if (pFunc && PyCallable_Check(pFunc)) {
            pArgs = PyTuple_New(0);
            pValue = PyObject_CallObject(pFunc, pArgs);
            Py_DECREF(pArgs);
            if (pValue != NULL) {
              printf("Result of call lolmao: %s\n", PyString_AsString(pValue));
              Py_DECREF(pValue);
            }
            else {
                Py_DECREF(pFunc);
                Py_DECREF(pModule);
                PyErr_Print();
                fprintf(stderr,"Call failed\n");
                return 1;
            }
        }
        else {
            if (PyErr_Occurred())
                PyErr_Print();
            fprintf(stderr, "Cannot find function \"%s\"\n", "lol");
        }
        Py_XDECREF(pFunc);
        Py_DECREF(pModule);
    }
    else {
        PyErr_Print();
        fprintf(stderr, "Failed to load \"%s\"\n", "lol");
        return 1;
    }
    Py_Finalize();

    /*
     * FIXME
     *
     * actually i'm using a query that fetches all object class, but there
     * some objects that don't have attibute, this must be handled.
     *
     * TODO
     * the attribute fecthing could be improve to not loops every dummy_iterate call.
     */
    for (i=0; i < total_attributes; i++)
    {
        tup_values[i] = PyString_AsString(pValue);
    }

    /* TODO: needs a switch context here? */
    oldcontext = MemoryContextSwitchTo(node->ss.ps.ps_ExprContext->ecxt_per_query_memory);
    tuple = BuildTupleFromCStrings(state->attinmeta, tup_values);
    MemoryContextSwitchTo(oldcontext);
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
    /* TODO : Do things */
}


