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

#include "postgres.h"
#include "access/relscan.h"
#include "access/reloptions.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/numeric.h"
#include "utils/date.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "pgtime.h"
#include "mb/pg_wchar.h"
#include "Python.h"
#include "datetime.h"


PG_MODULE_MAGIC;

typedef struct MulticornPlanState
{
	PyObject   *needed_columns;
	PyObject   *quals;
	PyObject   *params;
}	MulticornPlanState;


typedef struct MulticornExecState
{
	AttInMetadata *attinmeta;
	int			rownum;
	PyObject   *pIterator;
	MulticornPlanState *planstate;
}	MulticornExecState;

typedef enum MulticornParamType
{
	MulticornQUAL,
	MulticornPARAM_EXTERN,
	MulticornPARAM_EXEC,
	MulticornVAR,
	MulticornUNKNOWN
}	MulticornParamType;


extern Datum multicorn_handler(PG_FUNCTION_ARGS);
extern Datum multicorn_validator(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(multicorn_handler);
PG_FUNCTION_INFO_V1(multicorn_validator);

/*
 * FDW functions declarations
 */
static void multicornGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void multicornGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *multicornGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
					ForeignPath *best_path, List *tlist, List *scan_clauses);
static void multicornExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void multicornBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *multicornIterateForeignScan(ForeignScanState *node);
static void multicornReScanForeignScan(ForeignScanState *node);
static void multicornEndForeignScan(ForeignScanState *node);

/*
   Helpers
   */
void		multicorn_init_plan_state(RelOptInfo *baserel, Oid foreigntableid);
void		multicorn_error_check(void);
void		_PG_init(void);
void		multicorn_get_options(Oid foreigntableid, PyObject *options_dict, char **module);
void		multicorn_get_attributes_def(TupleDesc desc, PyObject *dict);
PyObject   *multicorn_datum_to_python(Datum datumvalue, Oid type, Form_pg_attribute attribute);
void		multicorn_report_exception(PyObject *pErrType, PyObject *pErrValue, PyObject *pErrTraceback);
PyObject   *multicorn_get_instance(Relation rel);
HeapTuple	BuildTupleFromCStringsWithSize(AttInMetadata *attinmeta, char **values, ssize_t *sizes);
void		multicorn_get_columns(List *columnlist, TupleDesc desc, PyObject *);
void		multicorn_get_column(Expr *expr, TupleDesc desc, PyObject *list);
const char *get_encoding_from_attribute(Form_pg_attribute attribute);
void		multicorn_execute(ForeignScanState *node);
PyObject   *multicorn_get_quals(ForeignScanState *node, Relation rel, MulticornExecState * state);
void		multicorn_clean_state(MulticornExecState * state);
void		multicorn_extract_condition(Expr *clause, PyObject *qual_list, PyObject *param_list, Relation baserel);
MulticornParamType multicorn_extract_qual(Node *left, Node *right, Relation base_rel, Form_pg_operator operator, PyObject **result);
PyObject   *multicorn_is_filter_on_column(RestrictInfo *restrictinfo, Relation baserel, PyObject *colname);
void multicorn_append_path_from_restrictinfo(PlannerInfo *root, RelOptInfo *baserel,
		   RestrictInfo *restrictinfo, double parampathrows, Relids allrels);

PyObject   *multicorn_get_class(char *className);
PyObject   *multicorn_get_multicorn(void);
void		multicorn_unnest(Node *value, Node **result);

HeapTuple	pysequence_to_postgres_tuple(TupleDesc desc, PyObject *pyseq);
HeapTuple	pydict_to_postgres_tuple(TupleDesc desc, PyObject *pydict);
ssize_t		pyobject_to_cstring(PyObject *pyobject, Form_pg_attribute attribute, char **buffer);

PyObject   *TABLES_DICT;

Datum
multicorn_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	fdw_routine->GetForeignRelSize = multicornGetForeignRelSize;
	fdw_routine->GetForeignPaths = multicornGetForeignPaths;
	fdw_routine->GetForeignPlan = multicornGetForeignPlan;
	fdw_routine->ExplainForeignScan = multicornExplainForeignScan;
	fdw_routine->BeginForeignScan = multicornBeginForeignScan;
	fdw_routine->IterateForeignScan = multicornIterateForeignScan;
	fdw_routine->ReScanForeignScan = multicornReScanForeignScan;
	fdw_routine->EndForeignScan = multicornEndForeignScan;
	PG_RETURN_POINTER(fdw_routine);
}

Datum
multicorn_validator(PG_FUNCTION_ARGS)
{

	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *className = NULL;
	ListCell   *cell;
	PyObject   *pOptions,
			   *pStr;

	pOptions = PyDict_New();
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "wrapper") == 0)
		{
			/* Only at server creation can we set the wrapper,	*/
			/* for security issues. */
			if (catalog == ForeignTableRelationId)
			{
				ereport(ERROR, (errmsg("%s", "Cannot set the wrapper class on the table"),
								errhint("%s", "Set it on the server")));
			}
			else
			{
				className = (char *) defGetString(def);
			}
		}
		else
		{
			pStr = PyString_FromString((char *) defGetString(def));
			PyDict_SetItemString(pOptions, def->defname, pStr);
		}
	}
	if (catalog == ForeignServerRelationId)
	{
		if (className == NULL)
		{
			ereport(ERROR, (errmsg("%s", "The wrapper parameter is mandatory, specify a valid class name")));
		}
		multicorn_get_class(className);
		multicorn_error_check();
	}
	PG_RETURN_VOID();
}

/*
 * multicornGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
multicornGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
	PyObject   *pObj,
			   *pMethod,
			   *pRowsAndWidth,
			   *pArgs,
			   *pQualsAndParams,
			   *pZero,
			   *pBuffer;
	Py_ssize_t	i;
	MulticornPlanState *state;
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	Relation	rel = RelationIdGetRelation(ftable->relid);

	multicorn_init_plan_state(baserel, foreigntableid);
	state = baserel->fdw_private;
	pObj = multicorn_get_instance(rel);
	Py_INCREF(pObj);
	RelationClose(rel);
	pQualsAndParams = PyList_New(0);
	for (i = 0; i < PyList_Size(state->quals); i++)
	{
		PyList_Append(pQualsAndParams, PyList_GetItem(state->quals, i));
	}
	for (i = 0; i < PyList_Size(state->params); i++)
	{
		PyList_Append(pQualsAndParams, PyList_GetItem(state->params, i));
	}

	pMethod = PyObject_GetAttrString(pObj, "get_rel_size");
	pArgs = Py_BuildValue("(O,O)", pQualsAndParams, state->needed_columns);
	pZero = Py_BuildValue("d");
	pRowsAndWidth = PyObject_CallObject(pMethod, pArgs);
	multicorn_error_check();
	pBuffer = PyTuple_GetItem(pRowsAndWidth, 0);
	PyNumber_Coerce(&pBuffer, &pZero);
	baserel->rows = PyFloat_AsDouble(pBuffer);
	pBuffer = PyTuple_GetItem(pRowsAndWidth, 1);
	PyNumber_Coerce(&pBuffer, &pZero);
	baserel->width = PyFloat_AsDouble(pBuffer);
	Py_DECREF(pObj);
}

void
multicorn_init_plan_state(RelOptInfo *baserel, Oid foreigntableid)
{
	if (baserel->fdw_private == NULL)
	{
		MulticornPlanState *state = (MulticornPlanState *) palloc(sizeof(MulticornPlanState));
		ForeignTable *ftable = GetForeignTable(foreigntableid);
		Relation	rel = RelationIdGetRelation(ftable->relid);
		Expr	   *current_expr;
		ListCell   *cell;

		state->needed_columns = PySet_New(NULL);
		state->quals = PyList_New(0);
		state->params = PyList_New(0);
		foreach(cell, baserel->reltargetlist)
		{
			current_expr = (Expr *) lfirst(cell);
			multicorn_get_column(current_expr, rel->rd_att, state->needed_columns);
		}
		foreach(cell, baserel->baserestrictinfo)
		{
			current_expr = ((RestrictInfo *) lfirst(cell))->clause;
			multicorn_get_column(current_expr, rel->rd_att, state->needed_columns);
			multicorn_extract_condition(current_expr, state->quals, state->params, rel);
		}
		RelationClose(rel);
		baserel->fdw_private = state;
	}
}

/*
 *	Test wether the given restrictinfo clause is an equal clause on the given
 *	column (identified by name)
 * */
PyObject *
multicorn_is_filter_on_column(RestrictInfo *restrictinfo, Relation baserel, PyObject *colname)
{
	Expr	   *clause = restrictinfo->clause;
	Expr	   *left;
	Expr	   *right;
	HeapTuple	tp;
	Form_pg_operator operator_tup;
	PyObject   *qual;
	OpExpr	   *op;
	int			isEq;
	MulticornParamType param_type;

	if (IsA(clause, OpExpr))
	{
		op = (OpExpr *) clause;
		left = list_nth(op->args, 0);
		right = list_nth(op->args, 1);
		tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
		operator_tup = (Form_pg_operator) GETSTRUCT(tp);
		ReleaseSysCache(tp);
		param_type = multicorn_extract_qual((Node *) left, (Node *) right, baserel, operator_tup, &qual);
		switch (param_type)
		{
			case MulticornPARAM_EXTERN:
			case MulticornPARAM_EXEC:
			case MulticornQUAL:
				isEq = PyObject_Compare(colname, PyObject_GetAttrString(qual, "field_name"));
				multicorn_error_check();
				return isEq == 0 ? qual : NULL;
			case MulticornVAR:
				isEq = PyObject_Compare(colname, PyObject_GetAttrString(qual, "field_name"));
				multicorn_error_check();
				return isEq == 0 ? qual : NULL;
				if (isEq == 0)
				{
					return qual;
				}
				isEq = PyObject_Compare(colname, PyObject_GetAttrString(qual, "value"));
				multicorn_error_check();
				return isEq == 0 ? qual : NULL;
			default:
				return NULL;
		}
	}
	return NULL;
}

void
multicorn_append_path_from_restrictinfo(PlannerInfo *root, RelOptInfo *baserel,
			RestrictInfo *restrictinfo, double parampathrows, Relids allrels)
{
	ParamPathInfo *ppi = makeNode(ParamPathInfo);
	Relids		otherrels = NULL;
	ForeignPath *foreignPath;

	if (allrels)
	{
		otherrels = bms_del_member(bms_copy((Bitmapset *) allrels), baserel->relid);
	}
	foreignPath = create_foreignscan_path(root, baserel,
										  parampathrows,
									  baserel->baserestrictcost.startup + 10,
										  parampathrows * baserel->width,
										  NIL,
										  NULL,
										  baserel->fdw_private);
	ppi->ppi_req_outer = otherrels;
	ppi->ppi_rows = parampathrows;
	ppi->ppi_clauses = lappend(ppi->ppi_clauses, restrictinfo);
	foreignPath->path.param_info = ppi;
	add_path(baserel, (Path *) foreignPath);
}

/*
 * multicornGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 */
static void
multicornGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	PyObject   *pObj,
			   *pMethod,
			   *pPathKeysList,
			   *pPathKeytuple,
			   *pPathKeyAttrName,
			   *pPathKeyCost,
			   *pZero,
			   *pQual;
	Py_ssize_t	i;
	ListCell   *ri_cell,
			   *eq_class_cell;
	EquivalenceClass *eq_class;
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	Relation	rel = RelationIdGetRelation(ftable->relid);
	RestrictInfo *restrictinfo;
	double		parampathrows;
	MulticornPlanState *state = baserel->fdw_private;

	pObj = multicorn_get_instance(rel);
	pZero = Py_BuildValue("d");

	pMethod = PyObject_GetAttrString(pObj, "get_path_keys");
	pPathKeysList = PyObject_CallObject(pMethod, Py_BuildValue("()"));
	multicorn_error_check();
	/* Add values for restrictinfo. */
	for (i = 0; i < PyList_Size(pPathKeysList); i++)
	{
		pPathKeytuple = PyList_GetItem(pPathKeysList, i);
		pPathKeyAttrName = PyTuple_GetItem(pPathKeytuple, 0);
		pPathKeyCost = PyTuple_GetItem(pPathKeytuple, 1);
		PyNumber_Coerce(&pPathKeyCost, &pZero);
		parampathrows = PyFloat_AsDouble(pPathKeyCost);
		multicorn_error_check();
		/* Add values for equivalence classes */
		foreach(eq_class_cell, root->eq_classes)
		{
			eq_class = (EquivalenceClass *) lfirst(eq_class_cell);
			foreach(ri_cell, eq_class->ec_sources)
			{
				restrictinfo = (RestrictInfo *) lfirst(ri_cell);
				pQual = multicorn_is_filter_on_column(restrictinfo, rel, pPathKeyAttrName);
				if (pQual)
				{
					multicorn_append_path_from_restrictinfo(root, baserel, restrictinfo, parampathrows, eq_class->ec_relids);
					multicorn_error_check();
				}
			}
		}
	}
	RelationClose(rel);
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 baserel->baserestrictcost.startup,
									 baserel->rows * baserel->width,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 (void *) state));

}

/*
 * multicornGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
multicornGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses)
{
	Index		scan_relid = baserel->relid;
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							baserel->fdw_private);
}


static void
multicornExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
}

static void
multicornBeginForeignScan(ForeignScanState *node, int eflags)
{
    PyObject * dummyQuals;
	AttInMetadata *attinmeta;
	MulticornExecState *state;
	MulticornPlanState *plan_state;
	Relation	rel = node->ss.ss_currentRelation;
	ListCell   *lc;

	attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
	state = (MulticornExecState *) palloc(sizeof(MulticornExecState));
	state->rownum = 0;
	state->attinmeta = attinmeta;
	state->pIterator = NULL;
	plan_state = (MulticornPlanState *) ((ForeignScan *) node->ss.ps.plan)->fdw_private;
	state->planstate = plan_state;
	node->fdw_state = (void *) state;
    dummyQuals = PyList_New(0);
	if (!bms_is_empty(node->ss.ps.plan->extParam))
	{
		/* If we still have pending external params,  */
		/* look for them in the qual list.  */
        /* We use dummy quals since we do not want to parse quals two times */
		foreach(lc, node->ss.ps.plan->qual)
		{
			multicorn_extract_condition((Expr *) lfirst(lc), dummyQuals,
										state->planstate->params, rel);
		}
	}
}

PyObject *
multicorn_get_quals(ForeignScanState *node, Relation rel, MulticornExecState * state)
{
	PyObject   *pConds,
			   *pParams;
	ParamListInfo params = node->ss.ps.state->es_param_list_info;
	ParamExecData *exec_params = node->ss.ps.state->es_param_exec_vals;

	/* Fill the params with the concrete values. */
	if (PyList_Size(state->planstate->params) > 0)
	{
		Py_ssize_t	i,
					valueIndex,
					attrIndex,
					length;
		PyObject   *pCurrentParam,
				   *pValue,
				   *pOperator,
				   *pKey,
				   *qual_class;
		Datum		value;

		qual_class = multicorn_get_class("multicorn.Qual");
		pParams = state->planstate->params;
		/* Copy the list of quals */
		length = PyList_Size(state->planstate->quals);
		pConds = PyList_New(0);
		for (i = 0; i < length; i++)
		{
			PyList_Append(pConds, PyList_GetItem(state->planstate->quals, i));
		}
		length = PyList_Size(state->planstate->params);
		for (i = 0; i < length; i++)
		{
			pCurrentParam = PyList_GetItem(pParams, i);
			valueIndex = PyInt_AsSsize_t(PyObject_GetAttrString(pCurrentParam, "param_id"));
			attrIndex = PyInt_AsSsize_t(PyObject_GetAttrString(pCurrentParam, "att_num"));
			pKey = PyObject_GetAttrString(pCurrentParam, "field_name");
			pOperator = PyObject_GetAttrString(pCurrentParam, "operator");
			switch (PyInt_AsSsize_t(PyObject_GetAttrString(pCurrentParam, "param_kind")))
			{
				case MulticornPARAM_EXTERN:
					if (params->params[valueIndex - 1].isnull)
					{
						value = -1;
					}
					else
					{
						value = params->params[valueIndex - 1].value;
					}
					break;
				case MulticornPARAM_EXEC:
					value = exec_params[valueIndex].value;
					break;
				default:
					continue;
			}
			multicorn_error_check();
			pValue = multicorn_datum_to_python(value,
											   PyInt_AsSsize_t(PyObject_GetAttrString(pCurrentParam, "param_type")),
											   rel->rd_att->attrs[attrIndex]);
			PyList_Append(pConds, PyObject_CallObject(qual_class, Py_BuildValue("(O,O,O)", pKey, pOperator, pValue)));
			multicorn_error_check();
		}
	}
	else
	{
		/* Reuse "quals" directly */
		pConds = state->planstate->quals;
	}

	return pConds;
}

void
multicorn_execute(ForeignScanState *node)
{
	PyObject   *pArgs,
			   *pValue,
			   *pObj,
			   *pMethod,
			   *pConds,
			   *colList;

	MulticornExecState *state = node->fdw_state;
	Relation	rel = node->ss.ss_currentRelation;

	/* Get the python FDW instance */
	pObj = multicorn_get_instance(rel);
	Py_INCREF(pObj);
	pConds = multicorn_get_quals(node, rel, state);
	pArgs = PyTuple_New(2);
	colList = state->planstate->needed_columns;
	Py_INCREF(pConds);
	Py_INCREF(colList);
	PyTuple_SetItem(pArgs, 0, pConds);
	PyTuple_SetItem(pArgs, 1, colList);
	multicorn_error_check();
	pMethod = PyObject_GetAttrString(pObj, "execute");
	pValue = PyObject_CallObject(pMethod, pArgs);
	Py_DECREF(pMethod);
	Py_DECREF(pArgs);
	multicorn_error_check();
	state->pIterator = PyObject_GetIter(pValue);
	multicorn_error_check();
	Py_DECREF(pValue);
	Py_DECREF(pObj);
}


static TupleTableSlot *
multicornIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	MulticornExecState *state = (MulticornExecState *) node->fdw_state;
	HeapTuple	tuple;
	PyObject   *pValue,
			   *pIterator,
			   *pyStopIteration,
			   *pErrType,
			   *pErrValue,
			   *pErrTraceback;

	if (state->pIterator == NULL)
	{
		multicorn_execute(node);
	}
	ExecClearTuple(slot);
	pIterator = state->pIterator;
	pValue = PyIter_Next(pIterator);
	PyErr_Fetch(&pErrType, &pErrValue, &pErrTraceback);
	if (pErrType)
	{
		pyStopIteration = PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("exceptions")),
												 "StopIteration");
		if (PyErr_GivenExceptionMatches(pErrType, pyStopIteration))
		{
			/* "Normal" stop iteration */
			return slot;
		}
		else
		{
			multicorn_report_exception(pErrType, pErrValue, pErrTraceback);
		}
	}
	if (pValue == NULL || pValue == Py_None)
	{
		return slot;
	}
	if (PyMapping_Check(pValue))
	{
		tuple = pydict_to_postgres_tuple(node->ss.ss_currentRelation->rd_att, pValue);
	}
	else
	{
		if (PySequence_Check(pValue))
		{
			tuple = pysequence_to_postgres_tuple(node->ss.ss_currentRelation->rd_att, pValue);
		}
		else
		{
			elog(ERROR, "Cannot transform anything else than mappings and sequences to rows");
			return slot;
		}
	}
	Py_DECREF(pValue);
	ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	state->rownum++;
	return slot;
}

static void
multicornReScanForeignScan(ForeignScanState *node)
{
	multicorn_clean_state((MulticornExecState *) node->fdw_state);
}

static void
multicornEndForeignScan(ForeignScanState *node)
{
	MulticornExecState *state = node->fdw_state;

	multicorn_clean_state(state);
	Py_DECREF(state->planstate->needed_columns);
	Py_DECREF(state->planstate->quals);
	Py_DECREF(state->planstate->params);
	state->planstate->needed_columns = NULL;
	state->planstate->quals = NULL;
	state->planstate->params = NULL;
}

void
multicorn_error_check()
{
	PyObject   *pErrType,
			   *pErrValue,
			   *pErrTraceback;

	PyErr_Fetch(&pErrType, &pErrValue, &pErrTraceback);
	if (pErrType)
	{
		multicorn_report_exception(pErrType, pErrValue, pErrTraceback);
	}
}

void
multicorn_get_options(Oid foreigntableid, PyObject *pOptions, char **module)
{
	ForeignTable *f_table;
	ForeignServer *f_server;
	UserMapping *mapping;
	List	   *options;
	ListCell   *lc;
	bool		got_module = false;
	PyObject   *pStr;

	f_table = GetForeignTable(foreigntableid);
	f_server = GetForeignServer(f_table->serverid);

	options = NIL;
	options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);
	/* An error might occur if no user mapping is defined. */
	/* In that case, just ignore it */
	PG_TRY();
	{
		mapping = GetUserMapping(GetUserId(), f_table->serverid);
		options = list_concat(options, mapping->options);
	}
	PG_CATCH();
	{
		FlushErrorState();
		/* DO NOTHING HERE */
	}
	PG_END_TRY();

	foreach(lc, options)
	{

		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "wrapper") == 0)
		{
			*module = (char *) defGetString(def);
			got_module = true;
		}
		else
		{
			pStr = PyString_FromString((char *) defGetString(def));
			PyDict_SetItemString(pOptions, def->defname, pStr);
			Py_DECREF(pStr);
		}
	}
	if (!got_module)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("wrapper option not found"),
				 errhint("You must set wrapper option to a ForeignDataWrapper python class, for example multicorn.csv.CsvFdw")));
	}
}


HeapTuple
pydict_to_postgres_tuple(TupleDesc desc, PyObject *pydict)
{
	HeapTuple	tuple;
	AttInMetadata *attinmeta = TupleDescGetAttInMetadata(desc);
	PyObject   *pStr;
	char	   *key;
	char	  **tup_values;
	int			i,
				natts;
	ssize_t    *sizes;
	char	   *buffer;

	natts = desc->natts;
	tup_values = (char **) palloc(sizeof(char *) * natts);
	sizes = (ssize_t *) palloc(sizeof(ssize_t) * natts);
	for (i = 0; i < natts; i++)
	{
		key = NameStr(desc->attrs[i]->attname);
		if (PyMapping_HasKeyString(pydict, key))
		{
			pStr = PyMapping_GetItemString(pydict, key);
			multicorn_error_check();
			sizes[i] = pyobject_to_cstring(pStr, desc->attrs[i], &buffer);
			if (sizes[i] < 0)
			{
				tup_values[i] = NULL;
				sizes[i] = 0;
			}
			else
			{
				tup_values[i] = (char *) palloc(sizeof(char) * (sizes[i] + 1));
				memcpy(tup_values[i], buffer, sizes[i] + 1);
			}
			multicorn_error_check();
			if (pStr != Py_None)
			{
				Py_DECREF(pStr);
			}
		}
		else
		{
			sizes[i] = 0;
			tup_values[i] = NULL;
		}
	}
	tuple = BuildTupleFromCStringsWithSize(attinmeta, tup_values, sizes);
	return tuple;
}

HeapTuple
pysequence_to_postgres_tuple(TupleDesc desc, PyObject *pyseq)
{
	HeapTuple	tuple;
	AttInMetadata *attinmeta = TupleDescGetAttInMetadata(desc);
	char	  **tup_values;
	int			i,
				natts;
	PyObject   *pStr;
	ssize_t    *sizes;
	char	   *buffer;

	natts = desc->natts;
	sizes = (ssize_t *) palloc(sizeof(ssize_t) * natts);
	if (PySequence_Size(pyseq) != natts)
	{
		elog(ERROR, "The python backend did not return a valid sequence");
		return NULL;
	}
	else
	{
		tup_values = (char **) palloc(sizeof(char *) * natts);
		for (i = 0; i < natts; i++)
		{
			pStr = PySequence_GetItem(pyseq, i);
			multicorn_error_check();
			sizes[i] = pyobject_to_cstring(pStr, desc->attrs[i], &buffer);
			if (sizes[i] < 0)
			{
				tup_values[i] = NULL;
				sizes[i] = 0;
			}
			else
			{
				tup_values[i] = (char *) palloc(sizeof(char) * (sizes[i] + 1));
				memcpy(tup_values[i], buffer, sizes[i] + 1);
			}
			multicorn_error_check();
			Py_DECREF(pStr);
		}
		tuple = BuildTupleFromCStringsWithSize(attinmeta, tup_values, sizes);
		return tuple;
	}
}

const char *
get_encoding_from_attribute(Form_pg_attribute attribute)
{
	HeapTuple	tp;
	Form_pg_collation colltup;
	const char *encoding_name;

	tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(attribute->attcollation));
	if (!HeapTupleIsValid(tp))
		return "ascii";
	colltup = (Form_pg_collation) GETSTRUCT(tp);
	ReleaseSysCache(tp);
	if (colltup->collencoding == -1)
	{
		/* No encoding information, do stupid things */
		encoding_name = GetDatabaseEncodingName();
	}
	else
	{
		encoding_name = (char *) pg_encoding_to_char(colltup->collencoding);
	}
	if (strcmp(encoding_name, "SQL_ASCII") == 0)
	{
		encoding_name = "ascii";
	}
	return encoding_name;
}

ssize_t
pyobject_to_cstring(PyObject *pyobject, Form_pg_attribute attribute, char **buffer)
{
	PyObject   *date_module = PyImport_Import(PyUnicode_FromString("datetime"));
	PyObject   *date_cls = PyObject_GetAttrString(date_module, "date");
	PyObject   *pStr;
	Py_ssize_t	strlength = 0;


	if (PyNumber_Check(pyobject))
	{
		PyString_AsStringAndSize(PyObject_Str(pyobject), buffer, &strlength);
		return strlength;
	}
	if (pyobject == NULL || pyobject == Py_None)
	{
		return -1;
	}
	if (PyUnicode_Check(pyobject))
	{
		Py_ssize_t	unicode_size;
		const char *encoding_name = get_encoding_from_attribute(attribute);

		unicode_size = PyUnicode_GET_SIZE(pyobject);
		if (!encoding_name)
		{
			PyString_AsStringAndSize(pyobject, buffer, &strlength);
		}
		else
		{
			PyString_AsStringAndSize(PyUnicode_Encode(PyUnicode_AsUnicode(pyobject), unicode_size,
								   encoding_name, NULL), buffer, &strlength);
		}
		multicorn_error_check();
		return strlength;
	}
	if (PyString_Check(pyobject))
	{
		if (PyString_AsStringAndSize(pyobject, buffer, &strlength) < 0)
		{
			ereport(WARNING,
					(errmsg("An error occured while decoding the %s column", NameStr(attribute->attname)),
					 errhint("Hou should maybe return unicode instead?")));
		}
		multicorn_error_check();
		return strlength;
	}
	if (PyObject_IsInstance(pyobject, date_cls))
	{
		PyObject   *formatted_date;

		formatted_date = PyObject_CallMethod(pyobject, "isoformat", "()");
		multicorn_error_check();
		PyString_AsStringAndSize(formatted_date, buffer, &strlength);
		multicorn_error_check();
		return strlength;
	}
	if (PySequence_Check(pyobject))
	{
		/* Its an array */
		Py_ssize_t	i;
		Py_ssize_t	size = PySequence_Size(pyobject);
		char	   *tempbuffer;
		PyObject   *delimiter = PyString_FromString(", ");
		PyObject   *mapped_list = PyList_New(0);

		for (i = 0; i < size; i++)
		{
			pyobject_to_cstring(PySequence_GetItem(pyobject, i), attribute, &tempbuffer);
			PyList_Append(mapped_list, PyString_FromString(tempbuffer));
		}
		pStr = PyString_Format(PyString_FromString("{%s}"), PyObject_CallMethod(delimiter, "join", "(O)", mapped_list));
		multicorn_error_check();
		PyString_AsStringAndSize(pStr, buffer, &strlength);
		Py_DECREF(mapped_list);
		Py_DECREF(delimiter);
		Py_DECREF(pStr);
		return strlength;
	}
	if (PyMapping_Check(pyobject))
	{
		char	   *keybuffer;
		char	   *valuebuffer;
		PyObject   *mapped_list = PyList_New(0);
		PyObject   *items = PyMapping_Items(pyobject);
		PyObject   *delimiter = PyString_FromString(", ");
		PyObject   *current_tuple;
		Py_ssize_t	i;
		Py_ssize_t	size = PyList_Size(items);

		for (i = 0; i < size; i++)
		{
			current_tuple = PySequence_GetItem(items, i);
			pyobject_to_cstring(PyTuple_GetItem(current_tuple, 0), attribute, &keybuffer);
			pyobject_to_cstring(PyTuple_GetItem(current_tuple, 1), attribute, &valuebuffer);
			PyList_Append(mapped_list, PyString_FromFormat("%s=>%s", keybuffer, valuebuffer));
		}
		PyString_AsStringAndSize(PyObject_CallMethod(delimiter, "join", "(O)", mapped_list), buffer, &strlength);
		return strlength;
	}
	Py_DECREF(date_module);
	Py_DECREF(date_cls);
	PyString_AsStringAndSize(PyObject_Str(pyobject), buffer, &strlength);
	return strlength;
}

/*	Appends the columns names as python strings to the given python list */
void
multicorn_get_attributes_def(TupleDesc desc, PyObject *dict)
{
	char	   *key,
			   *typname;
	int			typOid;
	PyObject   *column_class,
			   *column_instance;
	Py_ssize_t	i,
				natts;

	natts = desc->natts;
	column_class = multicorn_get_class("multicorn.ColumnDefinition");
	for (i = 0; i < natts; i++)
	{
		typOid = desc->attrs[i]->atttypid;
		typname = format_type_be(typOid);
		key = NameStr(desc->attrs[i]->attname);
		column_instance = PyObject_CallObject(column_class, Py_BuildValue("(s,i,s)", key, typOid, typname));
		PyMapping_SetItemString(dict, key, column_instance);
	}
}

void
multicorn_unnest(Node *node, Node **result)
{
	switch (node->type)
	{
		case T_RelabelType:
			*result = (Node *) ((RelabelType *) node)->arg;
			break;
		case T_ArrayCoerceExpr:
			*result = (Node *) ((ArrayCoerceExpr *) node)->arg;
			break;
		default:
			*result = node;
	}
}

MulticornParamType
multicorn_extract_qual(Node *left, Node *right, Relation baserel, Form_pg_operator operator, PyObject **result)
{
	HeapTuple	tp;
	Form_pg_attribute attr,
				attr_right;
	char	   *key;
	TupleDesc	tupdesc = baserel->rd_att;
	PyObject   *value = NULL;
	PyObject   *qual_class = multicorn_get_class("multicorn.Qual");
	PyObject   *param_class = multicorn_get_class("multicorn.Param");
	MulticornParamType param_type;
	Node	   *normalized_left;
	Node	   *normalized_right;
	Param	   *param;

	multicorn_error_check();
	multicorn_unnest(left, &normalized_left);
	multicorn_unnest(right, &normalized_right);
	if (IsA(normalized_right, Var) &&!IsA(normalized_left, Var))
	{
		/* Switch the operands if possible */
		/* Lookup the inverse operator (eg, <= for > and so on) */
		tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(operator->oprcom));
		if (HeapTupleIsValid(tp))
		{
			Node	   *temp = normalized_left;

			normalized_left = normalized_right;
			normalized_right = temp;
			operator = (Form_pg_operator) GETSTRUCT(tp);
			ReleaseSysCache(tp);
		}
	}
	if (IsA(normalized_left, Var))
	{
		attr = tupdesc->attrs[((Var *) normalized_left)->varattno - 1];
		key = NameStr(attr->attname);
		multicorn_error_check();
		switch (normalized_right->type)
		{
			case T_Const:
				value = multicorn_datum_to_python(((Const *) normalized_right)->constvalue, ((Const *) normalized_right)->consttype, attr);
				*result = PyObject_CallObject(qual_class, Py_BuildValue("(s,s,O)", key, NameStr(operator->oprname), value));
				multicorn_error_check();
				return MulticornQUAL;
				break;
			case T_Param:
				param = (Param *) normalized_right;
				switch (param->paramkind)
				{
					case PARAM_EXTERN:
						param_type = MulticornPARAM_EXTERN;
						break;
					case PARAM_EXEC:
						param_type = MulticornPARAM_EXEC;
						break;
					default:
						return MulticornUNKNOWN;
				}
				/* Build the param with key, operator, paramid, attno */
				*result = PyObject_CallObject(param_class,
											  Py_BuildValue("(s,s,i,i,i,i)", key, NameStr(operator->oprname),
															param->paramid,
									 ((Var *) normalized_left)->varattno - 1,
															param_type,
														  param->paramtype));
				multicorn_error_check();
				return param_type;
			case T_Var:
				attr_right = tupdesc->attrs[((Var *) normalized_right)->varattno - 1];
				/* Check that the attributes are from the same rel. */
				if (attr->attrelid == attr_right->attrelid)
				{
					*result = PyObject_CallObject(qual_class, Py_BuildValue("(s,s,s)", key, NameStr(operator->oprname), NameStr(attr_right->attname)));
					return MulticornVAR;
				}
				return MulticornUNKNOWN;
			default:
				elog(WARNING, "Cant manage type %i", normalized_right->type);
				break;
		}
	}
	return MulticornUNKNOWN;
}

void
multicorn_extract_condition(Expr *clause, PyObject *qual_list, PyObject *param_list, Relation baserel)
{
	PyObject   *tempqual = NULL;
	HeapTuple	tp;
	Node	   *left,
			   *right;
	Form_pg_operator operator_tup;
	MulticornParamType paramtype;

	if (clause != NULL)
	{
		switch (clause->type)
		{
			case T_OpExpr:
				if (list_length(((OpExpr *) clause)->args) == 2)
				{
					OpExpr	   *op = (OpExpr *) clause;

					left = list_nth(op->args, 0);
					right = list_nth(op->args, 1);
					tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
					if (!HeapTupleIsValid(tp))
						elog(ERROR, "cache lookup failed for operator %u", op->opno);
					operator_tup = (Form_pg_operator) GETSTRUCT(tp);
					ReleaseSysCache(tp);
					paramtype = multicorn_extract_qual(left, right, baserel, operator_tup, &tempqual);
					switch (paramtype)
					{
						case MulticornQUAL:
							PyList_Append(qual_list, tempqual);
							break;
						case MulticornPARAM_EXTERN:
						case MulticornPARAM_EXEC:
							PyList_Append(param_list, tempqual);
							break;
							/* MulticornVAR ignored here, it should be */
							/* converted to a param extern or param_exec after */
							/* a path has been chosen. */
						default:
							break;
					}
				}
				break;
			case T_ScalarArrayOpExpr:
				if (list_length(((ScalarArrayOpExpr *) clause)->args) == 2)
				{
					ScalarArrayOpExpr *op = (ScalarArrayOpExpr *) clause;

					left = list_nth(op->args, 0);
					right = list_nth(op->args, 1);
					tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
					if (!HeapTupleIsValid(tp))
						elog(ERROR, "cache lookup failed for operator %u", op->opno);
					operator_tup = (Form_pg_operator) GETSTRUCT(tp);
					ReleaseSysCache(tp);

					/*
					 * Build the qual "normally" and set the operator to a
					 * tuple instead
					 */
					paramtype = multicorn_extract_qual(left, right, baserel, operator_tup, &tempqual);

					if (tempqual)
					{
						multicorn_unnest(left, &left);
						if (IsA(left, Var))
						{
							/*
							 * Don't add it to the list if the form is
							 * constant = ANY(column)
							 */
							if (op->useOr)
							{
								/* ANY clause on the array + "=" -> IN */
								PyObject_SetAttrString(tempqual, "operator", Py_BuildValue("(O,O)",
																						   PyObject_GetAttrString(tempqual, "operator"), Py_True));
							}
							else
							{
								PyObject_SetAttrString(tempqual, "operator", Py_BuildValue("(O,O)",
																						   PyObject_GetAttrString(tempqual, "operator"), Py_False));
							}
						}
					}
					switch (paramtype)
					{
						case MulticornQUAL:
							PyList_Append(qual_list, tempqual);
							break;
						case MulticornPARAM_EXEC:
						case MulticornPARAM_EXTERN:
							PyList_Append(param_list, tempqual);
							break;
						default:
							break;
					}
				}
				break;
			case T_NullTest:
				/* TODO: this code is pretty much duplicated from */
				/* get_param, find a way to share it. */
				if IsA
					(((NullTest *) clause)->arg, Var)
				{
					char	   *operator_name;
					NullTest   *nulltest = (NullTest *) clause;
					TupleDesc	tupdesc = baserel->rd_att;
					PyObject   *qual_class = multicorn_get_class("multicorn.Qual");
					Form_pg_attribute attr = tupdesc->attrs[((Var *) nulltest->arg)->varattno - 1];

					if (nulltest->nulltesttype == IS_NULL)
					{
						operator_name = "=";
					}
					else
					{
						operator_name = "<>";
					}
					PyList_Append(qual_list, PyObject_CallObject(qual_class,
																 Py_BuildValue("(s,s,O)", NameStr(attr->attname), operator_name, Py_None)));
				}
				break;
			default:
				elog(WARNING, "GOT AN UNEXPECTED TYPE: %i", clause->type);
				break;
		}
	}
}

PyObject *
multicorn_datum_to_python(Datum datumvalue, Oid type, Form_pg_attribute attribute)
{
	PyObject   *result;
	HeapTuple	typeTuple;
	Form_pg_type typeStruct;
	const char *encoding_name;
	char	   *tempvalue;
	long		number;
	fsec_t		fsec;
	struct pg_tm *pg_tm_value;
	Py_ssize_t	size;

	if (datumvalue == -1)
	{
		return Py_None;
	}
	switch (type)
	{
		case TEXTOID:
		case VARCHAROID:
			/* Its a string */
			tempvalue = TextDatumGetCString(datumvalue);
			size = strlen(tempvalue);
			encoding_name = get_encoding_from_attribute(attribute);
			if (!encoding_name)
			{
				result = PyString_FromString(tempvalue);
				multicorn_error_check();
			}
			else
			{
				result = PyUnicode_Decode(tempvalue, size, encoding_name, NULL);
				multicorn_error_check();
			}
			break;
		case NUMERICOID:
			/* Its a numeric */
			tempvalue = (char *) DirectFunctionCall1(numeric_out, (ssize_t) DatumGetNumeric(datumvalue));
			result = PyFloat_FromString(PyString_FromString(tempvalue), NULL);
			break;
		case INT4OID:
			number = DatumGetInt32(datumvalue);
			result = PyInt_FromLong(number);
			break;
		case DATEOID:
			datumvalue = DirectFunctionCall1(date_timestamp, datumvalue);
			pg_tm_value = palloc(sizeof(struct pg_tm));
			timestamp2tm(DatumGetTimestamp(datumvalue), NULL, pg_tm_value, &fsec, NULL, NULL);
			result = PyDate_FromDate(pg_tm_value->tm_year,
								  pg_tm_value->tm_mon, pg_tm_value->tm_mday);
			break;
		case TIMESTAMPOID:
			/* TODO: see what could go wrong */
			pg_tm_value = palloc(sizeof(struct pg_tm));
			timestamp2tm(DatumGetTimestamp(datumvalue), NULL, pg_tm_value, &fsec, NULL, NULL);
			result = PyDateTime_FromDateAndTime(pg_tm_value->tm_year,
								   pg_tm_value->tm_mon, pg_tm_value->tm_mday,
												pg_tm_value->tm_hour, pg_tm_value->tm_min, pg_tm_value->tm_sec, 0);
			break;
		default:
			/* Try to manage array types */
			typeTuple = SearchSysCache1(TYPEOID,
										ObjectIdGetDatum(type));
			if (!HeapTupleIsValid(typeTuple))
				elog(ERROR, "lookup failed for type %u",
					 type);
			typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);
			ReleaseSysCache(typeTuple);
			if ((typeStruct->typelem != 0) && (typeStruct->typlen == -1))
			{
				/* Its an array */
				Datum		buffer;
				PyObject   *listelem;
				ArrayIterator iterator;
				bool		isnull;

				result = PyList_New(0);
				iterator = array_create_iterator(DatumGetArrayTypeP(datumvalue), 0);
				while (array_iterate(iterator, &buffer, &isnull))
				{
					if (isnull)
					{
						PyList_Append(result, Py_None);
					}
					else
					{
						listelem = multicorn_datum_to_python(buffer, typeStruct->typelem, attribute);
						if (listelem == NULL)
						{
							result = NULL;
							break;
						}
						else
						{
							PyList_Append(result, listelem);
						}
					}
				}
				array_free_iterator(iterator);
			}
			else
			{
				/* NOT AN ARRAY, nor a known type, not managed */
				return NULL;
			}
	}
	return result;
}

void
multicorn_report_exception(PyObject *pErrType, PyObject *pErrValue, PyObject *pErrTraceback)
{
	char	   *errName,
			   *errValue,
			   *errTraceback = "";
	PyObject   *traceback_list;
	PyObject   *tracebackModule = PyImport_Import(PyString_FromString("traceback"));
	PyObject   *format_exception = PyObject_GetAttrString(tracebackModule, "format_exception");
	PyObject   *newline = PyString_FromString("\n");

	PyErr_NormalizeException(&pErrType, &pErrValue, &pErrTraceback);
	errName = PyString_AsString(PyObject_GetAttrString(pErrType, "__name__"));
	errValue = PyString_AsString(PyObject_Str(pErrValue));
	if (pErrTraceback)
	{
		traceback_list = PyObject_CallObject(format_exception, Py_BuildValue("(O,O,O)", pErrType, pErrValue, pErrTraceback));
		errTraceback = PyString_AsString(PyObject_CallObject(PyObject_GetAttrString(newline, "join"), Py_BuildValue("(O)", traceback_list)));
	}
	ereport(ERROR, (errmsg("Error in python: %s", errName),
					errdetail("%s", errValue),
					errdetail_log("%s", errTraceback)));
}

void
_PG_init()
{
	/* TODO: managed locks and things */
	Py_Initialize();
	PyDateTime_IMPORT;
	TABLES_DICT = PyDict_New();
}

PyObject *
multicorn_get_multicorn()
{
	return PyImport_Import(PyUnicode_FromString("multicorn"));
}

PyObject *
multicorn_get_class(char *classname)
{
	return PyObject_CallObject(PyObject_GetAttrString(multicorn_get_multicorn(),
													  "get_class"),
							   Py_BuildValue("(s)", classname));
}

PyObject *
multicorn_get_instance(Relation rel)
{
	PyObject   *pOptions,
			   *pClass,
			   *pObj,
			   *pColumns,
			   *pTableId,
			   *pOrderedDictClass;
	Oid			tablerelid;
	char	   *module;

	tablerelid = RelationGetRelid(rel);
	pTableId = PyInt_FromSsize_t(tablerelid);
	multicorn_error_check();
	if (PyMapping_HasKey(TABLES_DICT, pTableId))
	{
		pObj = PyDict_GetItem(TABLES_DICT, pTableId);
	}
	else
	{
		pOptions = PyDict_New();
		multicorn_get_options(tablerelid, pOptions, &module);
		pClass = multicorn_get_class(module);
		multicorn_error_check();
		pOrderedDictClass = multicorn_get_class("multicorn.ordered_dict.OrderedDict");
		pColumns = PyObject_CallObject(pOrderedDictClass, PyTuple_New(0));
		multicorn_get_attributes_def(rel->rd_att, pColumns);
		pObj = PyObject_CallObject(pClass, Py_BuildValue("(O,O)", pOptions, pColumns));
		multicorn_error_check();
		PyDict_SetItem(TABLES_DICT, pTableId, pObj);
		multicorn_error_check();
		Py_DECREF(pOptions);
		Py_DECREF(pClass);
		Py_DECREF(pObj);
	}
	return pObj;
}


HeapTuple
BuildTupleFromCStringsWithSize(AttInMetadata *attinmeta, char **values, ssize_t *sizes)
{
	TupleDesc	tupdesc = attinmeta->tupdesc;
	int			natts = tupdesc->natts;
	Datum	   *dvalues;
	bool	   *nulls;
	int			i;
	HeapTuple	tuple,
				typeTuple;
	Oid			typeoid;
	Oid			element_type;

	dvalues = (Datum *) palloc(natts * sizeof(Datum));
	nulls = (bool *) palloc(natts * sizeof(bool));


	/* Call the "in" function for each non-dropped attribute */
	for (i = 0; i < natts; i++)
	{
		if (!tupdesc->attrs[i]->attisdropped)
		{
			/* Non-dropped attributes */

			typeoid = attinmeta->tupdesc->attrs[i]->atttypid;
			typeTuple = SearchSysCache1(TYPEOID,
										ObjectIdGetDatum(typeoid));
			if (!HeapTupleIsValid(typeTuple))
				elog(ERROR, "lookup failed for type %u",
					 typeoid);
			ReleaseSysCache(typeTuple);
			typeoid = HeapTupleGetOid(typeTuple);
			element_type = get_element_type(typeoid);
			typeoid = getBaseType(element_type ? element_type : typeoid);
			if (typeoid == BYTEAOID || typeoid == TEXTOID)
			{
				dvalues[i] = PointerGetDatum(cstring_to_text_with_len(values[i], sizes[i]));
				SET_VARSIZE(dvalues[i], sizes[i] + VARHDRSZ);
			}
			else
			{
				dvalues[i] = InputFunctionCall(&attinmeta->attinfuncs[i],
											   values[i],
											   attinmeta->attioparams[i],
											   attinmeta->atttypmods[i]);
			}
			if (values[i] != NULL)
				nulls[i] = false;
			else
				nulls[i] = true;
		}
		else
		{
			/* Handle dropped attributes by setting to NULL */
			dvalues[i] = (Datum) 0;
			nulls[i] = true;
		}
	}

	/*
	 * Form a tuple
	 */
	tuple = heap_form_tuple(tupdesc, dvalues, nulls);

	/*
	 * Release locally palloc'd space.  XXX would probably be good to pfree
	 * values of pass-by-reference datums, as well.
	 */
	pfree(dvalues);
	pfree(nulls);

	return tuple;
}

void
multicorn_get_column(Expr *expr, TupleDesc desc, PyObject *list)
{
	ListCell   *cell;
	char	   *key;

	if (expr == NULL)
	{
		return;
	}
	switch (expr->type)
	{
		case T_Var:
			key = NameStr(desc->attrs[((Var *) expr)->varattno - 1]->attname);
			if (key != NULL)
			{
				PySet_Add(list, PyString_FromString(key));
			}
			break;

		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_FuncExpr:
			foreach(cell, ((OpExpr *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);

			}
			break;


		case T_ScalarArrayOpExpr:
			foreach(cell, ((ScalarArrayOpExpr *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_Const:
		case T_Param:
		case T_CaseTestExpr:
		case T_CoerceToDomainValue:
		case T_CurrentOfExpr:
			break;

		case T_TargetEntry:
			if (((TargetEntry *) expr)->resorigcol == 0)
			{
				multicorn_get_column(((TargetEntry *) expr)->expr, desc, list);
			}
			else
			{
				key = NameStr(desc->attrs[((TargetEntry *) expr)->resorigcol - 1]->attname);
				if (key != NULL)
				{
					PySet_Add(list, PyString_FromString(key));
				}
			}
			break;

		case T_RelabelType:
			multicorn_get_column(((RelabelType *) expr)->arg, desc, list);
			break;

		case T_RestrictInfo:
			multicorn_get_column(((RestrictInfo *) expr)->clause, desc, list);
			break;

		case T_Aggref:
			foreach(cell, ((Aggref *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			foreach(cell, ((Aggref *) expr)->aggorder)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			foreach(cell, ((Aggref *) expr)->aggdistinct)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_WindowFunc:
			foreach(cell, ((WindowFunc *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_ArrayRef:
			foreach(cell, ((ArrayRef *) expr)->refupperindexpr)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			foreach(cell, ((ArrayRef *) expr)->reflowerindexpr)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			multicorn_get_column(((ArrayRef *) expr)->refexpr, desc, list);
			multicorn_get_column(((ArrayRef *) expr)->refassgnexpr, desc, list);
			break;

		case T_BoolExpr:
			foreach(cell, ((BoolExpr *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_SubPlan:
			foreach(cell, ((SubPlan *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_AlternativeSubPlan:
			/* Do not really know what to do */
			break;

		case T_NamedArgExpr:
			multicorn_get_column(((NamedArgExpr *) expr)->arg, desc, list);
			break;

		case T_FieldSelect:
			multicorn_get_column(((FieldSelect *) expr)->arg, desc, list);
			break;

		case T_CoerceViaIO:
			multicorn_get_column(((CoerceViaIO *) expr)->arg, desc, list);
			break;

		case T_ArrayCoerceExpr:
			multicorn_get_column(((ArrayCoerceExpr *) expr)->arg, desc, list);
			break;

		case T_ConvertRowtypeExpr:
			multicorn_get_column(((ConvertRowtypeExpr *) expr)->arg, desc, list);
			break;

		case T_CollateExpr:
			multicorn_get_column(((CollateExpr *) expr)->arg, desc, list);
			break;

		case T_CaseExpr:
			foreach(cell, ((CaseExpr *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			multicorn_get_column(((CaseExpr *) expr)->arg, desc, list);
			multicorn_get_column(((CaseExpr *) expr)->defresult, desc, list);
			break;

		case T_CaseWhen:
			multicorn_get_column(((CaseWhen *) expr)->expr, desc, list);
			multicorn_get_column(((CaseWhen *) expr)->result, desc, list);
			break;

		case T_ArrayExpr:
			foreach(cell, ((ArrayExpr *) expr)->elements)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_RowExpr:
			foreach(cell, ((RowExpr *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_RowCompareExpr:
			foreach(cell, ((RowCompareExpr *) expr)->largs)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			foreach(cell, ((RowCompareExpr *) expr)->rargs)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_CoalesceExpr:
			foreach(cell, ((CoalesceExpr *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_MinMaxExpr:
			foreach(cell, ((MinMaxExpr *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_XmlExpr:
			foreach(cell, ((XmlExpr *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			foreach(cell, ((XmlExpr *) expr)->named_args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_NullTest:
			multicorn_get_column(((NullTest *) expr)->arg, desc, list);
			break;

		case T_BooleanTest:
			multicorn_get_column(((BooleanTest *) expr)->arg, desc, list);
			break;

		case T_CoerceToDomain:
			multicorn_get_column(((CoerceToDomain *) expr)->arg, desc, list);
			break;

		default:
			ereport(ERROR,
					(errmsg("Unknown node type %d", expr->type)));
	}
}

void
multicorn_clean_state(MulticornExecState * state)
{
	if (state->pIterator)
	{
		Py_DECREF(state->pIterator);
		state->pIterator = NULL;
	}
}
