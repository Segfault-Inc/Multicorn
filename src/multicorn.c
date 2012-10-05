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
#include "lib/stringinfo.h"
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
#include "utils/memutils.h"
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
	Datum	   *dvalues;
	bool	   *nulls;
	Oid		   *typoids;
    StringInfo buffer;
    StringInfo keyBuffer;
    StringInfo valueBuffer;
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
MulticornPlanState *multicorn_init_plan_state(RelOptInfo *baserel, Oid foreigntableid);
void		multicorn_error_check(void);
void		_PG_init(void);
void		_PG_fini(void);
void		multicorn_get_options(Oid foreigntableid, PyObject *options_dict, char **module);
void		multicorn_get_attributes_def(TupleDesc desc, PyObject *dict);
PyObject   *multicorn_datum_to_python(Datum datumvalue, Oid type, Form_pg_attribute attribute);
void		multicorn_report_exception(PyObject *pErrType, PyObject *pErrValue, PyObject *pErrTraceback);
PyObject   *multicorn_get_instance(Relation rel);
void		multicorn_get_column(Expr *expr, TupleDesc desc, PyObject *list);
const char *get_encoding_from_attribute(Form_pg_attribute attribute);
void		multicorn_execute(ForeignScanState *node);
PyObject   *multicorn_get_quals(ForeignScanState *node, Relation rel, MulticornExecState * state);
void		multicorn_clean_state(MulticornExecState * state);
void		multicorn_extract_condition(Expr *clause, PyObject *qual_list, PyObject *param_list, Relation baserel, Relids accepted_relids);
MulticornParamType multicorn_extract_qual(Node *left, Node *right, Relation base_rel,
					   Relids base_relids,
					   Form_pg_operator operator, PyObject **result);
bool		multicorn_is_on_column(RestrictInfo *restrictinfo, Relation rel, PyObject *colname, Relids base_relids);
void multicorn_append_param_path(PlannerInfo *root, RelOptInfo *baserel,
				  Relids *relids, double parampathrows, List *restrictinfos);

PyObject   *multicorn_get_class(char *className);
PyObject   *multicorn_get_multicorn(void);
void		multicorn_unnest(Node *value, Node **result);

void		pysequence_to_postgres_tuple(MulticornExecState * state, TupleDesc desc, PyObject *pyseq);
void		pydict_to_postgres_tuple(MulticornExecState * state, TupleDesc desc, PyObject *pydict);
ssize_t		pyobject_to_cstring(PyObject *pyobject, Form_pg_attribute attribute, MulticornExecState *state);

PyObject   *TABLES_DICT,
		   *MULTICORN_PZERO,
		   *MULTICORN,
           *MULTICORN_DATE_MODULE,
           *MULTICORN_DATE_CLASS;


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
				Py_DECREF(pOptions);
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
			Py_DECREF(pStr);
		}
	}
	Py_DECREF(pOptions);
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
			   *pRowsAndWidth,
			   *pQualsAndParams,
			   *pBuffer;
	Py_ssize_t	i;
	MulticornPlanState *state;
	MemoryContext oldcontext = CurrentMemoryContext;
	MemoryContext statecontext;
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	Relation	rel = RelationIdGetRelation(ftable->relid);

	/* Switch to a temporary memory context to avoid leaks. */
	statecontext = AllocSetContextCreate(CurrentMemoryContext,
										 "multicorn temporary context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(statecontext);
	state = multicorn_init_plan_state(baserel, foreigntableid);
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
	pRowsAndWidth = PyObject_CallMethod(pObj, "get_rel_size", "(O,O)", pQualsAndParams, state->needed_columns);
	multicorn_error_check();
	pBuffer = PyTuple_GetItem(pRowsAndWidth, 0);
	PyNumber_Coerce(&pBuffer, &MULTICORN_PZERO);
	baserel->rows = PyFloat_AsDouble(pBuffer);
	Py_DECREF(pBuffer);
	pBuffer = PyTuple_GetItem(pRowsAndWidth, 1);
	PyNumber_Coerce(&pBuffer, &MULTICORN_PZERO);
	baserel->width = PyFloat_AsDouble(pBuffer);
	Py_DECREF(pBuffer);
	Py_DECREF(state->needed_columns);
	Py_DECREF(state->quals);
	Py_DECREF(state->params);
	pfree(state);
	Py_DECREF(pObj);
	Py_DECREF(pRowsAndWidth);
	Py_DECREF(pQualsAndParams);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(statecontext);
}

MulticornPlanState *
multicorn_init_plan_state(RelOptInfo *baserel, Oid foreigntableid)
{
	MulticornPlanState *state = (MulticornPlanState *) palloc(sizeof(MulticornPlanState));
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	Relation	rel = RelationIdGetRelation(ftable->relid);
	Expr	   *current_expr;
	ListCell   *cell;
	Relids		relids = bms_make_singleton(baserel->relid);

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
		multicorn_extract_condition(current_expr, state->quals, state->params, rel, relids);
	}
	RelationClose(rel);
	return state;
}

bool
multicorn_is_on_column(RestrictInfo *restrictinfo, Relation rel, PyObject *colname, Relids base_relids)
{
	Expr	   *clause;
	Expr	   *left;
	Expr	   *right;
	HeapTuple	tp;
	Form_pg_operator operator_tup;
	PyObject   *qual = NULL,
			   *pTemp = NULL;
	OpExpr	   *op;
	int			isEq;
	MulticornParamType param_type;

	clause = restrictinfo->clause;
	if (IsA(clause, OpExpr))
	{
		op = (OpExpr *) clause;
		left = list_nth(op->args, 0);
		right = list_nth(op->args, 1);
		tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
		operator_tup = (Form_pg_operator) GETSTRUCT(tp);
		ReleaseSysCache(tp);
		param_type = multicorn_extract_qual((Node *) left, (Node *) right, rel, base_relids, operator_tup, &qual);
		switch (param_type)
		{
			case MulticornPARAM_EXTERN:
			case MulticornPARAM_EXEC:
			case MulticornQUAL:
				pTemp = PyObject_GetAttrString(qual, "field_name");
				isEq = PyObject_Compare(colname, pTemp);
				Py_DECREF(pTemp);
				multicorn_error_check();
				return isEq == 0;
			case MulticornVAR:
				pTemp = PyObject_GetAttrString(qual, "field_name");
				isEq = PyObject_Compare(colname, pTemp);
				Py_DECREF(pTemp);
				multicorn_error_check();
				if (isEq == 0)
				{
					return true;
				}
				pTemp = PyObject_GetAttrString(qual, "value");
				isEq = PyObject_Compare(colname, pTemp);
				Py_DECREF(pTemp);
				multicorn_error_check();
				return isEq == 0;
			default:
				break;
		}
		if (qual != NULL)
		{
			Py_DECREF(qual);
		}
	}
	return false;
}

/*
 * Build a parameterized path using the given restrictinfo, and how much rows
 * should be available using this restrictinfo.
 */
void
multicorn_append_param_path(PlannerInfo *root, RelOptInfo *baserel,
				   Relids *relids, double parampathrows, List *restrictinfos)
{
	ParamPathInfo *ppi = makeNode(ParamPathInfo);
	Relids		otherrels = NULL;
	ForeignPath *foreignPath;

	if (relids)
	{
		otherrels = bms_difference((Bitmapset *) relids, bms_make_singleton(baserel->relid));
	}
	foreignPath = create_foreignscan_path(root, baserel,
										  parampathrows,
									  baserel->baserestrictcost.startup + 10,
										  parampathrows * baserel->width,
										  NIL,
										  NULL,
										  NULL);
	ppi->ppi_req_outer = otherrels;
	ppi->ppi_rows = parampathrows;
	ppi->ppi_clauses = list_concat(ppi->ppi_clauses, restrictinfos);
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
			   *pPathKeysList,
			   *pPathKeytuple,
			   *pPathKeys,
			   *pPathKeyAttrName,
			   *pPathKeyCost;
	Py_ssize_t	i,
				j;
	ListCell   *lc;
	ListCell   *ri_cell;
	RestrictInfo *restrictinfo;
	EquivalenceClass *eq_class;

	ForeignTable *ftable = GetForeignTable(foreigntableid);
	Relation	rel = RelationIdGetRelation(ftable->relid);
	double		parampathrows;
	int			found = 0;
	List	   *clauses = NULL;
	Relids	   *outerrels;

	pObj = multicorn_get_instance(rel);
	pPathKeysList = PyObject_CallMethod(pObj, "get_path_keys", "()");
	multicorn_error_check();
	/* For each column on which the FDW can filter, */
	/* see if it is used in an equivalence class built for a join. */
	/* If it is, then build a parmeterized path that the planner will */
	/* consider. */
	for (i = 0; i < PySequence_Length(pPathKeysList); i++)
	{
		pPathKeytuple = PySequence_GetItem(pPathKeysList, i);
		pPathKeys = PySequence_GetItem(pPathKeytuple, 0);
		pPathKeyCost = PySequence_GetItem(pPathKeytuple, 1);
		PyNumber_Coerce(&pPathKeyCost, &MULTICORN_PZERO);
		parampathrows = PyFloat_AsDouble(pPathKeyCost);
		multicorn_error_check();
		clauses = NULL;
		outerrels = NULL;
		for (j = 0; j < PySequence_Length(pPathKeys); j++)
		{
			pPathKeyAttrName = PySequence_GetItem(pPathKeys, j);
			found = 0;
			/* Walk the equivalence_classes list for inner joins. */
			foreach(lc, root->eq_classes)
			{
				eq_class = (EquivalenceClass *) lfirst(lc);

				/*
				 * If there is only one member, then the equivalence class is
				 * either for an outer join, or a desired sort order. So we
				 * better leave it untouched.
				 */
				if (eq_class->ec_members->length > 1)
				{
					foreach(ri_cell, eq_class->ec_sources)
					{
						restrictinfo = (RestrictInfo *) lfirst(ri_cell);
						if (multicorn_is_on_column(restrictinfo, rel, pPathKeyAttrName, bms_make_singleton(baserel->relid)))
						{
							clauses = list_append_unique(clauses, restrictinfo);
							outerrels = (Relids *) bms_union((Bitmapset *) outerrels, eq_class->ec_relids);
							found = 1;
						}
					}
				}
			}
			/* Walk the outer joins. */
			foreach(ri_cell, list_union(root->left_join_clauses, root->right_join_clauses))
			{
				restrictinfo = (RestrictInfo *) lfirst(ri_cell);
				if (multicorn_is_on_column(restrictinfo, rel, pPathKeyAttrName, bms_make_singleton(baserel->relid)))
				{
					clauses = list_append_unique(clauses, restrictinfo);
					outerrels = (Relids *) bms_union((Bitmapset *) outerrels, restrictinfo->outer_relids);
					found = 1;
				}
			}
			Py_DECREF(pPathKeyAttrName);
			if (!found)
			{
				/*
				 * If a single path key is not found, break and do not do
				 * anything
				 */
				break;
			}
		}
		if (found)
		{
			multicorn_append_param_path(root, baserel, outerrels, parampathrows, clauses);
			multicorn_error_check();
		}
		Py_DECREF(pPathKeytuple);
		Py_DECREF(pPathKeys);
		Py_DECREF(pPathKeyCost);
	}
	Py_DECREF(MULTICORN_PZERO);
	Py_DECREF(pPathKeysList);
	RelationClose(rel);
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 baserel->baserestrictcost.startup,
									 baserel->rows * baserel->width,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 (void *) NULL));

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
	MulticornPlanState *state = multicorn_init_plan_state(baserel, foreigntableid);

	scan_clauses = extract_actual_clauses(scan_clauses, false);
	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							(void *) state);
}


static void
multicornExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
}


void
multicorn_init_typeoids(MulticornExecState * state)
{
	HeapTuple	typeTuple;
	Oid			typeoid;
	Oid			element_type;
	int			i,
				natts;

	natts = state->attinmeta->tupdesc->natts;
	for (i = 0; i < natts; i++)
	{
		typeoid = state->attinmeta->tupdesc->attrs[i]->atttypid;
		typeTuple = SearchSysCache1(TYPEOID,
									ObjectIdGetDatum(typeoid));
		if (!HeapTupleIsValid(typeTuple))
			elog(ERROR, "lookup failed for type %u",
				 typeoid);
		ReleaseSysCache(typeTuple);
		typeoid = HeapTupleGetOid(typeTuple);
		element_type = get_element_type(typeoid);
		state->typoids[i] = getBaseType(element_type ? element_type : typeoid);
	}
}

static void
multicornBeginForeignScan(ForeignScanState *node, int eflags)
{
	PyObject   *dummyQuals;
	AttInMetadata *attinmeta;
	MulticornExecState *state;
	MulticornPlanState *plan_state;
	Relation	rel = node->ss.ss_currentRelation;
	ListCell   *lc;
	int			i = 0;
	int			natts;
	RangeTblEntry *rtl_entry;
	Relids		accepted_relids = NULL;

	attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
	natts = node->ss.ss_currentRelation->rd_att->natts;
	state = (MulticornExecState *) palloc(sizeof(MulticornExecState));
	state->rownum = 0;
	state->attinmeta = attinmeta;
	state->pIterator = NULL;
	state->dvalues = palloc(natts * sizeof(Datum));
	state->nulls = palloc(natts * sizeof(bool));
    state->buffer = makeStringInfo();
    state->keyBuffer = makeStringInfo();
    state->valueBuffer = makeStringInfo();

	plan_state = (MulticornPlanState *) ((ForeignScan *) node->ss.ps.plan)->fdw_private;
	state->typoids = palloc(natts * sizeof(Oid));
	state->planstate = plan_state;
	node->fdw_state = (void *) state;
	multicorn_init_typeoids(state);
	if (!bms_is_empty(node->ss.ps.plan->extParam))
	{
		/* If we still have pending external params,  */
		/* look for them in the qual list.	*/
		/* We use dummy quals since we do not want to parse quals two times */
		dummyQuals = PyList_New(0);
		Py_DECREF(state->planstate->params);
		state->planstate->params = PyList_New(0);
		/* First, extract relids taht can be used. */
		foreach(lc, node->ss.ps.state->es_range_table)
		{
			i += 1;
			rtl_entry = (RangeTblEntry *) lfirst(lc);
			if (rtl_entry->relid == node->ss.ss_currentRelation->rd_id)
			{
				accepted_relids = bms_union(accepted_relids, bms_make_singleton(i));
			}
		}
		foreach(lc, node->ss.ps.plan->qual)
		{
			multicorn_extract_condition((Expr *) lfirst(lc), dummyQuals,
							 state->planstate->params, rel, accepted_relids);
		}
		Py_DECREF(dummyQuals);
	}
}

PyObject *
multicorn_get_quals(ForeignScanState *node, Relation rel, MulticornExecState * state)
{
	PyObject   *pConds,
			   *pParams,
			   *pTemp;
	ParamListInfo params = node->ss.ps.state->es_param_list_info;
	ParamExecData *exec_params = node->ss.ps.state->es_param_exec_vals;
	Py_ssize_t	i;

	/* Fill the params with the concrete values. */
	if (PyList_Size(state->planstate->params) > 0)
	{
		Py_ssize_t	valueIndex,
					attrIndex,
					length;
		PyObject   *pCurrentParam,
				   *pValue = NULL,
				   *pOperator,
				   *pKey,
				   *pParamKind,
				   *qual_class;
		Datum		value;

		qual_class = multicorn_get_class("multicorn.Qual");
		pParams = state->planstate->params;
		/* Copy the list of quals */
		length = PyList_Size(state->planstate->quals);
		pConds = PyList_New(0);
		for (i = 0; i < length; i++)
		{
			pTemp = PyList_GetItem(state->planstate->quals, i);
			PyList_Append(pConds, pTemp);
			Py_DECREF(pTemp);
		}
		length = PyList_Size(state->planstate->params);
		multicorn_error_check();
		for (i = 0; i < length; i++)
		{
			pCurrentParam = PyList_GetItem(pParams, i);
			pTemp = PyObject_GetAttrString(pCurrentParam, "param_id");
			valueIndex = PyInt_AsSsize_t(pTemp);
			Py_DECREF(pTemp);
			pTemp = PyObject_GetAttrString(pCurrentParam, "att_num");
			attrIndex = PyInt_AsSsize_t(pTemp);
			Py_DECREF(pTemp);
			pKey = PyObject_GetAttrString(pCurrentParam, "field_name");
			pOperator = PyObject_GetAttrString(pCurrentParam, "operator");
			pParamKind = PyObject_GetAttrString(pCurrentParam, "param_kind");
			switch (PyInt_AsSsize_t(pParamKind))
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
					Py_DECREF(pKey);
					Py_DECREF(pOperator);
					Py_DECREF(pValue);
					Py_DECREF(pParamKind);
					continue;
			}
			Py_DECREF(pParamKind);
			multicorn_error_check();
			pTemp = PyObject_GetAttrString(pCurrentParam, "param_type");
			pValue = multicorn_datum_to_python(value,
											   PyInt_AsSsize_t(pTemp),
											   rel->rd_att->attrs[attrIndex]);
			Py_DECREF(pTemp);
			pTemp = PyObject_CallFunctionObjArgs(qual_class, pKey, pOperator, pValue, NULL);
			PyList_Append(pConds, pTemp);
			Py_DECREF(pTemp);
			Py_DECREF(pOperator);
			Py_DECREF(pValue);
			Py_DECREF(pKey);
			multicorn_error_check();
		}
		Py_DECREF(qual_class);
	}
	else
	{
		/* Copy the "quals" */
		pConds = PyList_New(0);
		for (i = 0; i < PyList_Size(state->planstate->quals); i++)
		{
			pTemp = PyList_GetItem(state->planstate->quals, i);
			PyList_Append(pConds, pTemp);
			Py_DECREF(pTemp);
		}
	}

	return pConds;
}

void
multicorn_execute(ForeignScanState *node)
{
	PyObject   *pValue,
			   *pObj,
			   *pConds,
			   *colList;

	MulticornExecState *state = node->fdw_state;
	Relation	rel = node->ss.ss_currentRelation;

	/* Get the python FDW instance */
	pObj = multicorn_get_instance(rel);
	pConds = multicorn_get_quals(node, rel, state);
	colList = state->planstate->needed_columns;
	multicorn_error_check();
	pValue = PyObject_CallMethod(pObj, "execute", "(O,O)", pConds, colList);
	multicorn_error_check();
	if (pValue == Py_None)
	{
		state->pIterator = Py_None;
	}
	else
	{
		state->pIterator = PyObject_GetIter(pValue);
		multicorn_error_check();
	}
	Py_DECREF(pConds);
	Py_DECREF(pValue);
}


static TupleTableSlot *
multicornIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	MulticornExecState *state = (MulticornExecState *) node->fdw_state;
	PyObject   *pValue,
			   *pIterator,
			   *pStopIteration,
			   *pExceptionsModule,
			   *pErrType,
			   *pErrValue,
			   *pErrTraceback;

	if (state->pIterator == NULL)
	{
		multicorn_execute(node);
	}
	ExecClearTuple(slot);
	if (state->pIterator == Py_None)
	{
		/* No iterator returned from get_iterator */
		return slot;
	}
	pIterator = state->pIterator;
	pValue = PyIter_Next(pIterator);
	PyErr_Fetch(&pErrType, &pErrValue, &pErrTraceback);
	if (pErrType)
	{
		pExceptionsModule = PyImport_ImportModule("exceptions");
		pStopIteration = PyObject_GetAttrString(pExceptionsModule, "StopIteration");
		Py_DECREF(pExceptionsModule);
		if (PyErr_GivenExceptionMatches(pErrType, pStopIteration))
		{
			/* "Normal" stop iteration */
			Py_DECREF(pStopIteration);
			Py_DECREF(pErrType);
			Py_DECREF(pErrValue);
			Py_DECREF(pErrTraceback);
			Py_DECREF(pValue);
			return slot;
		}
		else
		{
			Py_DECREF(pStopIteration);
			multicorn_report_exception(pErrType, pErrValue, pErrTraceback);
		}
	}
	if (pValue == NULL || pValue == Py_None)
	{
		return slot;
	}
	if (PyMapping_Check(pValue))
	{
		pydict_to_postgres_tuple(state, node->ss.ss_currentRelation->rd_att, pValue);
	}
	else
	{
		if (PySequence_Check(pValue))
		{
			pysequence_to_postgres_tuple(state, node->ss.ss_currentRelation->rd_att, pValue);
		}
		else
		{
			elog(ERROR, "Cannot transform anything else than mappings and sequences to rows");
			return slot;
		}
	}
	Py_DECREF(pValue);
	slot->tts_values = state->dvalues;
	slot->tts_isnull = state->nulls;
	ExecStoreVirtualTuple(slot);
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
	pfree(state->dvalues);
	pfree(state->nulls);
	pfree(state->planstate);
    pfree(state->buffer);
    pfree(state->keyBuffer);
    pfree(state->valueBuffer);
	pfree(state);
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

void
multicorn_pyobject_to_datum(PyObject *object, TupleDesc tupdesc, MulticornExecState * state, int attnum)
{
	Datum	   *dvalues = state->dvalues;
	bool	   *nulls = state->nulls;
	Oid			typeoid;
	ssize_t		size = pyobject_to_cstring(object, tupdesc->attrs[attnum], state);

	state->dvalues[attnum] = (Datum) NULL;
	if (size < 0)
	{
		size = 0;
		nulls[attnum] = true;
	}
	else
	{
		if (!tupdesc->attrs[attnum]->attisdropped)
		{
			typeoid = state->typoids[attnum];
			if (typeoid == BYTEAOID || typeoid == TEXTOID)
			{
				dvalues[attnum] = PointerGetDatum(cstring_to_text_with_len(state->buffer->data, size));
				SET_VARSIZE(dvalues[attnum], size + VARHDRSZ);
			}
			else
			{
				dvalues[attnum] = InputFunctionCall(&state->attinmeta->attinfuncs[attnum],
													state->buffer->data,
									   state->attinmeta->attioparams[attnum],
									   state->attinmeta->atttypmods[attnum]);
			}
			if (dvalues[attnum])
				nulls[attnum] = false;
			else
				nulls[attnum] = true;
		}
		else
		{
			/* Handle dropped attributes by setting to NULL */
			dvalues[attnum] = (Datum) 0;
			nulls[attnum] = true;
		}
	}
}

void
pydict_to_postgres_tuple(MulticornExecState * state, TupleDesc desc, PyObject *pydict)
{
	PyObject   *pStr;
	char	   *key;
	int			i,
				natts;

	natts = desc->natts;
	for (i = 0; i < natts; i++)
	{
		key = NameStr(desc->attrs[i]->attname);
		if (PyMapping_HasKeyString(pydict, key))
		{
			pStr = PyMapping_GetItemString(pydict, key);
			multicorn_error_check();
			multicorn_pyobject_to_datum(pStr, desc, state, i);
			Py_DECREF(pStr);
			multicorn_error_check();
		}
		else
		{
			state->dvalues[i] = (Datum) NULL;
			state->nulls[i] = true;
		}
	}
}

void
pysequence_to_postgres_tuple(MulticornExecState * state, TupleDesc desc, PyObject *pyseq)
{
	int			i,
				natts = desc->natts;
	PyObject   *pStr;


	if (PySequence_Size(pyseq) != natts)
	{
		elog(ERROR, "The python backend did not return a valid sequence");
		return;
	}
	else
	{
		for (i = 0; i < natts; i++)
		{
			pStr = PySequence_GetItem(pyseq, i);
			multicorn_pyobject_to_datum(pStr, desc, state, i);
			Py_DECREF(pStr);
			multicorn_error_check();
		}
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
pyobject_to_cstring(PyObject *pyobject, Form_pg_attribute attribute, MulticornExecState * state)
{
	PyObject   *pStr;
	PyObject   *pTempStr;

	/* PyString_AsString and friends returns a pointer to the internal string. */
	/* We need to copy that in case it get dealloced later. */
	char	   *tempbuffer;
	Py_ssize_t	strlength = 0;
	if (PyNumber_Check(pyobject))
	{
		pTempStr = PyObject_Str(pyobject);
		PyString_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
        resetStringInfo(state->buffer);
        appendBinaryStringInfo(state->buffer, tempbuffer, strlength);
		Py_DECREF(pTempStr);
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
			PyString_AsStringAndSize(pyobject, &tempbuffer, &strlength);
            resetStringInfo(state->buffer);
            appendBinaryStringInfo(state->buffer, tempbuffer, strlength);
		}
		else
		{
			pTempStr = PyUnicode_Encode(PyUnicode_AsUnicode(pyobject), unicode_size,
										encoding_name, NULL);
			PyString_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
            resetStringInfo(state->buffer);
            appendBinaryStringInfo(state->buffer, tempbuffer, strlength);
			Py_DECREF(pTempStr);
		}
		multicorn_error_check();
		return strlength;
	}
	if (PyString_Check(pyobject))
	{
		if (PyString_AsStringAndSize(pyobject, &tempbuffer, &strlength) < 0)
		{
			ereport(WARNING,
					(errmsg("An error occured while decoding the %s column", NameStr(attribute->attname)),
					 errhint("You should maybe return unicode instead?")));
		}
        resetStringInfo(state->buffer);
        appendBinaryStringInfo(state->buffer, tempbuffer, strlength);
		multicorn_error_check();
		return strlength;
	}
	if (PyObject_IsInstance(pyobject, MULTICORN_DATE_CLASS))
	{
		PyObject   *formatted_date;

		formatted_date = PyObject_CallMethod(pyobject, "isoformat", "()");
		multicorn_error_check();
		PyString_AsStringAndSize(formatted_date, &tempbuffer, &strlength);
        resetStringInfo(state->buffer);
        appendBinaryStringInfo(state->buffer, tempbuffer, strlength);
		multicorn_error_check();
		Py_DECREF(formatted_date);
		return strlength;
	}
	if (PySequence_Check(pyobject))
	{
		/* Its an array */
		Py_ssize_t	i;
		Py_ssize_t	size = PySequence_Size(pyobject);
		PyObject   *delimiter = PyString_FromString(", ");
		PyObject   *mapped_list = PyList_New(0);
		PyObject   *format = PyString_FromString("{%s}");
		PyObject   *list_value;

		for (i = 0; i < size; i++)
		{
			pTempStr = PySequence_GetItem(pyobject, i);
			pyobject_to_cstring(pTempStr, attribute, state);
			Py_DECREF(pTempStr);
			list_value = PyString_FromString(state->buffer->data);
			PyList_Append(mapped_list, list_value);
			Py_DECREF(list_value);
		}
		pTempStr = PyObject_CallMethod(delimiter, "join", "(O)", mapped_list);
		pStr = PyString_Format(format, pTempStr);
		Py_DECREF(pTempStr);
		multicorn_error_check();
		PyString_AsStringAndSize(pStr, &tempbuffer, &strlength);
        resetStringInfo(state->buffer);
        appendBinaryStringInfo(state->buffer, tempbuffer, strlength);
		multicorn_error_check();
		Py_DECREF(mapped_list);
		Py_DECREF(delimiter);
		Py_DECREF(format);
		Py_DECREF(pStr);
		return strlength;
	}
	if (PyMapping_Check(pyobject))
	{
		PyObject   *mapped_list = PyList_New(0);
		PyObject   *items = PyMapping_Items(pyobject);
		PyObject   *delimiter = PyString_FromString(", ");
		PyObject   *current_tuple;
		PyObject   *pTemp;
		Py_ssize_t	i;
        ssize_t tempLength;
		Py_ssize_t	size = PyList_Size(items);
		for (i = 0; i < size; i++)
		{
			current_tuple = PySequence_GetItem(items, i);
            resetStringInfo(state->keyBuffer);
            resetStringInfo(state->valueBuffer);
			tempLength = pyobject_to_cstring(PyTuple_GetItem(current_tuple, 0), attribute, state);
            appendBinaryStringInfo(state->keyBuffer, state->buffer->data, tempLength);
			tempLength = pyobject_to_cstring(PyTuple_GetItem(current_tuple, 1), attribute, state);
            appendBinaryStringInfo(state->valueBuffer, state->buffer->data, tempLength);
			pTemp = PyString_FromFormat("%s=>%s", state->keyBuffer->data, state->valueBuffer->data);
			PyList_Append(mapped_list, pTemp);
			Py_DECREF(pTemp);
			Py_DECREF(current_tuple);
		}
		pTempStr = PyObject_CallMethod(delimiter, "join", "(O)", mapped_list);
		Py_DECREF(pTempStr);
		PyString_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
        resetStringInfo(state->buffer);
        appendBinaryStringInfo(state->buffer, tempbuffer, strlength);
		Py_DECREF(mapped_list);
		Py_DECREF(items);
		Py_DECREF(delimiter);
		return strlength;
	}
	pTempStr = PyObject_Str(pyobject);
	PyString_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
    resetStringInfo(state->buffer);
    appendBinaryStringInfo(state->buffer, tempbuffer, strlength);
	Py_DECREF(pTempStr);
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
		column_instance = PyObject_CallFunction(column_class, "(s,i,s)", key, typOid, typname);
		PyMapping_SetItemString(dict, key, column_instance);
		Py_DECREF(column_instance);
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
multicorn_extract_qual(Node *left, Node *right, Relation baserel, Relids base_relids, Form_pg_operator operator, PyObject **result)
{
	HeapTuple	tp;
	Form_pg_attribute attr;
	char	   *key,
			   *rightkey;
	TupleDesc	tupdesc = baserel->rd_att;
	PyObject   *value = NULL;
	PyObject   *qual_class = multicorn_get_class("multicorn.Qual");
	PyObject   *param_class = multicorn_get_class("multicorn.Param");
	PyObject   *var_class = multicorn_get_class("multicorn.Var");

	MulticornParamType param_type;
	Node	   *normalized_left;
	Node	   *normalized_right;
	Param	   *param;

	multicorn_error_check();
	multicorn_unnest(left, &normalized_left);
	multicorn_unnest(right, &normalized_right);
	if (IsA(normalized_right, Var))
	{
		/* If left is not for us, but right is, or is left is not a var, but */
		/* right is, switch them. */
		if ((!IsA(normalized_left, Var)) ||
			(!bms_is_member(((Var *) normalized_left)->varno, base_relids) &&
			 (bms_is_member(((Var *) normalized_right)->varno, base_relids))))
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
	}
	/* Do not treat the qual if is not in the form "one of our vars operator */
	/* something else" */
	if (IsA(normalized_left, Var) &&bms_is_member(((Var *) normalized_left)->varno, base_relids))
	{
		attr = tupdesc->attrs[((Var *) normalized_left)->varattno - 1];
		key = NameStr(attr->attname);
		multicorn_error_check();
		switch (normalized_right->type)
		{
			case T_Const:
				value = multicorn_datum_to_python(((Const *) normalized_right)->constvalue, ((Const *) normalized_right)->consttype, attr);
				*result = PyObject_CallFunction(qual_class, "(s,s,O)", key, NameStr(operator->oprname), value);
				Py_DECREF(value);
				Py_DECREF(qual_class);
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
				*result = PyObject_CallFunction(param_class,
							"(s,s,i,i,i,i)", key, NameStr(operator->oprname),
												param->paramid,
									 ((Var *) normalized_left)->varattno - 1,
												param_type,
												param->paramtype);
				multicorn_error_check();
				Py_DecRef(param_class);
				return param_type;
			case T_Var:
				if (bms_is_member(((Var *) normalized_right)->varno, base_relids))
				{
					/* The right side is also for us, build the value */
					rightkey = NameStr(tupdesc->attrs[((Var *) normalized_right)->varattno - 1]->attname);
				}
				else
				{
					/* placeholder value */
					rightkey = "";
				}
				*result = PyObject_CallFunction(var_class, "(s,s,s)", key, NameStr(operator->oprname), rightkey);
				return MulticornVAR;
			default:
				elog(WARNING, "Cant manage type %i", normalized_right->type);
				break;
		}
	}
	return MulticornUNKNOWN;
}

/*
 * Extract a condition usable from python from an clause, and append it to one
 * of qual_list or param_list.
 *
 * Baserel is used to look up attribute names, while base_relids is used to
 * determine which relids are suitable.
 * */
void
multicorn_extract_condition(Expr *clause, PyObject *qual_list, PyObject *param_list, Relation baserel, Relids base_relids)
{
	PyObject   *tempqual = NULL,
			   *pTemp,
			   *pOp;
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
					paramtype = multicorn_extract_qual(left, right, baserel, base_relids, operator_tup, &tempqual);
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
					if (tempqual != NULL)
					{
						Py_DECREF(tempqual);
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
					paramtype = multicorn_extract_qual(left, right, baserel, base_relids, operator_tup, &tempqual);

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
								pOp = PyObject_GetAttrString(tempqual, "operator");
								pTemp = Py_BuildValue("(O,O)", pOp, Py_True);
								Py_DECREF(pOp);
								PyObject_SetAttrString(tempqual, "operator", pTemp);
								Py_DECREF(pTemp);
							}
							else
							{
								pOp = PyObject_GetAttrString(tempqual, "operator");
								pTemp = Py_BuildValue("(O,O)", pOp, Py_False);
								Py_DECREF(pOp);
								PyObject_SetAttrString(tempqual, "operator", pTemp);
								Py_DECREF(pTemp);
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
					if (tempqual != NULL)
					{
						Py_DECREF(tempqual);
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
					PyObject   *pTemp;
					Form_pg_attribute attr = tupdesc->attrs[((Var *) nulltest->arg)->varattno - 1];

					if (nulltest->nulltesttype == IS_NULL)
					{
						operator_name = "=";
					}
					else
					{
						operator_name = "<>";
					}
					pTemp = PyObject_CallFunction(qual_class, "(s, s, O)", NameStr(attr->attname), operator_name, Py_None);
					PyList_Append(qual_list, pTemp);
					Py_DECREF(pTemp);
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
	PyObject   *result,
			   *tempString;

	HeapTuple	typeTuple;
	Form_pg_type typeStruct;
	const char *encoding_name;
	char	   *tempvalue;
	long		number;
	fsec_t		fsec;
	struct pg_tm *pg_tm_value;
	Py_ssize_t	size;

	if (!datumvalue)
	{
		Py_INCREF(Py_None);
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
			tempString = PyString_FromString(tempvalue);
			result = PyFloat_FromString(tempString, NULL);
			Py_DECREF(tempString);
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
			pfree(pg_tm_value);
			break;
		case TIMESTAMPOID:
			/* TODO: see what could go wrong */
			pg_tm_value = palloc(sizeof(struct pg_tm));
			timestamp2tm(DatumGetTimestamp(datumvalue), NULL, pg_tm_value, &fsec, NULL, NULL);
			result = PyDateTime_FromDateAndTime(pg_tm_value->tm_year,
								   pg_tm_value->tm_mon, pg_tm_value->tm_mday,
												pg_tm_value->tm_hour, pg_tm_value->tm_min, pg_tm_value->tm_sec, 0);
			pfree(pg_tm_value);
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
				Datum		buffer = (Datum) NULL;
				PyObject   *listelem = NULL;
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
							Py_DECREF(listelem);
							result = NULL;
							break;
						}
						else
						{
							PyList_Append(result, listelem);
							Py_DECREF(listelem);
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
	PyObject   *pTemp;
	PyObject   *tracebackModule = PyImport_ImportModule("traceback");
	PyObject   *format_exception = PyObject_GetAttrString(tracebackModule, "format_exception");
	PyObject   *newline = PyString_FromString("\n");

	PyErr_NormalizeException(&pErrType, &pErrValue, &pErrTraceback);
	pTemp = PyObject_GetAttrString(pErrType, "__name__");
	errName = PyString_AsString(pTemp);
	Py_DECREF(pTemp);
	errValue = PyString_AsString(PyObject_Str(pErrValue));
	if (pErrTraceback)
	{
		traceback_list = PyObject_CallFunction(format_exception, "(O,O,O)", pErrType, pErrValue, pErrTraceback);
		errTraceback = PyString_AsString(PyObject_CallMethod(newline, "join", "(O)", traceback_list));
		Py_DECREF(pErrTraceback);
		Py_DECREF(traceback_list);
	}
	Py_DECREF(pErrType);
	Py_DECREF(pErrValue);
	Py_DECREF(format_exception);
	Py_DECREF(tracebackModule);
	Py_DECREF(newline);
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
	MULTICORN_PZERO = Py_BuildValue("d");
	MULTICORN = PyImport_ImportModule("multicorn");
	MULTICORN_DATE_MODULE = PyImport_ImportModule("datetime");
    MULTICORN_DATE_CLASS = PyObject_GetAttrString(MULTICORN_DATE_MODULE, "date");
}

void
_PG_fini()
{
	Py_DECREF(TABLES_DICT);
    Py_DECREF(MULTICORN_PZERO);
    Py_DECREF(MULTICORN_DATE_MODULE);
    Py_DECREF(MULTICORN_DATE_CLASS);
	Py_Finalize();
}

PyObject *
multicorn_get_multicorn()
{
	return MULTICORN;
}

PyObject *
multicorn_get_class(char *classname)
{
	return PyObject_CallMethod(multicorn_get_multicorn(), "get_class", "(s)", classname);
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
		pColumns = PyObject_CallFunction(pOrderedDictClass, "()");
		Py_DECREF(pOrderedDictClass);
		multicorn_get_attributes_def(rel->rd_att, pColumns);
		pObj = PyObject_CallFunction(pClass, "(O,O)", pOptions, pColumns);
		Py_DECREF(pClass);
		Py_DECREF(pOptions);
		Py_DECREF(pColumns);
		multicorn_error_check();
		PyDict_SetItem(TABLES_DICT, pTableId, pObj);
		Py_DECREF(pObj);
		multicorn_error_check();
	}
	Py_DECREF(pTableId);
	return pObj;
}



void
multicorn_get_column(Expr *expr, TupleDesc desc, PyObject *list)
{
	ListCell   *cell;
	char	   *key;
	PyObject   *pTemp;

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
				pTemp = PyString_FromString(key);
				PySet_Add(list, pTemp);
				Py_DECREF(pTemp);
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
					pTemp = PyString_FromString(key);
					PySet_Add(list, pTemp);
					Py_DECREF(pTemp);
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
