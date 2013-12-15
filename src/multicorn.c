/*
 * The Multicorn Foreign Data Wrapper allows you to fetch foreign data in
 * Python in your PostgreSQL server
 *
 * This software is released under the postgresql licence
 *
 * author: Kozea
 */
#include "multicorn.h"
#include "commands/explain.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "parser/parsetree.h"


PG_MODULE_MAGIC;


extern Datum multicorn_handler(PG_FUNCTION_ARGS);
extern Datum multicorn_validator(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(multicorn_handler);
PG_FUNCTION_INFO_V1(multicorn_validator);


void		_PG_init(void);
void		_PG_fini(void);

/*
 * List of modified relations
 */
static List *multicorn_modified_relations = NULL;


/*
 * FDW functions declarations
 */

static void multicornGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);
static void multicornGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);
static ForeignScan *multicornGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses);
static void multicornExplainForeignScan(ForeignScanState *node,
							ExplainState *es);
static void multicornBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *multicornIterateForeignScan(ForeignScanState *node);
static void multicornReScanForeignScan(ForeignScanState *node);
static void multicornEndForeignScan(ForeignScanState *node);

#if PG_VERSION_NUM >= 90300
static void multicornAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation);

static List *multicornPlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index);
static void multicornBeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *resultRelInfo,
							List *fdw_private,
							int subplan_index,
							int eflags);
static TupleTableSlot *multicornExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planslot);
static TupleTableSlot *multicornExecForeignDelete(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *multicornExecForeignUpdate(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot);
static void multicornEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo);

static void multicorn_xact_callback(XactEvent event, void *arg);

static void multicorn_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                                           SubTransactionId parentSubid, void *arg);

typedef struct MulticornCallbackData
{
	Oid			foreigntableid;
}	MulticornCallbackData;
#endif

/*	Helpers functions */
void	   *serializePlanState(MulticornPlanState * planstate);
MulticornExecState *initializeExecState(void *internal_plan_state);



void
_PG_init()
{
	Py_Initialize();
	multicorn_modified_relations = NULL;
	RegisterXactCallback(multicorn_xact_callback, NULL);
        RegisterSubXactCallback(multicorn_subxact_callback, NULL);
}

void
_PG_fini()
{
	Py_Finalize();
}


Datum
multicorn_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	/* Plan phase */
	fdw_routine->GetForeignRelSize = multicornGetForeignRelSize;
	fdw_routine->GetForeignPaths = multicornGetForeignPaths;
	fdw_routine->GetForeignPlan = multicornGetForeignPlan;
	fdw_routine->ExplainForeignScan = multicornExplainForeignScan;

	/* Scan phase */
	fdw_routine->BeginForeignScan = multicornBeginForeignScan;
	fdw_routine->IterateForeignScan = multicornIterateForeignScan;
	fdw_routine->ReScanForeignScan = multicornReScanForeignScan;
	fdw_routine->EndForeignScan = multicornEndForeignScan;

#if PG_VERSION_NUM >= 90300
	/* Code for 9.3 */
	fdw_routine->AddForeignUpdateTargets = multicornAddForeignUpdateTargets;
	/* Writable API */
	fdw_routine->PlanForeignModify = multicornPlanForeignModify;
	fdw_routine->BeginForeignModify = multicornBeginForeignModify;
	fdw_routine->ExecForeignInsert = multicornExecForeignInsert;
	fdw_routine->ExecForeignDelete = multicornExecForeignDelete;
	fdw_routine->ExecForeignUpdate = multicornExecForeignUpdate;
	fdw_routine->EndForeignModify = multicornEndForeignModify;
#endif

	PG_RETURN_POINTER(fdw_routine);
}

Datum
multicorn_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *className = NULL;
	ListCell   *cell;
	PyObject   *p_class;

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
	}
	if (catalog == ForeignServerRelationId)
	{
		if (className == NULL)
		{
			ereport(ERROR, (errmsg("%s", "The wrapper parameter is mandatory, specify a valid class name")));
		}
		/* Try to import the class. */
		p_class = getClassString(className);
		errorCheck();
		Py_DECREF(p_class);
	}
	PG_RETURN_VOID();
}


/*
 * multicornGetForeignRelSize
 *		Obtain relation size estimates for a foreign table.
 *		This is done by calling the
 */
static void
multicornGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
	MulticornPlanState *planstate = palloc0(sizeof(MulticornPlanState));
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	ListCell   *lc;

	baserel->fdw_private = planstate;
	planstate->fdw_instance = getInstance(foreigntableid);
	planstate->foreigntableid = foreigntableid;
	/* Initialize the conversion info array */
	{
		Relation	rel = RelationIdGetRelation(ftable->relid);
		AttInMetadata *attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(rel));

		planstate->numattrs = RelationGetNumberOfAttributes(rel);

		planstate->cinfos = palloc0(sizeof(ConversionInfo *) *
									planstate->numattrs);
		initConversioninfo(planstate->cinfos, attinmeta);
		RelationClose(rel);
	}

	/* Pull "var" clauses to build an appropriate target list */
	foreach(lc, extractColumns(baserel->reltargetlist, baserel->baserestrictinfo))
	{
		Var		   *var = (Var *) lfirst(lc);
		Value	   *colname;

		/* Store only a Value node containing the string name of the column. */
		colname = colnameFromVar(var, root, planstate);
		if (colname != NULL && strVal(colname) != NULL)
		{
			planstate->target_list = lappend(planstate->target_list, colname);
		}
	}
	/* Extract the restrictions from the plan. */
	foreach(lc, baserel->baserestrictinfo)
	{
		extractRestrictions(baserel->relids, ((RestrictInfo *) lfirst(lc))->clause,
							&planstate->qual_list);

	}
	/* Inject the "rows" and "width" attribute into the baserel */
	getRelSize(planstate, root, &baserel->rows, &baserel->width);
}

/*
 * multicornGetForeignPaths
 *		Create possible access paths for a scan on the foreign table.
 *		This is done by calling the "get_path_keys method on the python side,
 *		and parsing its result to build parameterized paths according to the
 *		equivalence classes found in the plan.
 */
static void
multicornGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	Path	   *path;
	MulticornPlanState *planstate = baserel->fdw_private;

	/* Extract a friendly version of the pathkeys. */
	List	   *possiblePaths = pathKeys(planstate);

	findPaths(root, baserel, possiblePaths, planstate->startupCost);
	/* Add a default path */
	path = (Path *) create_foreignscan_path(root, baserel,
											baserel->rows,
											planstate->startupCost,
											baserel->rows * baserel->width,
											NIL,		/* no pathkeys */
											NULL,		/* no outer rel either */
											(void *) baserel->fdw_private);

	add_path(baserel, path);
	errorCheck();
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
	MulticornPlanState *planstate = (MulticornPlanState *) baserel->fdw_private;
	ListCell   *lc;

	scan_clauses = extract_actual_clauses(scan_clauses, false);
	/* Extract the quals coming from a parameterized path, if any */
	if (best_path->path.param_info)
	{

		foreach(lc, scan_clauses)
		{
			extractRestrictions(baserel->relids, (Expr *) lfirst(lc),
								&planstate->qual_list);
		}
	}
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							scan_clauses,		/* no expressions to evaluate */
							serializePlanState(planstate));
}

/*
 * multicornExplainForeignScan
 *		Placeholder for additional "EXPLAIN" information.
 *		This should (at least) output the python class name, as well
 *		as information that was taken into account for the choice of a path.
 */
static void
multicornExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
}

/*
 *	multicornBeginForeignScan
 *		Initialize the foreign scan.
 *		This (primarily) involves :
 *			- retrieving cached info from the plan phase
 *			- initializing various buffers
 */
static void
multicornBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fscan = (ForeignScan *) node->ss.ps.plan;
	MulticornExecState *execstate;
	TupleDesc	tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
	ListCell   *lc;

	execstate = initializeExecState(fscan->fdw_private);
	execstate->values = palloc(sizeof(Datum) * tupdesc->natts);
	execstate->nulls = palloc(sizeof(bool) * tupdesc->natts);
	execstate->qual_list = NULL;
	foreach(lc, fscan->fdw_exprs)
	{
		extractRestrictions(bms_make_singleton(fscan->scan.scanrelid),
							((Expr *) lfirst(lc)),
							&execstate->qual_list);
	}
	initConversioninfo(execstate->cinfos, TupleDescGetAttInMetadata(tupdesc));
	node->fdw_state = execstate;
}


/*
 * multicornIterateForeignScan
 *		Retrieve next row from the result set, or clear tuple slot to indicate
 *		EOF.
 *
 *		This is done by iterating over the result from the "execute" python
 *		method.
 */
static TupleTableSlot *
multicornIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	MulticornExecState *execstate = node->fdw_state;
	PyObject   *p_value;

	if (execstate->p_iterator == NULL)
	{
		execute(node);
	}
	ExecClearTuple(slot);
	if (execstate->p_iterator == Py_None)
	{
		/* No iterator returned from get_iterator */
		return slot;
	}
	p_value = PyIter_Next(execstate->p_iterator);
	/* A none value results in an empty slot. */
	if (p_value == NULL || p_value == Py_None)
	{
		if (p_value != NULL)
		{
			Py_DECREF(p_value);
		}
		return slot;
	}
	slot->tts_values = execstate->values;
	slot->tts_isnull = execstate->nulls;
	pythonResultToTuple(p_value, slot, execstate->cinfos, execstate->buffer);
	ExecStoreVirtualTuple(slot);
	Py_DECREF(p_value);

	return slot;
}

/*
 * multicornReScanForeignScan
 *		Restart the scan
 */
static void
multicornReScanForeignScan(ForeignScanState *node)
{
	MulticornExecState *state = node->fdw_state;

	if (state->p_iterator)
	{
		Py_DECREF(state->p_iterator);
		state->p_iterator = NULL;
	}
}

/*
 *	multicornEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan.
 */
static void
multicornEndForeignScan(ForeignScanState *node)
{
	MulticornExecState *state = node->fdw_state;
	PyObject   *result = PyObject_CallMethod(state->fdw_instance, "end_scan", "()");

	errorCheck();
	Py_DECREF(result);
	Py_DECREF(state->fdw_instance);
	if (state->p_iterator != NULL)
	{
		Py_DECREF(state->p_iterator);
	}
	state->p_iterator = NULL;
}



#if PG_VERSION_NUM >= 90300
/*
 * multicornAddForeigUpdateTargets
 *		Add resjunk columns needed for update/delete.
 */
static void
multicornAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation)
{
	Var		   *var = NULL;
	TargetEntry *tle,
			   *returningTle;
	PyObject   *instance = getInstance(target_relation->rd_id);
	const char *attrname = getRowIdColumn(instance);
	TupleDesc	desc = target_relation->rd_att;
	int			i;
	ListCell   *cell;

	foreach(cell, parsetree->returningList)
	{
		returningTle = lfirst(cell);
		tle = copyObject(returningTle);
		tle->resjunk = true;
		parsetree->targetList = lappend(parsetree->targetList, tle);
	}


	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = desc->attrs[i];

		if (!att->attisdropped)
		{
			if (strcmp(NameStr(att->attname), attrname) == 0)
			{
				var = makeVar(parsetree->resultRelation,
							  att->attnum,
							  att->atttypid,
							  att->atttypmod,
							  att->attcollation,
							  0);
				break;
			}
		}
	}
	if (var == NULL)
	{
		ereport(ERROR, (errmsg("%s", "The rowid attribute does not exist")));
	}
	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  strdup(attrname),
						  true);
	parsetree->targetList = lappend(parsetree->targetList, tle);
	Py_DECREF(instance);
}


/*
 * multicornPlanForeignModify
 *		Plan a foreign write operation.
 *		This is done by checking the "supported operations" attribute
 *		on the python class.
 */
static List *
multicornPlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index)
{
	return NULL;
}


/*
 * multicornBeginForeignModify
 *		Initialize a foreign write operation.
 */
static void
multicornBeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *resultRelInfo,
							List *fdw_private,
							int subplan_index,
							int eflags)
{
	MulticornModifyState *modstate = palloc0(sizeof(MulticornModifyState));
	Relation	rel = resultRelInfo->ri_RelationDesc;
	TupleDesc	desc = RelationGetDescr(rel);
	PlanState  *ps = mtstate->mt_plans[subplan_index];
	Plan	   *subplan = ps->plan;
	MemoryContext oldcontext;
	int			i;

	modstate->cinfos = palloc0(sizeof(ConversionInfo *) *
							   desc->natts);
	modstate->buffer = makeStringInfo();
	modstate->fdw_instance = getInstance(rel->rd_id);
	modstate->rowidAttrName = getRowIdColumn(modstate->fdw_instance);
	initConversioninfo(modstate->cinfos, TupleDescGetAttInMetadata(desc));
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	multicorn_modified_relations = list_append_unique_oid(multicorn_modified_relations, rel->rd_id);
	MemoryContextSwitchTo(oldcontext);
	if (ps->ps_ResultTupleSlot)
	{
		TupleDesc	resultTupleDesc = ps->ps_ResultTupleSlot->tts_tupleDescriptor;

		modstate->resultCinfos = palloc0(sizeof(ConversionInfo *) *
										 resultTupleDesc->natts);
		initConversioninfo(modstate->resultCinfos, TupleDescGetAttInMetadata(resultTupleDesc));
	}
	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = desc->attrs[i];

		if (!att->attisdropped)
		{
			if (strcmp(NameStr(att->attname), modstate->rowidAttrName) == 0)
			{
				modstate->rowidCinfo = modstate->cinfos[i];
				break;
			}
		}
	}
	modstate->rowidAttno = ExecFindJunkAttributeInTlist(subplan->targetlist, modstate->rowidAttrName);
	resultRelInfo->ri_FdwState = modstate;
}

/*
 * multicornExecForeignInsert
 *		Execute a foreign insert operation
 *		This is done by calling the python "insert" method.
 */
static TupleTableSlot *
multicornExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	MulticornModifyState *modstate = resultRelInfo->ri_FdwState;
	PyObject   *fdw_instance = modstate->fdw_instance;
	PyObject   *values = tupleTableSlotToPyObject(slot, modstate->cinfos);
	PyObject   *p_new_value = PyObject_CallMethod(fdw_instance, "insert", "(O)", values);

	errorCheck();
	if (p_new_value && p_new_value != Py_None)
	{
		ExecClearTuple(slot);
		pythonResultToTuple(p_new_value, slot, modstate->cinfos, modstate->buffer);
		ExecStoreVirtualTuple(slot);
		Py_DECREF(p_new_value);
	}
	Py_DECREF(values);
	errorCheck();
	return slot;
}

/*
 * multicornExecForeignDelete
 *		Execute a foreign delete operation
 *		This is done by calling the python "delete" method, with the opaque
 *		rowid that was supplied.
 */
static TupleTableSlot *
multicornExecForeignDelete(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	MulticornModifyState *modstate = resultRelInfo->ri_FdwState;
	PyObject   *fdw_instance = modstate->fdw_instance,
			   *p_row_id,
			   *p_new_value;
	bool		is_null;
	ConversionInfo *cinfo = modstate->rowidCinfo;
	Datum		value = ExecGetJunkAttribute(planSlot, modstate->rowidAttno, &is_null);

	p_row_id = datumToPython(value, cinfo->atttypoid, cinfo);
	p_new_value = PyObject_CallMethod(fdw_instance, "delete", "(O)", p_row_id);
	errorCheck();
	if (p_new_value == NULL || p_new_value == Py_None)
	{
		p_new_value = tupleTableSlotToPyObject(planSlot, modstate->resultCinfos);
	}
	ExecClearTuple(slot);
	pythonResultToTuple(p_new_value, slot, modstate->cinfos, modstate->buffer);
	ExecStoreVirtualTuple(slot);
	Py_DECREF(p_new_value);
	Py_DECREF(p_row_id);
	errorCheck();
	return slot;
}

/*
 * multicornExecForeignUpdate
 *		Execute a foreign update operation
 *		This is done by calling the python "update" method, with the opaque
 *		rowid that was supplied.
 */
static TupleTableSlot *
multicornExecForeignUpdate(EState *estate, ResultRelInfo *resultRelInfo,
						   TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	MulticornModifyState *modstate = resultRelInfo->ri_FdwState;
	PyObject   *fdw_instance = modstate->fdw_instance,
			   *p_row_id,
			   *p_new_value,
			   *p_value = tupleTableSlotToPyObject(slot, modstate->cinfos);
	bool		is_null;
	ConversionInfo *cinfo = modstate->rowidCinfo;
	Datum		value = ExecGetJunkAttribute(planSlot, modstate->rowidAttno, &is_null);

	p_row_id = datumToPython(value, cinfo->atttypoid, cinfo);
	p_new_value = PyObject_CallMethod(fdw_instance, "update", "(O,O)", p_row_id,
									  p_value);
	errorCheck();
	if (p_new_value != NULL && p_new_value != Py_None)
	{
		ExecClearTuple(slot);
		pythonResultToTuple(p_new_value, slot, modstate->cinfos, modstate->buffer);
		ExecStoreVirtualTuple(slot);
		Py_DECREF(p_new_value);
	}
	Py_DECREF(p_row_id);
	errorCheck();
	return slot;
}

/*
 * multicornEndForeignModify
 *		Clean internal state after a modify operation.
 */
static void
multicornEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)

{
	MulticornModifyState *modstate = resultRelInfo->ri_FdwState;
	PyObject   *result = PyObject_CallMethod(modstate->fdw_instance, "end_modify", "()");

	errorCheck();
	Py_DECREF(modstate->fdw_instance);
	Py_DECREF(result);
}

/*
 * Callback used to propagate pre-commit / commit / rollback.
 */
static void
multicorn_xact_callback(XactEvent event, void *arg)
{
	PyObject   *instance;
	ListCell   *lc;

	foreach(lc, multicorn_modified_relations)
	{
		Oid			foreigntableid = lfirst_oid(lc);
                CacheEntry* entry  = getCacheEntry(foreigntableid);
                instance = entry->value;

                if (entry->xact_depth == 0)
                        continue;

		switch (event)
		{
			case XACT_EVENT_PRE_COMMIT:
				PyObject_CallMethod(instance, "pre_commit", "()");
				errorCheck();
				break;
			case XACT_EVENT_COMMIT:
				PyObject_CallMethod(instance, "commit", "()");
				errorCheck();
				break;
			case XACT_EVENT_ABORT:
				PyObject_CallMethod(instance, "rollback", "()");
				errorCheck();
				break;
			default:
				break;
		}
                Py_DECREF(instance);

                entry->xact_depth = 0;
	}
	list_free(multicorn_modified_relations);
	multicorn_modified_relations = NULL;
}


/*
 * Callback used to propagate a subtransaction end.
 */
static void
multicorn_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
                                           SubTransactionId parentSubid, void *arg)
{
  PyObject      *instance;
  int           curlevel;
  ListCell      *lc;


  /* Nothing to do after commit or subtransaction start. */
  if (event == SUBXACT_EVENT_COMMIT_SUB || event == SUBXACT_EVENT_START_SUB)
    return;

  curlevel = GetCurrentTransactionNestLevel();

  foreach(lc, multicorn_modified_relations)
  {
    Oid foreigntableid = lfirst_oid(lc);
    CacheEntry* entry  = getCacheEntry(foreigntableid);
    instance = entry->value;

    if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
    {
      PyObject_CallMethod(instance, "sub_commit", "(i)", curlevel);
    }
    else
    {
      PyObject_CallMethod(instance, "sub_rollback", "(i)", curlevel);
    }
    errorCheck();
    Py_DECREF(instance);

    entry->xact_depth--;
  }
}
#endif


/*
 *	"Serialize" a MulticornPlanState, so that it is safe to be carried
 *	between the plan and the execution safe.
 */
void *
serializePlanState(MulticornPlanState * state)
{
	List	   *result = NULL;

	result = lappend(result, makeConst(INT4OID,
						  -1, InvalidOid, -1, state->numattrs, false, true));
	result = lappend(result, makeConst(INT4OID,
					-1, InvalidOid, -1, state->foreigntableid, false, true));
	result = lappend(result, state->target_list);
	return result;
}

/*
 *	"Deserialize" an internal state and inject it in an
 *	MulticornExecState
 */
MulticornExecState *
initializeExecState(void *internalstate)
{
	MulticornExecState *execstate = palloc0(sizeof(MulticornExecState));
	List	   *values = (List *) internalstate;
	AttrNumber	attnum = ((Const *) linitial(values))->constvalue;
	Oid			foreigntableid = ((Const *) lsecond(values))->constvalue;

	/* Those list must be copied, because their memory context can become */
	/* invalid during the execution (in particular with the cursor interface) */
	execstate->target_list = copyObject(lthird(values));
	execstate->fdw_instance = getInstance(foreigntableid);
	execstate->buffer = makeStringInfo();
	execstate->cinfos = palloc0(sizeof(ConversionInfo *) * attnum);
	execstate->values = palloc(attnum * sizeof(Datum));
	execstate->nulls = palloc(attnum * sizeof(bool));
	return execstate;
}
