/*
 * The Multicorn Foreign Data Wrapper allows you to fetch foreign data in
 * Python in your PostgreSQL servner
 *
 * This software is released under the postgresql licence
 *
 * author: Kozea
 */
#include "multicorn.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/clauses.h"
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#endif
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "parser/parsetree.h"
#include "executor/spi.h"

PG_MODULE_MAGIC;

extern Datum multicorn_handler(PG_FUNCTION_ARGS);
extern Datum multicorn_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(multicorn_handler);
PG_FUNCTION_INFO_V1(multicorn_validator);


void		_PG_init(void);
void		_PG_fini(void);

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
						List *scan_clauses
#if PG_VERSION_NUM >= 90500
						, Plan *outer_plan
#endif
		);
static void multicornExplainForeignScan(ForeignScanState *node, ExplainState *es);
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

static void multicorn_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						   SubTransactionId parentSubid, void *arg);
#endif

#if PG_VERSION_NUM >= 90500
static List *multicornImportForeignSchema(ImportForeignSchemaStmt * stmt,
							 Oid serverOid);
#endif

static void multicorn_xact_callback(XactEvent event, void *arg);

static void multicorn_release_callback(ResourceReleasePhase phase,
				       bool isCommit,
				       bool isTopLevel,
				       void *arg);

/*	Helpers functions */
void	   *serializePlanState(MulticornPlanState * planstate);
MulticornExecState *initializeExecState(void *internal_plan_state);

/* Hash table mapping oid to fdw instances */
HTAB	   *InstancesHash;

void
_PG_init()
{
	HASHCTL		ctl;
	MemoryContext oldctx = MemoryContextSwitchTo(CacheMemoryContext);

	Py_Initialize();
	RegisterXactCallback(multicorn_xact_callback, NULL);
#if PG_VERSION_NUM >= 90300
	RegisterSubXactCallback(multicorn_subxact_callback, NULL);
#endif
	/* RegisterResourceReleaseBallback dates back to 2004 and is in
	   the 8.0 release.  Not sure if we need a version number 
	   restriction around it. */
	RegisterResourceReleaseCallback(multicorn_release_callback, NULL);
	
	/* Initialize the global oid -> python instances hash */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(CacheEntry);
	ctl.hash = oid_hash;
	ctl.hcxt = CacheMemoryContext;
	InstancesHash = hash_create("multicorn instances", 32,
								&ctl,
								HASH_ELEM | HASH_FUNCTION);
	MemoryContextSwitchTo(oldctx);
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

#if PG_VERSION_NUM >= 90500
	fdw_routine->ImportForeignSchema = multicornImportForeignSchema;
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
	bool		needWholeRow = false;
	TupleDesc	desc;

	baserel->fdw_private = planstate;
	planstate->fdw_instance = getInstance(foreigntableid);
	planstate->foreigntableid = foreigntableid;
	/* Initialize the conversion info array */
	{
		Relation	rel = RelationIdGetRelation(ftable->relid);
		AttInMetadata *attinmeta;

		desc = RelationGetDescr(rel);
		attinmeta = TupleDescGetAttInMetadata(desc);
		planstate->numattrs = RelationGetNumberOfAttributes(rel);

		planstate->cinfos = palloc0(sizeof(ConversionInfo *) *
									planstate->numattrs);
		initConversioninfo(planstate->cinfos, attinmeta);
		needWholeRow = rel->trigdesc && rel->trigdesc->trig_insert_after_row;
		RelationClose(rel);
	}
	if (needWholeRow)
	{
		int			i;

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);

			if (!att->attisdropped)
			{
				planstate->target_list = lappend(planstate->target_list, makeString(NameStr(att->attname)));
			}
		}
	}
	else
	{
		/* Pull "var" clauses to build an appropriate target list */
#if PG_VERSION_NUM >= 90600
		foreach(lc, extractColumns(baserel->reltarget->exprs, baserel->baserestrictinfo))
#else
		foreach(lc, extractColumns(baserel->reltargetlist, baserel->baserestrictinfo))
#endif
		{
			Var		   *var = (Var *) lfirst(lc);
			Value	   *colname;

			/*
			 * Store only a Value node containing the string name of the
			 * column.
			 */
			colname = colnameFromVar(var, root, planstate);
			if (colname != NULL && strVal(colname) != NULL)
			{
				planstate->target_list = lappend(planstate->target_list, colname);
			}
		}
	}
	/* Extract the restrictions from the plan. */
	foreach(lc, baserel->baserestrictinfo)
	{
		extractRestrictions(baserel->relids, ((RestrictInfo *) lfirst(lc))->clause,
							&planstate->qual_list);

	}
	/* Inject the "rows" and "width" attribute into the baserel */
#if PG_VERSION_NUM >= 90600
	getRelSize(planstate, root, &baserel->rows, &baserel->reltarget->width);
	planstate->width = baserel->reltarget->width;
#else
	getRelSize(planstate, root, &baserel->rows, &baserel->width);
#endif
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
	List				*pathes; /* List of ForeignPath */
	MulticornPlanState	*planstate = baserel->fdw_private;
	ListCell		    *lc;

	/* These lists are used to handle sort pushdown */
	List				*apply_pathkeys = NULL;
	List				*deparsed_pathkeys = NULL;

	/* Extract a friendly version of the pathkeys. */
	List	   *possiblePaths = pathKeys(planstate);

	/* Try to find parameterized paths */
	pathes = findPaths(root, baserel, possiblePaths, planstate->startupCost,
			planstate, apply_pathkeys, deparsed_pathkeys);

	/* Add a simple default path */
	pathes = lappend(pathes, create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
												 	  NULL,  /* default pathtarget */
#endif
			baserel->rows,
			planstate->startupCost,
#if PG_VERSION_NUM >= 90600
			baserel->rows * baserel->reltarget->width,
#else
			baserel->rows * baserel->width,
#endif
			NIL,		/* no pathkeys */
		    NULL,
#if PG_VERSION_NUM >= 90500
			NULL,
#endif
			NULL));

	/* Handle sort pushdown */
	if (root->query_pathkeys)
	{
		List		*deparsed = deparse_sortgroup(root, foreigntableid, baserel);

		if (deparsed)
		{
			/* Update the sort_*_pathkeys lists if needed */
			computeDeparsedSortGroup(deparsed, planstate, &apply_pathkeys,
					&deparsed_pathkeys);
		}
	}

	/* Add each ForeignPath previously found */
	foreach(lc, pathes)
	{
		ForeignPath *path = (ForeignPath *) lfirst(lc);

		/* Add the path without modification */
		add_path(baserel, (Path *) path);

		/* Add the path with sort pusdown if possible */
		if (apply_pathkeys && deparsed_pathkeys)
		{
			ForeignPath *newpath;

			newpath = create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
												 	  NULL,  /* default pathtarget */
#endif
					path->path.rows,
					path->path.startup_cost, path->path.total_cost,
					apply_pathkeys, NULL,
#if PG_VERSION_NUM >= 90500
					NULL,
#endif
					(void *) deparsed_pathkeys);

			newpath->path.param_info = path->path.param_info;
			add_path(baserel, (Path *) newpath);
		}
	}
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
						List *scan_clauses
#if PG_VERSION_NUM >= 90500
						, Plan *outer_plan
#endif
		)
{
	Index		scan_relid = baserel->relid;
	MulticornPlanState *planstate = (MulticornPlanState *) baserel->fdw_private;
	ListCell   *lc;

	best_path->path.pathtarget->width = planstate->width;

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
	planstate->pathkeys = (List *) best_path->fdw_private;
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							scan_clauses,		/* no expressions to evaluate */
							serializePlanState(planstate)
#if PG_VERSION_NUM >= 90500
							, NULL
							, NULL /* All quals are meant to be rechecked */
							, NULL
#endif
							);
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
	PyObject *p_iterable = execute(node, es),
			 *p_item,
			 *p_str;
	Py_INCREF(p_iterable);
	while((p_item = PyIter_Next(p_iterable))){
		p_str = PyObject_Str(p_item);
		ExplainPropertyText("Multicorn", PyString_AsString(p_str), es);
		Py_DECREF(p_str);
	}
	Py_DECREF(p_iterable);
	errorCheck();
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
	ListCell   *lc;

	execstate = initializeExecState(fscan->fdw_private);
	execstate->qual_list = NULL;
	execstate->values = NULL;
	execstate->nulls = NULL;
	
	foreach(lc, fscan->fdw_exprs)
	{
		extractRestrictions(bms_make_singleton(fscan->scan.scanrelid),
							((Expr *) lfirst(lc)),
							&execstate->qual_list);
	}
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

	Assert(slot != NULL);
	
	if (execstate->tt_tupleDescriptor !=
	    slot->tts_tupleDescriptor)
	{
		if (execstate->tt_tupleDescriptor != NULL)
		{
			if (errstart(WARNING, __FILE__,
				     __LINE__, PG_FUNCNAME_MACRO,
				     TEXTDOMAIN))
			{
				errmsg("tupleDescriptor Changed");
				errdetail("Reallocing and reintializec cinfo struct may be a performance hit.");
			}
			errfinish(0);
		}

		
		execstate->tt_tupleDescriptor = slot->tts_tupleDescriptor;

		if (execstate->cinfos  != NULL) {
			execstate->cinfos = repalloc(execstate->cinfos,
						     sizeof(ConversionInfo *) *
						     slot->tts_tupleDescriptor->natts);
		} else {
			execstate->cinfos = palloc(sizeof(ConversionInfo *) *
						   slot->tts_tupleDescriptor->natts);
		}
		memset(execstate->cinfos,
		       0,
		       sizeof(ConversionInfo *) *
		       slot->tts_tupleDescriptor->natts);
		if (execstate->values != NULL) {
			execstate->values = repalloc(execstate->values,
						     sizeof(Datum) *
						     slot->tts_tupleDescriptor->natts);
		} else {
			execstate->values = palloc(sizeof(Datum) *
						   slot->tts_tupleDescriptor->natts);
		}


		if (execstate->nulls != NULL) {
			execstate->nulls = repalloc(execstate->nulls,
						    sizeof(bool) *
						    slot->tts_tupleDescriptor->natts);
		} else {
			execstate->nulls = palloc(sizeof(bool) *
						  slot->tts_tupleDescriptor->natts);
		}
		initConversioninfo(execstate->cinfos,
				   TupleDescGetAttInMetadata(execstate->tt_tupleDescriptor));
	}

	if (execstate->p_iterator == NULL)
	{
		execute(node, NULL);
	}
	ExecClearTuple(slot);
	if (execstate->p_iterator == Py_None)
	{
		/* No iterator returned from get_iterator */
		Py_DECREF(execstate->p_iterator);
		return slot;
	}
	p_value = PyIter_Next(execstate->p_iterator);
	errorCheck();
	/* A none value results in an empty slot. */
	if (p_value == NULL || p_value == Py_None)
	{
		Py_XDECREF(p_value);
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
	PyObject   *result = PYOBJECT_CALLMETHOD(state->fdw_instance, "end_scan", "()");

	errorCheck();
	Py_DECREF(result);
	Py_DECREF(state->fdw_instance);
	Py_XDECREF(state->p_iterator);
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
	TargetEntry *tle, *returningTle;
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
		Form_pg_attribute att = TupleDescAttr(desc, i);

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
		Form_pg_attribute att = TupleDescAttr(desc, i);

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
	PyObject   *p_new_value = PYOBJECT_CALLMETHOD(fdw_instance, "insert", "(O)", values);

	errorCheck();
	if (p_new_value && p_new_value != Py_None)
	{
		// XXXXXX FIXME: If there is no result tuple, this assumes
		// that the given slot matches the table.
		// This does not appear to be the case.
		ExecClearTuple(slot);
		pythonResultToTuple(p_new_value, slot, modstate->cinfos, modstate->buffer);
		ExecStoreVirtualTuple(slot);
	}
	Py_XDECREF(p_new_value);
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
	p_new_value = PYOBJECT_CALLMETHOD(fdw_instance, "delete", "(O)", p_row_id);
	errorCheck();
	if (p_new_value == NULL || p_new_value == Py_None)
	{
		Py_XDECREF(p_new_value);
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
	p_new_value = PYOBJECT_CALLMETHOD(fdw_instance, "update", "(O,O)", p_row_id,
									  p_value);
	errorCheck();
	if (p_new_value != NULL && p_new_value != Py_None)
	{
		ExecClearTuple(slot);
		pythonResultToTuple(p_new_value, slot, modstate->cinfos, modstate->buffer);
		ExecStoreVirtualTuple(slot);
	}
	Py_XDECREF(p_new_value);
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
	PyObject   *result = PYOBJECT_CALLMETHOD(modstate->fdw_instance, "end_modify", "()");

	errorCheck();
	Py_DECREF(modstate->fdw_instance);
	Py_DECREF(result);
}

/*
 * Callback used to propagate a subtransaction end.
 */
static void
multicorn_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						   SubTransactionId parentSubid, void *arg)
{
	PyObject   *instance;
	int			curlevel;
	HASH_SEQ_STATUS status;
	CacheEntry *entry;

	/* Nothing to do after commit or subtransaction start. */
	if (event == SUBXACT_EVENT_COMMIT_SUB || event == SUBXACT_EVENT_START_SUB)
		return;

	curlevel = GetCurrentTransactionNestLevel();

	hash_seq_init(&status, InstancesHash);

	while ((entry = (CacheEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->xact_depth < curlevel)
			continue;

		instance = entry->value;
		if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		{
			PYOBJECT_CALLMETHOD(instance, "sub_commit", "(i)", curlevel);
		}
		else
		{
			PYOBJECT_CALLMETHOD(instance, "sub_rollback", "(i)", curlevel);
		}
		errorCheck();
		entry->xact_depth--;
	}
}
#endif

/*
 * Callback used to propagate pre-commit / commit / rollback.
 */
static void
multicorn_xact_callback(XactEvent event, void *arg)
{
	PyObject   *instance;
	HASH_SEQ_STATUS status;
	CacheEntry *entry;
	
	hash_seq_init(&status, InstancesHash);
	while ((entry = (CacheEntry *) hash_seq_search(&status)) != NULL)
	{
		instance = entry->value;
		if (entry->xact_depth == 0)
			continue;

		switch (event)
		{
#if PG_VERSION_NUM >= 90300
			case XACT_EVENT_PRE_COMMIT:
				PYOBJECT_CALLMETHOD(instance, "pre_commit", "()");
				break;
#endif
			case XACT_EVENT_COMMIT:
				PYOBJECT_CALLMETHOD(instance, "commit", "()");
				entry->xact_depth = 0;
				break;
			case XACT_EVENT_ABORT:
				PYOBJECT_CALLMETHOD(instance, "rollback", "()");
				entry->xact_depth = 0;
				break;
			default:
				break;
		}
		errorCheck();
	}
}

/*
 * Callback for after commit to relase any locks or other
 * resources.  Key thing is locks have already been released.
 * This allows updates/inserts back to postgres without worrying 
 * about locks.  Can be a big performance win in a few
 * corner cases.
 */
static void
multicorn_release_callback(ResourceReleasePhase phase, bool isCommit,
			   bool isTopLevel, void *arg) {
	PyObject   *instance;
	HASH_SEQ_STATUS status;
	CacheEntry *entry;

	if (!isTopLevel) {
	  return;
	}

	hash_seq_init(&status, InstancesHash);
	while ((entry = (CacheEntry *) hash_seq_search(&status)) != NULL)
	{
		instance = entry->value;
		if (entry->xact_depth == 0)
			continue;

		switch (phase)
		{
		  /* XXXXX FIXME:
		   * Do we want to pass isCommit?
		   */
		case RESOURCE_RELEASE_BEFORE_LOCKS:
		  PYOBJECT_CALLMETHOD(instance, "release_before", "()");
		  break;
		  
		case RESOURCE_RELEASE_LOCKS:
		  PYOBJECT_CALLMETHOD(instance, "release", "()");
		  break;

		case RESOURCE_RELEASE_AFTER_LOCKS:
		  PYOBJECT_CALLMETHOD(instance, "release_after", "()");
		  break;
		  
		default:
		  break;
		}
		errorCheck();
	}
	
}


#if PG_VERSION_NUM >= 90500
static List *
multicornImportForeignSchema(ImportForeignSchemaStmt * stmt,
							 Oid serverOid)
{
	List	   *cmds = NULL;
	List	   *options = NULL;
	UserMapping *mapping;
	ForeignServer *f_server;
	char	   *restrict_type = NULL;
	PyObject   *p_class = NULL;
	PyObject   *p_tables,
			   *p_srv_options,
			   *p_options,
			   *p_restrict_list,
			   *p_iter,
			   *p_item;
	ListCell   *lc;

	f_server = GetForeignServer(serverOid);
	foreach(lc, f_server->options)
	{
		DefElem    *option = (DefElem *) lfirst(lc);

		if (strcmp(option->defname, "wrapper") == 0)
		{
			p_class = getClassString(defGetString(option));
			errorCheck();
		}
		else
		{
			options = lappend(options, option);
		}
	}
	mapping = multicorn_GetUserMapping(GetUserId(), serverOid);
	if (mapping)
		options = list_concat(options, mapping->options);

	if (p_class == NULL)
	{
		/*
		 * This should never happen, since we validate the wrapper parameter
		 * at
		 */
		/* object creation time. */
		ereport(ERROR, (errmsg("%s", "The wrapper parameter is mandatory, specify a valid class name")));
	}
	switch (stmt->list_type)
	{
		case FDW_IMPORT_SCHEMA_LIMIT_TO:
			restrict_type = "limit";
			break;
		case FDW_IMPORT_SCHEMA_EXCEPT:
			restrict_type = "except";
			break;
		case FDW_IMPORT_SCHEMA_ALL:
			break;
	}
	p_srv_options = optionsListToPyDict(options);
	p_options = optionsListToPyDict(stmt->options);
	p_restrict_list = PyList_New(0);
	foreach(lc, stmt->table_list)
	{
		RangeVar   *rv = (RangeVar *) lfirst(lc);
		PyObject   *p_tablename = PyUnicode_Decode(
											rv->relname, strlen(rv->relname),
												   getPythonEncodingName(),
												   NULL);

		errorCheck();
		PyList_Append(p_restrict_list, p_tablename);
		Py_DECREF(p_tablename);
	}
	errorCheck();
	p_tables = PYOBJECT_CALLMETHOD(p_class, "import_schema", "(s, O, O, s, O)",
							   stmt->remote_schema, p_srv_options, p_options,
								   restrict_type, p_restrict_list);
	errorCheck();
	Py_DECREF(p_class);
	Py_DECREF(p_options);
	Py_DECREF(p_srv_options);
	Py_DECREF(p_restrict_list);
	errorCheck();
	p_iter = PyObject_GetIter(p_tables);
	while ((p_item = PyIter_Next(p_iter)))
	{
		PyObject   *p_string;
		char	   *value;

		p_string = PYOBJECT_CALLMETHOD(p_item, "to_statement", "(s,s)",
					       stmt->local_schema,
					       f_server->servername);
		errorCheck();
		value = PyString_AsString(p_string);
		errorCheck();
		cmds = lappend(cmds, pstrdup(value));
		Py_DECREF(p_string);
		Py_DECREF(p_item);
	}
	errorCheck();
	Py_DECREF(p_iter);
	Py_DECREF(p_tables);
	return cmds;
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
						  -1, InvalidOid, 4, Int32GetDatum(state->numattrs), false, true));
	result = lappend(result, makeConst(INT4OID,
					-1, InvalidOid, 4, Int32GetDatum(state->foreigntableid), false, true));
	result = lappend(result, state->target_list);

	result = lappend(result, serializeDeparsedSortGroup(state->pathkeys));

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
	Oid			foreigntableid = ((Const *) lsecond(values))->constvalue;
	List		*pathkeys;

	/* Those list must be copied, because their memory context can become */
	/* invalid during the execution (in particular with the cursor interface) */
	execstate->target_list = copyObject(lthird(values));
	pathkeys = lfourth(values);
	execstate->pathkeys = deserializeDeparsedSortGroup(pathkeys);
	execstate->fdw_instance = getInstance(foreigntableid);
	execstate->buffer = makeStringInfo();
	execstate->cinfos = NULL;
	execstate->values = NULL;
	execstate->nulls = NULL;
	execstate->tt_tupleDescriptor = NULL;
	return execstate;
}


static int multicorn_SPI_depth = 0;
static bool multicorn_SPI_connected = false;

void
multicorn_connect(void) {

	if (multicorn_SPI_depth == 0)
	{
		if (errstart(FATAL, __FILE__, __LINE__,
			     PG_FUNCNAME_MACRO, TEXTDOMAIN))
		{
			errmsg("Attempting to connect to SPI without wrapper");
			errfinish(0);
		}
		return;
		
	}

	if (!multicorn_SPI_connected)
	{
		if (SPI_connect() != SPI_OK_CONNECT)
		{
			if (errstart(FATAL, __FILE__, __LINE__,
				     PG_FUNCNAME_MACRO, TEXTDOMAIN))
			{
				errmsg("Unable to connect to SPI");
				errfinish(0);
			}
			return;
		}
		multicorn_SPI_connected = true;
	}
}

/*
 * Pass through the pyobject so we can easily
 * have a macro call this for every
 * PyObject_CallMethod and PyObject_CallFunction
 * call.
 */
PyObject *
multicorn_spi_leave(PyObject *po) {

	if (--multicorn_SPI_depth == 0 && multicorn_SPI_connected)
	{
		multicorn_SPI_connected = false;
		//SPI_finish();
	}
	return po;
}

PyObject *
multicorn_spi_enter(PyObject *po) {
	multicorn_SPI_depth++;
	return po;
}
