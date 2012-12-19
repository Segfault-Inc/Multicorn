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
#include "access/reloptions.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "access/relscan.h"


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
						List *scan_clauses);
static void multicornExplainForeignScan(ForeignScanState *node,
							ExplainState *es);
static void multicornBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *multicornIterateForeignScan(ForeignScanState *node);
static void multicornReScanForeignScan(ForeignScanState *node);
static void multicornEndForeignScan(ForeignScanState *node);

/*	Helpers functions */
void	   *serializePlanState(MulticornPlanState * planstate);
MulticornExecState *initializeExecState(void *internal_plan_state);

void
_PG_init()
{
	Py_Initialize();
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
	planstate->foreigntableid = foreigntableid;
	/* Initialize the conversion info array */
	{
		Relation	rel = RelationIdGetRelation(ftable->relid);
		AttInMetadata *attinmeta = TupleDescGetAttInMetadata(rel->rd_att);

		planstate->numattrs = RelationGetNumberOfAttributes(rel);
		planstate->cinfos = palloc0(sizeof(ConversionInfo *) *
									planstate->numattrs);
		initConversioninfo(planstate->cinfos, attinmeta);
		RelationClose(rel);
	}

	planstate->fdw_instance = getInstance(foreigntableid);
	/* Pull "var" clauses to build an appropriate target list */
	foreach(lc, extractColumns(root, baserel))
	{
		Var		   *var = (Var *) lfirst(lc);

		/* Store only a Value node containing the string name of the column. */
		planstate->target_list = lappend(planstate->target_list,
										 colnameFromVar(var, root));
	}
	foreach(lc, baserel->baserestrictinfo)
	{
		extractRestrictions(root, baserel, (RestrictInfo *) lfirst(lc),
							&planstate->qual_list,
							&planstate->param_list);

	}
	/* Extract the restrictions from the plan. */
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

	findPaths(root, baserel, possiblePaths);
	/* Add a default path */
	path = (Path *) create_foreignscan_path(root, baserel,
											baserel->rows,
											baserel->baserestrictcost.startup,
											baserel->rows * baserel->width,
											NIL,		/* no pathkeys */
											NULL,		/* no outer rel either */
											(void *) baserel->fdw_private);

	add_path(baserel, path);
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
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	// Extract the the quals coming from a parameterized path, if any
	if(best_path->path.param_info)
	{
		ListCell *lc;
		foreach(lc, best_path->path.param_info->ppi_clauses)
		{
			extractRestrictions(root, baserel, (RestrictInfo *) lfirst(lc),
								&planstate->qual_list,
								&planstate->param_list);
		}
	}
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
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
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	MulticornExecState *execstate;

	execstate = initializeExecState(fscan->fdw_private);
	{
		TupleDesc	tupdesc = slot->tts_tupleDescriptor;

		execstate->values = palloc(sizeof(Datum) * tupdesc->natts);
		execstate->nulls = palloc(sizeof(bool) * tupdesc->natts);
		execstate->attinmeta = TupleDescGetAttInMetadata(tupdesc);
	}
	initConversioninfo(execstate->cinfos, execstate->attinmeta);
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
	if (try_except("exceptions.StopIteration"))
	{
		return slot;
	}
	/* A none value results in an empty slot. */
	if (p_value == NULL || p_value == Py_None)
	{
		if (p_value != NULL)
		{
			Py_DECREF(p_value);
		}
		return slot;
	}
	pythonResultToTuple(p_value, execstate);
	slot->tts_values = execstate->values;
	slot->tts_isnull = execstate->nulls;
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

	Py_DECREF(state->fdw_instance);
	if (state->p_iterator != NULL)
	{
		Py_DECREF(state->p_iterator);
	}
	state->p_iterator = NULL;
}


/*
 *	"Serialize" a MulticornPlanState, so that it is safe to be carried
 *	between the plan and the execution safe.
 */
void *
serializePlanState(MulticornPlanState * state)
{
	List	   *result = NULL;

	result = lappend_int(result, state->numattrs);
	result = lappend_int(result, state->foreigntableid);
	result = lappend(result, state->target_list);
	result = lappend(result, state->qual_list);
	result = lappend(result, state->param_list);
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
	AttrNumber	attnum = linitial_int(values);
	Oid			foreigntableid = lsecond_int(values);

	/* Those list must be copied, because their memory context can become */
	/* invalid during the execution (in particular with the cursor interface) */
	execstate->target_list = copyObject(lthird(values));
	execstate->qual_list = copyObject(lfourth(values));
	execstate->param_list = copyObject(list_nth(values, 4));
	execstate->fdw_instance = getInstance(foreigntableid);
	execstate->numattrs = attnum;
	execstate->buffer = makeStringInfo();
	execstate->cinfos = palloc0(sizeof(ConversionInfo *) *
								attnum);
	execstate->values = palloc(attnum * sizeof(Datum));
	execstate->nulls = palloc(execstate->numattrs * sizeof(bool));
	return execstate;
}
