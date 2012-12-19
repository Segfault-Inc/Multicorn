#include "Python.h"
#include "postgres.h"
#include "nodes/pg_list.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "access/relscan.h"
#include "nodes/makefuncs.h"
#include "nodes/bitmapset.h"

#ifndef PG_MULTICORN_H
#define PG_MULTICORN_H


/* Data structures */

typedef struct ConversionInfo
{
	char	   *attrname;
	const char *encodingname;
	FmgrInfo   *attinfunc;
	Oid			atttypoid;
	Oid			attioparam;
	int32		atttypmod;
	int			attnum;
}	ConversionInfo;


typedef struct MulticornPlanState
{
	Oid			foreigntableid;
	AttrNumber	rowid_attnum;
	AttrNumber	numattrs;
	PyObject   *fdw_instance;
	List	   *target_list;
	List	   *qual_list;
	List	   *param_list;
	ConversionInfo **cinfos;
}	MulticornPlanState;

typedef struct MulticornExecState
{
	/* instance and iterator */
	PyObject   *fdw_instance;
	PyObject   *p_iterator;
	/* Information carried from the plan phase. */
	List	   *target_list;
	List	   *qual_list;
	List	   *param_list;
	AttInMetadata *attinmeta;
	Datum	   *values;
	bool	   *nulls;
	AttrNumber	numattrs;
	ConversionInfo **cinfos;
	/* Common buffer to avoid repeated allocations */
	StringInfo	buffer;
}	MulticornExecState;

/*	errors.c */
void		errorCheck(void);
bool		try_except(char *exceptionname);

/* python.c */
PyObject   *getInstance(Oid foreigntableid);
PyObject   *qualToPyObject(Expr *expr, PlannerInfo *root);
PyObject   *getClassString(char *className);
PyObject   *execute(ForeignScanState *state);
void pythonResultToTuple(PyObject *p_value,
					MulticornExecState * state);

void getRelSize(MulticornPlanState * state,
		   PlannerInfo *root,
		   double *rows,
		   int *width);

List	   *pathKeys(MulticornPlanState * state);


/* query.c */
void extractRestrictions(PlannerInfo *root,
					RelOptInfo *baserel,
					Expr *node,
					List **quals,
					List **params);
List	   *extractColumns(PlannerInfo *root, RelOptInfo *baserel);
void initConversioninfo(ConversionInfo ** cinfo,
				   AttInMetadata *attinmeta);
Value	   *colnameFromVar(Var *var, PlannerInfo *root);

void		findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths);

#endif   /* PG_MULTICORN_H */
