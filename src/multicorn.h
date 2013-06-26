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
	AttrNumber	numattrs;
	PyObject   *fdw_instance;
	List	   *target_list;
	List	   *qual_list;
	List	   *param_list;
	int			startupCost;
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
	Datum	   *values;
	bool	   *nulls;
	ConversionInfo **cinfos;
	/* Common buffer to avoid repeated allocations */
	StringInfo	buffer;
	AttrNumber	rowidAttno;
	char	   *rowidAttrName;
}	MulticornExecState;

typedef struct MulticornModifyState
{
	ConversionInfo **cinfos;
	ConversionInfo **resultCinfos;
	PyObject   *fdw_instance;
	StringInfo	buffer;
	AttrNumber	rowidAttno;
	char	   *rowidAttrName;
	ConversionInfo *rowidCinfo;
}	MulticornModifyState;

/* errors.c */
void		errorCheck(void);
bool		try_except(char *exceptionname);

/* python.c */
PyObject   *pgstringToPyUnicode(const char *string);
char *     *pyUnicodeToPgString(PyObject* pyobject);

PyObject   *getInstance(Oid foreigntableid);
PyObject   *qualToPyObject(Expr *expr, PlannerInfo *root);
PyObject   *getClassString(char *className);
PyObject   *execute(ForeignScanState *state);
void pythonResultToTuple(PyObject *p_value,
					TupleTableSlot *slot,
					ConversionInfo ** cinfos,
					StringInfo buffer);
PyObject   *tupleTableSlotToPyObject(TupleTableSlot *slot, ConversionInfo ** cinfos);
char	   *getRowIdColumn(PyObject *fdw_instance);

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
List	   *extractColumns(List *reltargetlist, List *restrictinfolist);
void initConversioninfo(ConversionInfo ** cinfo,
				   AttInMetadata *attinmeta);

Value *colnameFromVar(Var *var, PlannerInfo *root,
			   MulticornPlanState * state);

void		findPaths(PlannerInfo *root, RelOptInfo *baserel, List *possiblePaths, int startupCost);

PyObject   *datumToPython(Datum node, Oid typeoid, ConversionInfo * cinfo);

#endif   /* PG_MULTICORN_H */

char * PyUnicode_AsPgString(PyObject* p_unicode);

#if PY_MAJOR_VERSION >= 3
PyObject * PyString_FromString(const char* s);
PyObject * PyString_FromStringAndSize(const char* s, Py_ssize_t size);
char * PyString_AsString(PyObject *unicode);
int PyString_AsStringAndSize(PyObject *unicode, char** tempbuffer, Py_ssize_t *length);
#endif

