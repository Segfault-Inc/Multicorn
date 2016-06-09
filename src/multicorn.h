#include "Python.h"
#include "postgres.h"
#include "access/relscan.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "access/xact.h"


#ifndef PG_MULTICORN_H
#define PG_MULTICORN_H

/* Data structures */

typedef struct CacheEntry
{
	Oid			hashkey;
	PyObject   *value;
	List	   *options;
	List	   *columns;
	int			xact_depth;
	/* Keep the "options" and "columns" in a specific context to avoid leaks. */
	MemoryContext cacheContext;
}	CacheEntry;


typedef struct ConversionInfo
{
	char	   *attrname;
	FmgrInfo   *attinfunc;
	FmgrInfo   *attoutfunc;
	Oid			atttypoid;
	Oid			attioparam;
	int32		atttypmod;
	int			attnum;
	bool		is_array;
	int			attndims;
	bool		need_quote;
}	ConversionInfo;


typedef struct MulticornPlanState
{
	Oid			foreigntableid;
	AttrNumber	numattrs;
	PyObject   *fdw_instance;
	List	   *target_list;
	List	   *qual_list;
	int			startupCost;
	ConversionInfo **cinfos;
	List	   *pathkeys; /* list of MulticornDeparsedSortGroup) */
}	MulticornPlanState;

typedef struct MulticornExecState
{
	/* instance and iterator */
	PyObject   *fdw_instance;
	PyObject   *p_iterator;
	/* Information carried from the plan phase. */
	List	   *target_list;
	List	   *qual_list;
	Datum	   *values;
	bool	   *nulls;
	ConversionInfo **cinfos;
	/* Common buffer to avoid repeated allocations */
	StringInfo	buffer;
	AttrNumber	rowidAttno;
	char	   *rowidAttrName;
	List	   *pathkeys; /* list of MulticornDeparsedSortGroup) */
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
    List* update_columns_list;
}	MulticornModifyState;


typedef struct MulticornBaseQual
{
	AttrNumber	varattno;
	NodeTag		right_type;
	Oid			typeoid;
	char	   *opname;
	bool		isArray;
	bool		useOr;
}	MulticornBaseQual;

typedef struct MulticornConstQual
{
	MulticornBaseQual base;
	Datum		value;
	bool		isnull;
}	MulticornConstQual;

typedef struct MulticornVarQual
{
	MulticornBaseQual base;
	AttrNumber	rightvarattno;
}	MulticornVarQual;

typedef struct MulticornParamQual
{
	MulticornBaseQual base;
	Expr	   *expr;
}	MulticornParamQual;

typedef struct MulticornDeparsedSortGroup
{
	Name 			attname;
	int				attnum;
	bool			reversed;
	bool			nulls_first;
	Name			collate;
	PathKey	*key;
} MulticornDeparsedSortGroup;

extern HTAB	   *InstancesHash;

void _PG_init(void);
void _PG_fini(void);
Datum multicorn_handler(PG_FUNCTION_ARGS);
Datum multicorn_validator(PG_FUNCTION_ARGS);

void * serializePlanState(MulticornPlanState * state);
MulticornExecState * initializeExecState(void *internalstate);

#endif   /* PG_MULTICORN_H */
