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

typedef struct MulticornState
{
	AttInMetadata *attinmeta;
	PyObject   *quals;
	int			rownum;
	PyObject   *pIterator;
}	MulticornState;

extern Datum multicorn_handler(PG_FUNCTION_ARGS);
extern Datum multicorn_validator(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(multicorn_handler);
PG_FUNCTION_INFO_V1(multicorn_validator);

/*
 * FDW functions declarations
 */
static FdwPlan *multicorn_plan(Oid foreign_table_id, PlannerInfo * root, RelOptInfo * base_relation);
static void multicorn_explain(ForeignScanState * node, ExplainState * es);
static void multicorn_begin(ForeignScanState * node, int eflags);
static TupleTableSlot *multicorn_iterate(ForeignScanState * node);
static void multicorn_rescan(ForeignScanState * node);
static void multicorn_end(ForeignScanState * node);

/*
   Helpers
   */
static void multicorn_error_check(void);
static void init_if_needed(void);
void		multicorn_get_options(Oid foreign_table_id, PyObject * options_dict, char **module);
void		multicorn_get_attributes_def(TupleDesc desc, PyObject * dict);
void		multicorn_extract_conditions(ForeignScanState * node, PyObject * list);
PyObject   *multicorn_datum_to_python(Datum datumvalue, Oid type, Form_pg_attribute attribute);
void		multicorn_report_exception(PyObject * pErrType, PyObject * pErrValue, PyObject * pErrTraceback);
PyObject   *multicorn_get_instance(Relation rel);
HeapTuple	BuildTupleFromCStringsWithSize(AttInMetadata * attinmeta, char **values, ssize_t * sizes);
void		multicorn_get_columns(List * columnlist, TupleDesc desc, PyObject *);
void		multicorn_get_column(Expr * expr, TupleDesc desc, PyObject * list);
const char *get_encoding_from_attribute(Form_pg_attribute attribute);
void		multicorn_execute(ForeignScanState * node);
void		multicorn_clean_state(MulticornState * state);
void		multicorn_get_param(Node * left, Node * right, ForeignScanState * fss, Form_pg_operator operator, PyObject ** result);
PyObject   *multicorn_get_class(char *className);
PyObject   *multicorn_get_multicorn(void);
void		multicorn_unnest(Node * value, Node ** result);

HeapTuple	pysequence_to_postgres_tuple(TupleDesc desc, PyObject * pyseq);
HeapTuple	pydict_to_postgres_tuple(TupleDesc desc, PyObject * pydict);
ssize_t		pyobject_to_cstring(PyObject * pyobject, Form_pg_attribute attribute, char **buffer);

const char *DATE_FORMAT_STRING = "%Y-%m-%d";

PyObject   *TABLES_DICT;

Datum
multicorn_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdw_routine = makeNode(FdwRoutine);

	fdw_routine->PlanForeignScan = multicorn_plan;
	fdw_routine->ExplainForeignScan = multicorn_explain;
	fdw_routine->BeginForeignScan = multicorn_begin;
	fdw_routine->IterateForeignScan = multicorn_iterate;
	fdw_routine->ReScanForeignScan = multicorn_rescan;
	fdw_routine->EndForeignScan = multicorn_end;

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

	init_if_needed();
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

static FdwPlan *
multicorn_plan(Oid foreign_table_id,
			   PlannerInfo * root,
			   RelOptInfo * base_relation)
{
	FdwPlan    *fdw_plan;

	fdw_plan = makeNode(FdwPlan);
	fdw_plan->startup_cost = 10;
	base_relation->rows = 9999999;
	fdw_plan->total_cost = 15;
	return fdw_plan;
}

static void
multicorn_explain(ForeignScanState * node, ExplainState * es)
{
	/* TODO: calculate real values */
	ExplainPropertyText("Foreign multicorn", "multicorn", es);

	if (es->costs)
	{
		ExplainPropertyLong("Foreign multicorn cost", 10.5, es);
	}
}

static void
multicorn_begin(ForeignScanState * node, int eflags)
{
	/* TODO: do things if necessary */
	AttInMetadata *attinmeta;
	MulticornState *state;

	init_if_needed();
	attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
	state = (MulticornState *) palloc(sizeof(MulticornState));
	state->rownum = 0;
	state->attinmeta = attinmeta;
	state->pIterator = NULL;
	node->fdw_state = (void *) state;
}

void
multicorn_execute(ForeignScanState * node)
{
	PyObject   *pArgs,
			   *pValue,
			   *pObj,
			   *pMethod,
			   *pConds,
			   *colList;

	MulticornState *state = node->fdw_state;
	Relation	rel = node->ss.ss_currentRelation;

	pObj = multicorn_get_instance(rel);
	Py_INCREF(pObj);
	pArgs = PyTuple_New(2);
	pConds = PyList_New(0);
	multicorn_extract_conditions(node, pConds);
	PyTuple_SetItem(pArgs, 0, pConds);
	colList = PySet_New(NULL);
	Py_INCREF(colList);
	multicorn_get_columns(node->ss.ps.targetlist, rel->rd_att, colList);
	multicorn_get_columns(node->ss.ps.qual, rel->rd_att, colList);
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
}


static TupleTableSlot *
multicorn_iterate(ForeignScanState * node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	MulticornState *state = (MulticornState *) node->fdw_state;
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
multicorn_rescan(ForeignScanState * node)
{
	multicorn_clean_state((MulticornState *) node->fdw_state);
}

static void
multicorn_end(ForeignScanState * node)
{
	multicorn_clean_state((MulticornState *) node->fdw_state);
}

static void
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
multicorn_get_options(Oid foreign_table_id, PyObject * pOptions, char **module)
{
	ForeignTable *f_table;
	ForeignServer *f_server;
	UserMapping *mapping;
	List	   *options;
	ListCell   *lc;
	bool		got_module = false;
	PyObject   *pStr;

	f_table = GetForeignTable(foreign_table_id);
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
pydict_to_postgres_tuple(TupleDesc desc, PyObject * pydict)
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
pysequence_to_postgres_tuple(TupleDesc desc, PyObject * pyseq)
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
pyobject_to_cstring(PyObject * pyobject, Form_pg_attribute attribute, char **buffer)
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

		formatted_date = PyObject_CallMethod(pyobject, "strftime", "(s)", DATE_FORMAT_STRING);
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
multicorn_get_attributes_def(TupleDesc desc, PyObject * dict)
{
	char	   *key, *typname;
	HeapTuple	typeTuple;
	Form_pg_type typeStruct;
	Py_ssize_t	i,
				natts;
	natts = desc->natts;
	for (i = 0; i < natts; i++)
	{
        typeTuple = SearchSysCache1(TYPEOID,
                                    desc->attrs[i]->atttypid);
        if (!HeapTupleIsValid(typeTuple))
            elog(ERROR, "lookup failed for type %u",
                 desc->attrs[i]->atttypid);
        typeStruct = (Form_pg_type) GETSTRUCT(typeTuple);
        ReleaseSysCache(typeTuple);
        typname = NameStr(typeStruct->typname);
		key = NameStr(desc->attrs[i]->attname);
		PyDict_SetItem(dict, PyString_FromString(key), PyString_FromString(typname));
	}
}

void
multicorn_unnest(Node * node, Node ** result)
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

void
multicorn_get_param(Node * left, Node * right, ForeignScanState * fss, Form_pg_operator operator, PyObject ** result)
{
	HeapTuple	tp;
	ExprState  *exprstate;
	Form_pg_attribute attr;
	char	   *key;
	bool		isnull;
	TupleDesc	tupdesc = fss->ss.ss_currentRelation->rd_att;
	PyObject   *value = NULL;
	PyObject   *qual_class = multicorn_get_class("multicorn.Qual");
	Datum		exprvalue;
	Node	   *normalized_left;
	Node	   *normalized_right;

	multicorn_error_check();
	multicorn_unnest(left, &normalized_left);
	multicorn_unnest(right, &normalized_right);
	if (IsA(normalized_right, Var) && !IsA(normalized_left, Var))
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
		multicorn_error_check();
		attr = tupdesc->attrs[((Var *) normalized_left)->varattno - 1];
		key = NameStr(attr->attname);
		multicorn_error_check();
		switch (normalized_right->type)
		{
			case T_Const:
				value = multicorn_datum_to_python(((Const *) normalized_right)->constvalue, ((Const *) normalized_right)->consttype, attr);
				break;
			case T_Param:
				exprstate = ExecInitExpr((Expr *) normalized_right, &(fss->ss.ps));
				exprvalue = ExecEvalExpr(exprstate, fss->ss.ps.ps_ExprContext, &isnull, NULL);
				if (isnull)
				{
					value = Py_None;
				}
				else
				{
					value = multicorn_datum_to_python(exprvalue, ((Param *) normalized_right)->paramtype, attr);
				}
				break;
			default:
				elog(WARNING, "Cant manage type %i", normalized_right->type);
				break;
		}
	}
	if (value)
	{
		*result = PyObject_CallObject(qual_class, Py_BuildValue("(s,s,O)", key, NameStr(operator->oprname), value));
		multicorn_error_check();
	}
}

void
multicorn_extract_conditions(ForeignScanState * node, PyObject * list)
{
	if (node->ss.ps.plan->qual)
	{
		ListCell   *lc;
		List	   *quals = list_copy(node->ss.ps.qual);
		PyObject   *tempqual;
		HeapTuple	tp;
		Node	   *left,
				   *right;
		Form_pg_operator operator_tup;

		foreach(lc, quals)
		{
			Node	   *nodexp = (Node *) ((ExprState *) lfirst(lc))->expr;

			tempqual = NULL;
			if (nodexp != NULL)
			{
				switch (nodexp->type)
				{
					case T_OpExpr:
						if (list_length(((OpExpr *) nodexp)->args) == 2)
						{
							OpExpr	   *op = (OpExpr *) nodexp;

							left = list_nth(op->args, 0);
							right = list_nth(op->args, 1);
							tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
							if (!HeapTupleIsValid(tp))
								elog(ERROR, "cache lookup failed for operator %u", op->opno);
							operator_tup = (Form_pg_operator) GETSTRUCT(tp);
							ReleaseSysCache(tp);
							multicorn_get_param(left, right, node, operator_tup, &tempqual);
							if (tempqual)
							{
								PyList_Append(list, tempqual);
							}
						}
						break;
					case T_ScalarArrayOpExpr:
						if (list_length(((ScalarArrayOpExpr *) nodexp)->args) == 2)
						{
							ScalarArrayOpExpr *op = (ScalarArrayOpExpr *) nodexp;

							left = list_nth(op->args, 0);
							right = list_nth(op->args, 1);
							tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
							if (!HeapTupleIsValid(tp))
								elog(ERROR, "cache lookup failed for operator %u", op->opno);
							operator_tup = (Form_pg_operator) GETSTRUCT(tp);
							ReleaseSysCache(tp);

							/*
							 * Build the qual "normally" and set the operator
							 * to a tuple instead
							 */
							multicorn_get_param(left, right, node, operator_tup, &tempqual);
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
									PyList_Append(list, tempqual);
								}
							}
						}
						break;
					case T_NullTest:
						/* TODO: this code is pretty much duplicated from */
						/* get_param, find a way to share it. */
						if IsA
							(((NullTest *) nodexp)->arg, Var)
						{
							char	   *operator_name;
							NullTest   *nulltest = (NullTest *) nodexp;
							TupleDesc	tupdesc = node->ss.ss_currentRelation->rd_att;
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
							PyList_Append(list, PyObject_CallObject(qual_class,
																	Py_BuildValue("(s,s,O)", NameStr(attr->attname), operator_name, Py_None)));
						}
						break;
					default:
						elog(WARNING, "GOT AN UNEXPECTED TYPE: %i", nodexp->type);
						break;
				}
			}
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
				bool		isnull;
				Datum		buffer;
				PyObject   *listelem;
				ArrayIterator iterator;

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
multicorn_report_exception(PyObject * pErrType, PyObject * pErrValue, PyObject * pErrTraceback)
{
	char	   *errName,
			   *errValue;
	PyObject   *traceback_list;
	PyObject   *tracebackModule = PyImport_Import(PyString_FromString("traceback"));
	PyObject   *format_exception = PyObject_GetAttrString(tracebackModule, "format_exception");
	PyObject   *newline = PyString_FromString("\n");

	PyErr_NormalizeException(&pErrType, &pErrValue, &pErrTraceback);
	errName = PyString_AsString(PyObject_GetAttrString(pErrType, "__name__"));
	errValue = PyString_AsString(PyObject_Str(pErrValue));
	traceback_list = PyObject_CallObject(format_exception, Py_BuildValue("(O,O,O)", pErrType, pErrValue, pErrTraceback));
	ereport(ERROR, (errmsg("Error in python: %s", errName),
					errdetail("%s", errValue),
					errdetail_log("%s", PyString_AsString(PyObject_CallObject(PyObject_GetAttrString(newline, "join"), Py_BuildValue("(O)", traceback_list))))));
}

static void
init_if_needed()
{
	if (TABLES_DICT == NULL)
	{
		/* TODO: managed locks and things */
		Py_Initialize();
		PyDateTime_IMPORT;
		TABLES_DICT = PyDict_New();
	}
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
			   *pTableId;
	Oid			tablerelid;
	char	   *module;

	init_if_needed();
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
		pColumns = PyDict_New();
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
BuildTupleFromCStringsWithSize(AttInMetadata * attinmeta, char **values, ssize_t * sizes)
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
multicorn_get_columns(List * columnslist, TupleDesc desc, PyObject * result)
{
	Expr	   *current_expr;
	ListCell   *cell;

	foreach(cell, columnslist)
	{
		current_expr = (Expr *) lfirst(cell);
		multicorn_get_column(current_expr, desc, result);
	}
}

void
multicorn_get_column(Expr * expr, TupleDesc desc, PyObject * list)
{
	ListCell   *cell;
	char	   *key;

	if (expr == NULL)
	{
		return;
	}
	switch (expr->type)
	{

		case T_ExprState:
			multicorn_get_column(((ExprState *) expr)->expr, desc, list);
			break;

		case T_GenericExprState:
			multicorn_get_column(((GenericExprState *) expr)->xprstate.expr, desc, list);
			break;

		case T_FuncExprState:
		case T_ScalarArrayOpExprState:
			foreach(cell, ((FuncExprState *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

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
		case T_ScalarArrayOpExpr:
		case T_FuncExpr:
			foreach(cell, ((OpExpr *) expr)->args)
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

		case T_NullTestState:
			multicorn_get_column((Expr *) (((NullTestState *) expr)->arg), desc, list);
			break;

		case T_CoerceViaIOState:
			multicorn_get_column((Expr *) (((CoerceViaIOState *) expr)->arg), desc, list);
			break;

		case T_BooleanTest:
			multicorn_get_column(((BooleanTest *) expr)->arg, desc, list);
			break;

		case T_CoerceToDomain:
			multicorn_get_column(((CoerceToDomain *) expr)->arg, desc, list);
			break;

		case T_SubPlanState:
			multicorn_get_column(((Expr *) ((SubPlanState *) expr)->testexpr), desc, list);
			foreach(cell, ((SubPlanState *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		case T_ArrayCoerceExprState:
			multicorn_get_column(((Expr *) ((ArrayCoerceExprState *) expr)->arg), desc, list);
			break;

		case T_BoolExprState:
			foreach(cell, ((BoolExprState *) expr)->args)
			{
				multicorn_get_column((Expr *) lfirst(cell), desc, list);
			}
			break;

		default:
			ereport(ERROR,
					(errmsg("Unknown node type %d", expr->type)));
	}
}

void
multicorn_clean_state(MulticornState * state)
{
	if (state->pIterator)
	{
		Py_DECREF(state->pIterator);
		state->pIterator = NULL;
	}
}
