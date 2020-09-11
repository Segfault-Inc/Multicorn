#include <Python.h>
#include "datetime.h"
#include "postgres.h"
#include "multicorn.h"
#include "catalog/pg_user_mapping.h"
#include "access/reloptions.h"
#include "miscadmin.h"
#include "utils/numeric.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/array.h"
#include "utils/catcache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/rel.h"
#include "executor/nodeSubplan.h"
#include "bytesobject.h"
#include "mb/pg_wchar.h"
#include "access/xact.h"
#include "utils/lsyscache.h"


List	   *getOptions(Oid foreigntableid);
bool		compareOptions(List *options1, List *options2);

void		getColumnsFromTable(TupleDesc desc, PyObject **p_columns, List **columns);
bool		compareColumns(List *columns1, List *columns2);

PyObject   *getClass(PyObject *className);
PyObject   *valuesToPySet(List *targetlist);
PyObject   *qualDefsToPyList(List *quallist, ConversionInfo ** cinfo);
PyObject *pythonQual(char *operatorname, PyObject *value,
		   ConversionInfo * cinfo,
		   bool is_array,
		   bool use_or,
		   Oid typeoid);

PyObject  *getSortKey(MulticornDeparsedSortGroup *key);
MulticornDeparsedSortGroup *getDeparsedSortGroup(PyObject *key);


Datum pyobjectToDatum(PyObject *object, StringInfo buffer,
				ConversionInfo * cinfo);
PyObject   *qualdefToPython(MulticornConstQual * qualdef, ConversionInfo ** cinfo);
PyObject *paramDefToPython(List *paramdef, ConversionInfo ** cinfos,
				 Oid typeoid,
				 Datum value);


PyObject   *datumToPython(Datum node, Oid typeoid, ConversionInfo * cinfo);
PyObject   *datumStringToPython(Datum node, ConversionInfo * cinfo);
PyObject   *datumNumberToPython(Datum node, ConversionInfo * cinfo);
PyObject   *datumDateToPython(Datum datum, ConversionInfo * cinfo);
PyObject   *datumTimestampToPython(Datum datum, ConversionInfo * cinfo);
PyObject   *datumIntToPython(Datum datum, ConversionInfo * cinfo);
PyObject   *datumArrayToPython(Datum datum, Oid type, ConversionInfo * cinfo);
PyObject   *datumByteaToPython(Datum datum, ConversionInfo * cinfo);
PyObject   *datumUnknownToPython(Datum datum, ConversionInfo * cinfo, Oid type);


void pythonDictToTuple(PyObject *p_value,
				  TupleTableSlot *slot,
				  ConversionInfo ** cinfos,
				  StringInfo buffer);

void pythonSequenceToTuple(PyObject *p_value,
					  TupleTableSlot *slot,
					  ConversionInfo ** cinfos,
					  StringInfo buffer);

/* Python to cstring functions */
void pyobjectToCString(PyObject *pyobject, StringInfo buffer,
				  ConversionInfo * cinfo);

void pynumberToCString(PyObject *pyobject, StringInfo buffer,
				  ConversionInfo * cinfo);
void pyunicodeToCString(PyObject *pyobject, StringInfo buffer,
				   ConversionInfo * cinfo);
void pystringToCString(PyObject *pyobject, StringInfo buffer,
				  ConversionInfo * cinfo);
void pysequenceToCString(PyObject *pyobject, StringInfo buffer,
					ConversionInfo * cinfo);
void pymappingToCString(PyObject *pyobject, StringInfo buffer,
				   ConversionInfo * cinfo);
void pydateToCString(PyObject *pyobject, StringInfo buffer,
				ConversionInfo * cinfo);

void pyunknownToCstring(PyObject *pyobject, StringInfo buffer,
				   ConversionInfo * cinfo);

void appendBinaryStringInfoQuote(StringInfo buffer,
							char *tempbuffer,
							Py_ssize_t strlength,
							bool need_quote);


static void begin_remote_xact(CacheEntry * entry);

/*
 * Get a (python) encoding name for an attribute.
 */
const char *
getPythonEncodingName()
{
	const char *encoding_name = GetDatabaseEncodingName();

	if (strcmp(encoding_name, "SQL_ASCII") == 0)
	{
		encoding_name = "ascii";
	}
	return encoding_name;
}

char *
PyUnicode_AsPgString(PyObject *p_unicode)
{
	char	   *message = NULL;
	PyObject   *pTempStr;

	if (p_unicode == NULL)
	{
		elog(ERROR, "Received a null pointer in pyunicode_aspgstring");
	}
	pTempStr = PyUnicode_AsEncodedString(p_unicode, getPythonEncodingName(), NULL);
	errorCheck();
	message = strdup(PyBytes_AsString(pTempStr));
	errorCheck();
	Py_DECREF(pTempStr);
	return message;
}

#if PY_MAJOR_VERSION >= 3
/*
 * Convert a C string in the PostgreSQL server encoding to a Python
 * unicode object.	Reference ownership is passed to the caller.
 */
PyObject *
PyString_FromStringAndSize(const char *s, Py_ssize_t size)
{
	char	   *utf8string;
	PyObject   *o;

	utf8string = (char *) pg_do_encoding_conversion((unsigned char *) s,
													strlen(s),
													GetDatabaseEncoding(),
													PG_UTF8);
	if (size < 0)
	{
		o = PyUnicode_FromString(utf8string);
	}
	else
	{
		o = PyUnicode_FromStringAndSize(utf8string, size);
	}
	if (utf8string != s)
		pfree(utf8string);

	return o;
}

PyObject *
PyString_FromString(const char *s)
{
	return PyString_FromStringAndSize(s, -1);
}

char *
PyString_AsString(PyObject *unicode)
{
	char	   *rv;
	PyObject   *o = PyUnicode_AsEncodedString(unicode, GetDatabaseEncodingName(), NULL);
	errorCheck();
	rv = pstrdup(PyBytes_AsString(o));
	Py_XDECREF(o);
	return rv;
}

int
PyString_AsStringAndSize(PyObject *obj, char **buffer, Py_ssize_t *length)
{
	PyObject   *o;
	int			rv;
	char *tempbuffer;

	if (PyUnicode_Check(obj))
	{
		o = PyUnicode_AsEncodedString(obj, GetDatabaseEncodingName(), NULL);
		errorCheck();
		rv = PyBytes_AsStringAndSize(o, &tempbuffer, length);
		*buffer = pstrdup(tempbuffer);
		Py_XDECREF(o);
		return rv;
	}
	return PyBytes_AsStringAndSize(obj, buffer, length);
}
#endif   /* PY_MAJOR_VERSION >= 3 */

/*
 * Utility function responsible for importing, and returning, a class by name
 *
 * Returns a new reference to the class.
 */
PyObject *
getClass(PyObject *className)
{
	PyObject   *p_multicorn = PyImport_ImportModule("multicorn"),
			   *p_class = PYOBJECT_CALLMETHOD(p_multicorn, "get_class", "(O)",
											  className);

	errorCheck();
	Py_DECREF(p_multicorn);
	return p_class;
}

void
appendBinaryStringInfoQuote(StringInfo buffer,
							char *tempbuffer,
							Py_ssize_t strlength,
							bool need_quote)
{
	if (need_quote)
	{
		char	   *c;
		int			i;

		appendStringInfoChar(buffer, '"');
		for (c = tempbuffer, i = 0; i < strlength; ++i, ++c)
		{
			if (*c == '"')
			{
				appendBinaryStringInfo(buffer, "\\\"", 2);
			}
			else if (*c == '\\')
			{
				appendBinaryStringInfo(buffer, "\\\\", 2);
			}
			else
			{
				appendStringInfoChar(buffer, *c);
			}
		}
		appendStringInfoChar(buffer, '"');
	}
	else
	{
		appendBinaryStringInfo(buffer, tempbuffer, strlength);
	}
}

/*
 * Convert a list of Value nodes containing the column name as a string to a
 * pyset of python unicode strings.
 */
PyObject *
valuesToPySet(List *targetlist)
{
	PyObject   *result = PySet_New(0);
	ListCell   *lc;

	foreach(lc, targetlist)
	{
		Value	   *value = (Value *) lfirst(lc);
		PyObject   *pyString = PyString_FromString(strVal(value));

		PySet_Add(result, pyString);
		Py_DECREF(pyString);
	}
	return result;
}

PyObject *
qualDefsToPyList(List *qual_list, ConversionInfo ** cinfos)
{
	ListCell   *lc;
	PyObject   *p_quals = PyList_New(0);

	foreach(lc, qual_list)
	{
		MulticornBaseQual *qual_def = (MulticornBaseQual *) lfirst(lc);

		if (qual_def->right_type == T_Const)
		{
			PyObject   *python_qual = qualdefToPython((MulticornConstQual *) qual_def, cinfos);

			if (python_qual != NULL)
			{
				PyList_Append(p_quals, python_qual);
				Py_DECREF(python_qual);
			}
		}
	}
	return p_quals;
}


/*
 * Same as getClass, but accepts a C-String argument instead of a python
 * string.
 *
 * Returns a new reference to the class.
 */
PyObject *
getClassString(const char *className)
{
	PyObject   *p_classname = PyString_FromString(className),
			   *p_class = getClass(p_classname);

	Py_DECREF(p_classname);
	return p_class;
}


List *
getOptions(Oid foreigntableid)
{
	ForeignTable *f_table;
	ForeignServer *f_server;
	UserMapping *mapping;
	List	   *options;

	f_table = GetForeignTable(foreigntableid);
	f_server = GetForeignServer(f_table->serverid);

	options = NIL;
	options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);
	/* An error might occur if no user mapping is defined. */
	/* In that case, just ignore it */
	mapping = multicorn_GetUserMapping(GetUserId(), f_table->serverid);
	if (mapping)
		options = list_concat(options, mapping->options);
	return options;
}

/*
 * Reimplementation of GetUserMapping, which returns NULL instead of throwing an
 * error when the mapping is not found.
 */
UserMapping *
multicorn_GetUserMapping(Oid userid, Oid serverid)
{
	Datum		datum;
	HeapTuple	tp;
	bool		isnull;
	UserMapping *um;

	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(userid),
						 ObjectIdGetDatum(serverid));

	if (!HeapTupleIsValid(tp))
	{
		/* Not found for the specific user -- try PUBLIC */
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(serverid));
	}

	if (!HeapTupleIsValid(tp))
		return NULL;

	um = (UserMapping *) palloc(sizeof(UserMapping));
	um->userid = userid;
	um->serverid = serverid;

	/* Extract the umoptions */
	datum = SysCacheGetAttr(USERMAPPINGUSERSERVER,
							tp,
							Anum_pg_user_mapping_umoptions,
							&isnull);
	if (isnull)
		um->options = NIL;
	else
		um->options = untransformRelOptions(datum);

	ReleaseSysCache(tp);

	return um;
}


/*
 * Collect and validate options.
 * Only one option is required "wrapper".
 *
 * Returns a new reference to a dictionary.
 *
 * */
PyObject *
optionsListToPyDict(List *options)
{
	ListCell   *lc;
	PyObject   *p_options_dict = PyDict_New();

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		PyObject   *pStr = PyString_FromString((char *) defGetString(def));

		PyDict_SetItemString(p_options_dict, def->defname, pStr);
		Py_DECREF(pStr);
	}
	return p_options_dict;
}


bool
compareOptions(List *options1, List *options2)
{
	ListCell   *lc1,
			   *lc2;


	if (list_length(options1) != list_length(options2))
	{
		return false;
	}
	forboth(lc1, options1, lc2, options2)
	{
		DefElem    *def1 = (DefElem *) lfirst(lc1);
		DefElem    *def2 = (DefElem *) lfirst(lc2);

		if (def1 == NULL || def2 == NULL || strcmp(def1->defname, def2->defname) != 0)
		{
			return false;
		}
		if (strcmp(defGetString(def1), defGetString(def2)) != 0)
		{
			return false;
		}
	}
	return true;
}

void
getColumnsFromTable(TupleDesc desc, PyObject **p_columns, List **columns)
{
	PyObject   *columns_dict = *p_columns;
	List	   *columns_list = *columns;

	if ((columns_dict != NULL) && (columns_list != NULL))
	{
		return;
	}
	else
	{
		int			i;
		PyObject   *p_columnclass = getClassString("multicorn."
												   "ColumnDefinition"),
				   *p_collections = PyImport_ImportModule("collections"),
				   *p_dictclass = PyObject_GetAttrString(p_collections, "OrderedDict");

		columns_dict = PYOBJECT_CALLFUNCTION(p_dictclass, "()");

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);

			if (!att->attisdropped)
			{
				Oid			typOid = att->atttypid;

				char	   *key = NameStr(att->attname);
				int32		typmod = att->atttypmod;
				char	   *base_type = format_type_be(typOid);
				char	   *modded_type = format_type_with_typemod(typOid, typmod);
				List	   *options = GetForeignColumnOptions(att->attrelid,
															  att->attnum);
				PyObject   *p_options = optionsListToPyDict(options);
				PyObject   *column = PYOBJECT_CALLFUNCTION(p_columnclass,
														   "(s,i,i,s,s,O)",
														   key,
														   typOid,
														   typmod,
														   modded_type,
														   base_type,
														   p_options);
				List	   *columnDef = NULL;

				errorCheck();
				columnDef = lappend(columnDef, makeString(pstrdup(key)));
				columnDef = lappend(columnDef, makeConst(TYPEOID,
								   -1, InvalidOid, 4, ObjectIdGetDatum(typOid), false, true));
				columnDef = lappend(columnDef, makeConst(INT4OID,
								   -1, InvalidOid, 4, Int32GetDatum(typmod), false, true));
				columnDef = lappend(columnDef, options);
				columns_list = lappend(columns_list, columnDef);
				PyMapping_SetItemString(columns_dict, key, column);
				Py_DECREF(p_options);
				Py_DECREF(column);
			}
		}
		Py_DECREF(p_columnclass);
		Py_DECREF(p_collections);
		Py_DECREF(p_dictclass);
		errorCheck();
		*p_columns = columns_dict;
		*columns = columns_list;
	}

}

bool
compareColumns(List *columns1, List *columns2)
{
	ListCell   *lc1,
			   *lc2;

	if (columns1->length != columns2->length)
	{
		return false;
	}

	forboth(lc1, columns1, lc2, columns2)
	{
		List	   *coldef1 = lfirst(lc1);
		List	   *coldef2 = lfirst(lc2);
		ListCell   *cell1 = list_head(coldef1),
				   *cell2 = list_head(coldef2);

		/* Compare column name */
		if (strcmp(strVal(lfirst(cell1)), strVal(lfirst(cell2))) != 0)
		{
			return false;
		}
		cell1 = lnext(cell1);
		cell2 = lnext(cell2);
		/* Compare typoid */
		if (((Const *) (lfirst(cell1)))->constvalue != ((Const *) lfirst(cell2))->constvalue)
		{
			return false;
		}
		cell1 = lnext(cell1);
		cell2 = lnext(cell2);
		/* Compare typmod */
		if (((Const *) (lfirst(cell1)))->constvalue != ((Const *) lfirst(cell2))->constvalue)
		{
			return false;
		}
		cell1 = lnext(cell1);
		cell2 = lnext(cell2);
		/* Compare column options */
		if (!compareOptions(lfirst(cell1), lfirst(cell2)))
		{
			return false;
		}
	}
	return true;
}


CacheEntry *
getCacheEntry(Oid foreigntableid)
{
	/*
	 * create a temporary context. If we have to (re)create the python
	 * instance, it will be promoted to a cachememorycontext. Otherwise, it
	 * will be freed before returning the instance
	 */
	MemoryContext tempContext = AllocSetContextCreate(CurrentMemoryContext,
												  "multicorn temporary data",
													  ALLOCSET_SMALL_MINSIZE,
													  ALLOCSET_SMALL_INITSIZE,
													  ALLOCSET_SMALL_MAXSIZE),
				oldContext = MemoryContextSwitchTo(tempContext);
	CacheEntry *entry = NULL;
	bool		found = false;
	List	   *options = getOptions(foreigntableid);
	List	   *columns = NULL;
	PyObject   *p_columns = NULL;
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	Relation	rel = RelationIdGetRelation(ftable->relid);
	TupleDesc	desc = rel->rd_att;
	bool		needInitialization = false;

	entry = hash_search(InstancesHash, &foreigntableid, HASH_ENTER,
						&found);

	if (!found || entry->value == NULL)
	{
		entry->options = NULL;
		entry->columns = NULL;
		entry->cacheContext = NULL;
		entry->xact_depth = 0;
		needInitialization = true;
	}
	else
	{
		/* Even if found, we have to check several things */
		if (!compareOptions(entry->options, options))
		{
			/* Options have changed, we must purge the cache. */
			Py_XDECREF(entry->value);
			needInitialization = true;
		}
		else
		{
			/* Options have not changed, we should look at columns. */
			getColumnsFromTable(desc, &p_columns, &columns);
			if (!compareColumns(columns, entry->columns))
			{
				Py_XDECREF(entry->value);
				needInitialization = true;
			}
			else
			{
				Py_XDECREF(p_columns);
			}
		}
	}
	if (needInitialization)
	{
		PyObject   *p_options = optionsListToPyDict(options),
				   *p_class = getClass(PyDict_GetItemString(p_options,
															"wrapper")),
				   *p_instance;

		entry->value = NULL;
		getColumnsFromTable(desc, &p_columns, &columns);
		PyDict_DelItemString(p_options, "wrapper");
		p_instance = PYOBJECT_CALLFUNCTION(p_class, "(O,O)", p_options,
										   p_columns);
		errorCheck();
		/* Cleanup the old context, containing the old columns and options */
		/* values */
		if (entry->cacheContext != NULL)
		{
			MemoryContextDelete(entry->cacheContext);
		}
		/* Promote this tempcontext. */
		MemoryContextSetParent(tempContext, CacheMemoryContext);
		entry->cacheContext = tempContext;
		entry->options = options;
		entry->columns = columns;
		entry->xact_depth = 0;
		Py_DECREF(p_class);
		Py_DECREF(p_options);
		Py_DECREF(p_columns);
		errorCheck();
		entry->value = p_instance;
		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		MemoryContextSwitchTo(oldContext);
		MemoryContextDelete(tempContext);
	}
	RelationClose(rel);
	Py_INCREF(entry->value);

	/*
	 * Start a new transaction or subtransaction if needed.
	 */
	begin_remote_xact(entry);
	return entry;
}


/*
 * Returns the fdw_instance associated with the foreigntableid.
 *
 * For performance reasons, it is cached in hash table.
 */
PyObject *
getInstance(Oid foreigntableid)
{
	return getCacheEntry(foreigntableid)->value;
}


static void
begin_remote_xact(CacheEntry * entry)
{
	int			curlevel = GetCurrentTransactionNestLevel();
	PyObject   *rv;

	/* Start main transaction if we haven't yet */
	if (entry->xact_depth <= 0)
	{
		rv = PYOBJECT_CALLMETHOD(entry->value, "begin", "(i)", IsolationIsSerializable());
		Py_XDECREF(rv);
		errorCheck();
		entry->xact_depth = 1;
	}

	while (entry->xact_depth < curlevel)
	{
		entry->xact_depth++;
		rv = PYOBJECT_CALLMETHOD(entry->value, "sub_begin", "(i)", entry->xact_depth);
		Py_XDECREF(rv);
		errorCheck();
	}
}



/*
 * Returns the relation estimated size, in term of number of rows and width.
 * This is done by calling the getRelSize python method.
 *
 */
void
getRelSize(MulticornPlanState * state,
		   PlannerInfo *root,
		   double *rows,
		   int *width)
{
	PyObject   *p_targets_set,
			   *p_quals,
			   *p_rows_and_width,
			   *p_rows,
			   *p_width,
			   *p_startup_cost;

	p_targets_set = valuesToPySet(state->target_list);
	p_quals = qualDefsToPyList(state->qual_list, state->cinfos);
	p_rows_and_width = PYOBJECT_CALLMETHOD(state->fdw_instance, "get_rel_size",
										   "(O,O)", p_quals, p_targets_set);
	errorCheck();
	Py_DECREF(p_targets_set);
	Py_DECREF(p_quals);
	if ((p_rows_and_width == Py_None) || PyTuple_Size(p_rows_and_width) != 2)
	{
		Py_DECREF(p_rows_and_width);
		elog(ERROR, "The get_rel_size python method should return a tuple of length 2");
	}
	p_rows = PyNumber_Long(PyTuple_GetItem(p_rows_and_width, 0));
	p_width = PyNumber_Long(PyTuple_GetItem(p_rows_and_width, 1));
	p_startup_cost = PyNumber_Long(
			   PyObject_GetAttrString(state->fdw_instance, "_startup_cost"));
	*rows = PyLong_AsDouble(p_rows);
	*width = (int) PyLong_AsLong(p_width);
	state->startupCost = (int) PyLong_AsLong(p_startup_cost);
	Py_DECREF(p_rows);
	Py_DECREF(p_width);
	Py_DECREF(p_rows_and_width);
}

PyObject *
qualdefToPython(MulticornConstQual * qualdef, ConversionInfo ** cinfos)
{
	int			arrayindex = qualdef->base.varattno - 1;
	char	   *operatorname = qualdef->base.opname;
	ConversionInfo *cinfo = cinfos[arrayindex];
	bool		is_array = qualdef->base.isArray,
				use_or = qualdef->base.useOr;
	Oid			typeoid = qualdef->base.typeoid;
	Datum		value = qualdef->value;
	PyObject   *p_value;

	if (qualdef->isnull)
	{
		p_value = Py_None;
		Py_INCREF(Py_None);
	}
	else
	{
		if (typeoid == InvalidOid)
		{
			typeoid = cinfo->atttypoid;
		}
		p_value = datumToPython(value, typeoid, cinfo);
		if (p_value == NULL)
		{
			return NULL;
		}
	}

	if (typeoid <= 0)
	{
		typeoid = cinfo->atttypoid;
	}

	return pythonQual(operatorname, p_value,
					  cinfo, is_array, use_or, typeoid);
}


PyObject *
pythonQual(char *operatorname,
		   PyObject *value,
		   ConversionInfo * cinfo,
		   bool is_array,
		   bool use_or,
		   Oid typeoid)
{
	PyObject   *qualClass = getClassString("multicorn.Qual"),
			   *qualInstance,
			   *p_operatorname,
			   *operator,
			   *columnName;

	p_operatorname = PyUnicode_Decode(operatorname, strlen(operatorname), getPythonEncodingName(), NULL);
	errorCheck();
	if (is_array)
	{
		PyObject   *arrayOpType;


		if (use_or)
		{
			arrayOpType = Py_True;
		}
		else
		{
			arrayOpType = Py_False;
		}
		operator = Py_BuildValue("(O, O)", p_operatorname, arrayOpType);
		Py_DECREF(p_operatorname);
		errorCheck();
	}
	else
	{
		operator = p_operatorname;
	}

	columnName = PyUnicode_Decode(cinfo->attrname, strlen(cinfo->attrname), getPythonEncodingName(), NULL);
	qualInstance = PYOBJECT_CALLFUNCTION(qualClass, "(O,O,O)",
										 columnName,
										 operator,
										 value);
	errorCheck();
	Py_DECREF(value);
	Py_DECREF(operator);
	Py_DECREF(qualClass);
	Py_DECREF(columnName);
	return qualInstance;
}

PyObject  *
getSortKey(MulticornDeparsedSortGroup *key)
{
	PyObject *SortKeyClass = getClassString("multicorn.SortKey"),
			 *SortKeyInstance,
			 *p_attname,
			 *p_reversed,
			 *p_nulls_first,
			 *p_collate;

	p_attname = PyUnicode_Decode(NameStr(*(key->attname)), strlen(NameStr(*(key->attname))), getPythonEncodingName(), NULL);
	if (key->reversed)
		p_reversed = Py_True;
	else
		p_reversed = Py_False;
	if (key->nulls_first)
		p_nulls_first = Py_True;
	else
		p_nulls_first = Py_False;
	if(key->collate == NULL){
		p_collate = Py_None;
		Py_INCREF(p_collate);
	}
	else
		p_collate = PyUnicode_Decode(NameStr(*(key->collate)), strlen(NameStr(*(key->collate))), getPythonEncodingName(), NULL);
	SortKeyInstance = PYOBJECT_CALLFUNCTION(SortKeyClass, "(O,i,O,O,O)",
			p_attname,
			key->attnum,
			p_reversed,
			p_nulls_first,
			p_collate);
	errorCheck();
	Py_DECREF(p_attname);
	Py_DECREF(p_collate);
	Py_DECREF(SortKeyClass);
	return SortKeyInstance;
}

MulticornDeparsedSortGroup *
getDeparsedSortGroup(PyObject *sortKey)
{
	MulticornDeparsedSortGroup *md = palloc0(sizeof(MulticornDeparsedSortGroup));
	PyObject * p_temp;
	p_temp = PyObject_GetAttrString(sortKey, "attname");
	md->attname = (Name) strdup(PyUnicode_AS_DATA(p_temp));
	Py_DECREF(p_temp);
	p_temp = PyObject_GetAttrString(sortKey, "attnum");
	md->attnum = (int) PyLong_AsLong(p_temp);
	Py_DECREF(p_temp);
	p_temp = PyObject_GetAttrString(sortKey, "is_reversed");
	md->reversed = PyObject_IsTrue(p_temp);
	Py_DECREF(p_temp);
	p_temp = PyObject_GetAttrString(sortKey, "nulls_first");
	md->nulls_first = PyObject_IsTrue(PyObject_GetAttrString(sortKey, "nulls_first"));
	Py_DECREF(p_temp);
	p_temp = PyObject_GetAttrString(sortKey, "collate");
	if(p_temp == Py_None)
		md->collate = 0;
	else
		md->collate = (Name) strdup(PyUnicode_AS_DATA(p_temp));
	Py_DECREF(p_temp);
	return md;
}


/*
 * Execute the query in the python fdw, and returns an iterator.
 */
PyObject *
execute(ForeignScanState *node, ExplainState *es)
{
	MulticornExecState *state = node->fdw_state;
	PyObject   *p_targets_set,
			   *p_quals = PyList_New(0),
			   *p_pathkeys = PyList_New(0),
			   *p_iterable,
			   *p_method;
	ListCell   *lc;

	ExprContext *econtext = node->ss.ps.ps_ExprContext;

	foreach(lc, state->qual_list)
	{
		MulticornBaseQual *qual = lfirst(lc);
		MulticornConstQual *newqual = NULL;
		bool		isNull;
		ExprState  *expr_state = NULL;

		switch (qual->right_type)
		{
			case T_Param:
				expr_state = ExecInitExpr(((MulticornParamQual *) qual)->expr,
										  (PlanState *) node);
				newqual = palloc0(sizeof(MulticornConstQual));
				newqual->base.right_type = T_Const;
				newqual->base.varattno = qual->varattno;
				newqual->base.opname = qual->opname;
				newqual->base.isArray = qual->isArray;
				newqual->base.useOr = qual->useOr;

				#if PG_VERSION_NUM >= 100000
				newqual->value = ExecEvalExpr(expr_state, econtext, &isNull);
				#else
				newqual->value = ExecEvalExpr(expr_state, econtext, &isNull, NULL);
				#endif
				newqual->base.typeoid = ((Param*) ((MulticornParamQual *) qual)->expr)->paramtype;
				newqual->isnull = isNull;
				break;
			case T_Const:
				newqual = (MulticornConstQual *) qual;
				break;
			default:
				break;
		}
		if (newqual != NULL)
		{
			PyObject   *python_qual = qualdefToPython((MulticornConstQual *) newqual, state->cinfos);

			if (python_qual != NULL)
			{
				PyList_Append(p_quals, python_qual);
				Py_DECREF(python_qual);
			}
		}
	}
	/* Transform every object to a suitable python representation */
	p_targets_set = valuesToPySet(state->target_list);

	foreach(lc, state->pathkeys)
	{
		MulticornDeparsedSortGroup *pathkey = (MulticornDeparsedSortGroup *) lfirst(lc);
		PyObject *python_sortkey = getSortKey(pathkey);
		PyList_Append(p_pathkeys, python_sortkey);
		Py_DECREF(python_sortkey);
	}
	{
		PyObject * args,
				 * kwargs = PyDict_New();
		if(PyList_Size(p_pathkeys) > 0){
			PyDict_SetItemString(kwargs, "sortkeys", p_pathkeys);
		}
		if(es != NULL){
			PyObject * verbose;
			if(es->verbose){
				verbose = Py_True;
			} else {
				verbose = Py_False;
			}
			p_method = PyObject_GetAttrString(state->fdw_instance, "explain");
			args = PyTuple_Pack(2, p_quals, p_targets_set);
			PyDict_SetItemString(kwargs, "verbose", verbose);
			errorCheck();
		} else {
			p_method = PyObject_GetAttrString(state->fdw_instance, "execute");
			errorCheck();
			args = PyTuple_Pack(2, p_quals, p_targets_set);
			errorCheck();
		}
		p_iterable = PyObject_Call(p_method, args, kwargs);
		errorCheck();
		Py_DECREF(p_method);
		Py_DECREF(args);
		Py_DECREF(kwargs);
	}

	errorCheck();
	if (p_iterable == Py_None){
		state->p_iterator = p_iterable;
	}
	else
	{
		state->p_iterator = PyObject_GetIter(p_iterable);
	}
	Py_DECREF(p_quals);
	Py_DECREF(p_targets_set);
	Py_DECREF(p_pathkeys);
	Py_DECREF(p_iterable);
	errorCheck();
	return state->p_iterator;
}

void
pynumberToCString(PyObject *pyobject, StringInfo buffer,
				  ConversionInfo * cinfo)
{
	PyObject   *pTempStr;
	char	   *tempbuffer;
	Py_ssize_t	strlength = 0;

	pTempStr = PyObject_Str(pyobject);
	PyString_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
	appendBinaryStringInfo(buffer, tempbuffer, strlength);
	Py_DECREF(pTempStr);
}

void
pyunicodeToCString(PyObject *pyobject, StringInfo buffer,
				   ConversionInfo * cinfo)
{
	char	   *tempbuffer;
	Py_ssize_t	strlength = 0;
	PyObject   *pTempStr;
	pTempStr = PyUnicode_AsEncodedString(pyobject, getPythonEncodingName(), NULL);
	errorCheck();
	PyBytes_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
	appendBinaryStringInfoQuote(buffer, tempbuffer, strlength, cinfo->need_quote);
	Py_DECREF(pTempStr);
}

void
pystringToCString(PyObject *pyobject, StringInfo buffer,
				  ConversionInfo * cinfo)
{
	char	   *tempbuffer;
	Py_ssize_t	strlength = 0;

	if (PyString_AsStringAndSize(pyobject, &tempbuffer, &strlength) < 0)
	{
		ereport(WARNING,
				(errmsg("An error occured while decoding the %s column",
						cinfo->attrname),
				 errhint("You should maybe return unicode instead?")));
	}
	appendBinaryStringInfoQuote(buffer, tempbuffer, strlength, cinfo->need_quote);
}

void
pysequenceToCString(PyObject *pyobject, StringInfo buffer,
					ConversionInfo * cinfo)
{
	/* Its an array */
	Py_ssize_t	i,
				size = PySequence_Size(pyobject);
	PyObject   *p_item;
	int			previous_dims = cinfo->attndims,
				previous_needquote = cinfo->need_quote;

	if (cinfo->attndims == 0)
	{
		/* We are not supposed to be converted to an array. */
		pyunknownToCstring(pyobject, buffer, cinfo);
		return;
	}
	appendStringInfoChar(buffer, '{');
	/* We are an array, so we need to quote stuff */
	cinfo->need_quote = true;
	cinfo->attndims = cinfo->attndims - 1;
	for (i = 0; i < size; i++)
	{
		p_item = PySequence_GetItem(pyobject, i);
		pyobjectToCString(p_item, buffer, cinfo);
		Py_DECREF(p_item);
		if (i != size - 1)
		{
			appendBinaryStringInfo(buffer, ", ", 2);
		}
	}
	appendStringInfoChar(buffer, '}');
	cinfo->attndims = previous_dims;
	cinfo->need_quote = previous_needquote;
}

void
pymappingToCString(PyObject *pyobject, StringInfo buffer,
				   ConversionInfo * cinfo)
{
	PyObject   *items = PyMapping_Items(pyobject);
	PyObject   *current_tuple;
	Py_ssize_t	i;
	Py_ssize_t	size = PyList_Size(items);
	bool		need_quote = cinfo->need_quote;

	cinfo->need_quote = true;
	for (i = 0; i < size; i++)
	{
		current_tuple = PySequence_GetItem(items, i);
		pyobjectToCString(PyTuple_GetItem(current_tuple, 0),
						  buffer, cinfo);
		appendBinaryStringInfo(buffer, "=>", 2);
		pyobjectToCString(PyTuple_GetItem(current_tuple, 1),
						  buffer, cinfo);
		if (i != size - 1)
		{
			appendBinaryStringInfo(buffer, ", ", 2);
		}
		Py_DECREF(current_tuple);
	}
	Py_DECREF(items);
	cinfo->need_quote = need_quote;
}

void
pydateToCString(PyObject *pyobject, StringInfo buffer,
				ConversionInfo * cinfo)
{
	char	   *tempbuffer;
	Py_ssize_t	strlength = 0;
	PyObject   *formatted_date;

	formatted_date = PYOBJECT_CALLMETHOD(pyobject, "isoformat", "()");
	PyString_AsStringAndSize(formatted_date, &tempbuffer, &strlength);
	appendBinaryStringInfo(buffer, tempbuffer, strlength);
	Py_DECREF(formatted_date);
}


void
pyobjectToCString(PyObject *pyobject, StringInfo buffer,
				  ConversionInfo * cinfo)
{
	if (pyobject == NULL || pyobject == Py_None)
	{
		return;
	}
	if (PyNumber_Check(pyobject))
	{
		pynumberToCString(pyobject, buffer, cinfo);
		return;
	}
	if (PyUnicode_Check(pyobject))
	{
		pyunicodeToCString(pyobject, buffer, cinfo);
		return;
	}
	if (PyBytes_Check(pyobject))
	{
		pystringToCString(pyobject, buffer, cinfo);
		return;
	}
	if (PySequence_Check(pyobject))
	{
		pysequenceToCString(pyobject, buffer, cinfo);
		return;
	}
	if (PyMapping_Check(pyobject))
	{
		pymappingToCString(pyobject, buffer, cinfo);
		return;
	}
	PyDateTime_IMPORT;
	if (PyDate_Check(pyobject))
	{
		pydateToCString(pyobject, buffer, cinfo);
		return;
	}
	pyunknownToCstring(pyobject, buffer, cinfo);
}

void
pyunknownToCstring(PyObject *pyobject, StringInfo buffer,
				   ConversionInfo * cinfo)
{
	PyObject   *pTempStr = PyObject_Str(pyobject);
	char	   *tempbuffer;
	Py_ssize_t	strlength;

	PyString_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
	errorCheck();
	appendBinaryStringInfoQuote(buffer, tempbuffer, strlength, cinfo->need_quote);
	Py_DECREF(pTempStr);
	return;
}

void
pythonDictToTuple(PyObject *p_value,
				  TupleTableSlot *slot,
				  ConversionInfo ** cinfos,
				  StringInfo buffer)
{
	int			i;
	PyObject   *p_object;
	Datum	   *values = slot->tts_values;
	bool	   *nulls = slot->tts_isnull;

	for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		char	   *key;
		Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor,i);
		AttrNumber	cinfo_idx = attr->attnum - 1;

		if (cinfos[cinfo_idx] == NULL)
		{
			continue;
		}
		key = cinfos[cinfo_idx]->attrname;
		p_object = PyMapping_GetItemString(p_value, key);
		if (p_object != NULL && p_object != Py_None)
		{
			/* attr->attypid 0 seems to flag a junk column 
			   such as .....pg.droped.xxx.... */
			if(key == NULL ||
			   attr->atttypid == 0 ||
			   attr->attisdropped != 0 ||
			   strcmp(key, attr->attname.data) != 0)
			{
				if (key == NULL)
				{
					key="NULL";
				}
				if (errstart(ERROR,
					     __FILE__,
					     __LINE__,
					     PG_FUNCNAME_MACRO,
					     TEXTDOMAIN))
				{
					errmsg("Bad Attribute in multicorn");
					errhint("Multicorn needs to be fixed");
					errdetail("attr->atttypid=%d, attr->attlen=%d, attr->attisdropped=%d, attr->attname=%s key=%s",
						  attr->atttypid, attr->attlen,
						  attr->attisdropped,
						  attr->attname.data, key);
					errfinish(0);
				}
				
			}
			if (errstart(INFO,
				     __FILE__,
				     __LINE__,
				     PG_FUNCNAME_MACRO,
				     TEXTDOMAIN))
			{
				errmsg("Multicorn: Found %s in dict.", key);
				errhint("attr->attname.data=%s",
				       attr->attname.data);
				errfinish(0);
			}
			resetStringInfo(buffer);
			values[i] = pyobjectToDatum(p_object,
						    buffer,
						    cinfos[cinfo_idx]);
			if (buffer->data == NULL)
			{
				nulls[i] = true;
			}
			else
			{
				nulls[i] = false;
			}
		}
		else
		{
			/* "KeyError", doesnt matter. */
			PyErr_Clear();
			if (errstart(INFO,
				     __FILE__,
				     __LINE__,
				     PG_FUNCNAME_MACRO,
				     TEXTDOMAIN))
			{
				errmsg("Multicorn: Didn't find %s in dict.", key);
				errhint("attr->attname.data=%s",
				       attr->attname.data);
				errfinish(0);
			}
			values[i] = (Datum) NULL;
			nulls[i] = true;
		}
		Py_XDECREF(p_object);
	}
}

void
pythonSequenceToTuple(PyObject *p_value,
					  TupleTableSlot *slot,
					  ConversionInfo ** cinfos,
					  StringInfo buffer)
{
	int			i,
				j;
	Datum	   *values = slot->tts_values;
	bool	   *nulls = slot->tts_isnull;

	for (i = 0, j = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		PyObject   *p_object;
		Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor,i);
		AttrNumber	cinfo_idx = attr->attnum - 1;

		if (cinfos[cinfo_idx] == NULL)
		{
			continue;
		}
		p_object = PySequence_GetItem(p_value, j);
		if(p_object == NULL || p_object == Py_None){
			nulls[i] = true;
			values[i] = 0;
		}
		else
		{
			resetStringInfo(buffer);
			values[i] = pyobjectToDatum(p_object, buffer,
										cinfos[cinfo_idx]);
			if (buffer->data == NULL)
			{
				nulls[i] = true;
			}
			else
			{
				nulls[i] = false;
			}
		}
		errorCheck();
		Py_DECREF(p_object);
		j++;
	}
}

/*
 * Convert a python result (a sequence or a dictionary) to a tupletableslot.
 */
void
pythonResultToTuple(PyObject *p_value,
					TupleTableSlot *slot,
					ConversionInfo ** cinfos,
					StringInfo buffer)
{
	if (PySequence_Check(p_value))
	{
		pythonSequenceToTuple(p_value, slot, cinfos, buffer);
	}
	else
	{

		if (PyMapping_Check(p_value))
		{
			pythonDictToTuple(p_value, slot, cinfos, buffer);
		}
		else
		{
			elog(ERROR, "Cannot transform anything else than mappings and"
				 "sequences to rows");
		}
	}
}

Datum
pyobjectToDatum(PyObject *object, StringInfo buffer,
				ConversionInfo * cinfo)
{
	Datum		value = 0;

	pyobjectToCString(object, buffer,
					  cinfo);

	if (buffer->len >= 0)
	{

		if (cinfo->atttypoid == BYTEAOID || cinfo->atttypoid == TEXTOID ||
			cinfo->atttypoid == VARCHAROID)
		{
			/*
			 * Special case, since the value is already a byte string.
			 */
			value = PointerGetDatum(cstring_to_text_with_len(buffer->data,
															 buffer->len));
		}
		else
		{
			value = InputFunctionCall(cinfo->attinfunc,
									  buffer->data,
									  cinfo->attioparam,
									  cinfo->atttypmod);
		}
	}
	return value;
}

PyObject *
datumStringToPython(Datum datum, ConversionInfo * cinfo)
{
	char	   *temp;
	ssize_t		size;
	PyObject   *result;

	temp = datum == 0 ? "?" : TextDatumGetCString(datum);
	size = strlen(temp);
	result = PyUnicode_Decode(temp, size, getPythonEncodingName(), NULL);
	return result;
}

PyObject *
datumUnknownToPython(Datum datum, ConversionInfo * cinfo, Oid type)
{
	char	   *temp;
	ssize_t		size;
	PyObject   *result;
	Oid			outfuncoid;
	bool		isvarlena;
	FmgrInfo   *fmout = palloc0(sizeof(FmgrInfo));

	getTypeOutputInfo(type, &outfuncoid, &isvarlena);
	fmgr_info(outfuncoid, fmout);
	temp = OutputFunctionCall(fmout, datum);
	size = strlen(temp);
	result = PyUnicode_Decode(temp, size, getPythonEncodingName(), NULL);
	pfree(fmout);
	return result;
}

PyObject *
datumNumberToPython(Datum datum, ConversionInfo * cinfo)
{
	ssize_t		numvalue = (ssize_t) DatumGetNumeric(datum);
	char	   *tempvalue = (char *) DirectFunctionCall1(numeric_out,
														 numvalue);
	PyObject   *buffer = PyString_FromString(tempvalue),
#if PY_MAJOR_VERSION >= 3
			   *value = PyFloat_FromString(buffer);
#else
			   *value = PyFloat_FromString(buffer, NULL);
#endif
	Py_DECREF(buffer);
	return value;
}

PyObject *
datumDateToPython(Datum datum, ConversionInfo * cinfo)
{
	struct pg_tm *pg_tm_value = palloc(sizeof(struct pg_tm));
	PyObject   *result;
	fsec_t		fsec;

	PyDateTime_IMPORT;
	datum = DirectFunctionCall1(date_timestamp, datum);
	timestamp2tm(DatumGetTimestamp(datum), NULL, pg_tm_value, &fsec,
				 NULL, NULL);
	result = PyDate_FromDate(pg_tm_value->tm_year,
							 pg_tm_value->tm_mon, pg_tm_value->tm_mday);
	pfree(pg_tm_value);
	return result;
}

PyObject *
datumTimestampToPython(Datum datum, ConversionInfo * cinfo)
{
	struct pg_tm *pg_tm_value = palloc(sizeof(struct pg_tm));
	PyObject   *result;
	fsec_t		fsec;

	PyDateTime_IMPORT;
	timestamp2tm(DatumGetTimestamp(datum), NULL, pg_tm_value, &fsec,
				 NULL, NULL);
	result = PyDateTime_FromDateAndTime(pg_tm_value->tm_year,
										pg_tm_value->tm_mon,
										pg_tm_value->tm_mday,
										pg_tm_value->tm_hour,
										pg_tm_value->tm_min,
										pg_tm_value->tm_sec, 0);
	pfree(pg_tm_value);
	return result;
}

PyObject *
datumIntToPython(Datum datum, ConversionInfo * cinfo)
{
	return PyLong_FromLong(DatumGetInt32(datum));
}

PyObject *
datumArrayToPython(Datum datum, Oid type, ConversionInfo * cinfo)
{
#if PG_VERSION_NUM >= 90500
	ArrayIterator iterator = array_create_iterator(DatumGetArrayTypeP(datum),
												   0, NULL);
# else
	ArrayIterator iterator = array_create_iterator(DatumGetArrayTypeP(datum),
												   0);
# endif

	Datum		elem = (Datum) NULL;
	bool		isnull;
	PyObject   *result = PyList_New(0),
			   *pyitem;

	while (array_iterate(iterator, &elem, &isnull))
	{
		if (isnull)
		{
			PyList_Append(result, Py_None);
		}
		else
		{
			HeapTuple	tuple;
			Form_pg_type typeStruct;

			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "lookup failed for type %u",
					 type);
			}
			typeStruct = (Form_pg_type) GETSTRUCT(tuple);
			pyitem = datumToPython(elem, typeStruct->typelem, cinfo);
			ReleaseSysCache(tuple);

			PyList_Append(result, pyitem);
			Py_DECREF(pyitem);
		}
	}
	return result;
}


PyObject *
datumByteaToPython(Datum datum, ConversionInfo * cinfo)
{
	text	   *txt = DatumGetByteaP(datum);
	char	   *str = txt == NULL ? "?" : VARDATA(txt);
	size_t		size = VARSIZE(txt) - VARHDRSZ;

#if PY_MAJOR_VERSION >= 3
	return PyBytes_FromStringAndSize(str, size);
#else
	return PyString_FromStringAndSize(str, size);
#endif
}


PyObject *
datumToPython(Datum datum, Oid type, ConversionInfo * cinfo)
{
	HeapTuple	tuple;
	Form_pg_type typeStruct;

	switch (type)
	{
		case BYTEAOID:
			return datumByteaToPython(datum, cinfo);
		case TEXTOID:
		case VARCHAROID:
			return datumStringToPython(datum, cinfo);
		case NUMERICOID:
			return datumNumberToPython(datum, cinfo);
		case DATEOID:
			return datumDateToPython(datum, cinfo);
		case TIMESTAMPOID:
			return datumTimestampToPython(datum, cinfo);
		case INT4OID:
			return datumIntToPython(datum, cinfo);
		default:
			/* Case for the array ? */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "lookup failed for type %u",
					 type);
			}
			typeStruct = (Form_pg_type) GETSTRUCT(tuple);
			ReleaseSysCache(tuple);
			if ((typeStruct->typelem != 0) && (typeStruct->typlen == -1))
			{
				/* Its an array. */
				return datumArrayToPython(datum, type, cinfo);
			}
			return datumUnknownToPython(datum, cinfo, type);
	}
}

/*
 * Call the path_keys method from the python implementation, and convert the
 * result to a list of "tuples" (list) of the form:
 *
 * - Bitmapset of attnums - Cost (integer)
 */
List *
pathKeys(MulticornPlanState * state)
{
	List	   *result = NULL;
	Py_ssize_t	i;
	PyObject   *fdw_instance = state->fdw_instance,
			   *p_pathkeys;

	p_pathkeys = PYOBJECT_CALLMETHOD(fdw_instance, "get_path_keys", "()");
	errorCheck();
	for (i = 0; i < PySequence_Length(p_pathkeys); i++)
	{
		PyObject   *p_item = PySequence_GetItem(p_pathkeys, i),
				   *p_keys = PySequence_GetItem(p_item, 0),
				   *p_cost = PySequence_GetItem(p_item, 1),
				   *p_cost_long = PyNumber_Long(p_cost);
		double		rows = PyLong_AsDouble(p_cost_long);
		ssize_t		j;
		List	   *attnums = NULL;
		List	   *item = NULL;

		for (j = 0; j < PySequence_Length(p_keys); j++)
		{
			PyObject   *p_key = PySequence_GetItem(p_keys, j);
			ssize_t		k;

			/* Lookup the attribute number by its key. */
			for (k = 0; k < state->numattrs; k++)
			{
				ConversionInfo *cinfo = state->cinfos[k];

				if (cinfo == NULL)
				{
					continue;
				}
				if (p_key != Py_None &&
					strcmp(cinfo->attrname, PyString_AsString(p_key)) == 0)
				{
					attnums = list_append_unique_int(attnums, cinfo->attnum);
					break;
				}
			}
			Py_DECREF(p_key);
		}
		item = lappend(item, attnums);
		item = lappend(item, makeConst(INT4OID,
									 -1, InvalidOid, 4, rows, false, true));
		result = lappend(result, item);
		Py_DECREF(p_keys);
		Py_DECREF(p_cost);
		Py_DECREF(p_cost_long);
		Py_DECREF(p_item);
	}
	Py_DECREF(p_pathkeys);
	return result;
}

/*
 * Call the can_sort method from the python implementation. We provide a deparsed
 * version of the requested fields to sort with all detail as needed (nulls,
 * collate...), and convert the result to a list of "tuples" (list) of the form:
 *
 * - Bitmapset of attnums
 *
 * representing the fields that the foreign data wrapper can be sort as
 * we requested.
 */
List *
canSort(MulticornPlanState * state, List *deparsed)
{
	List	   *result = NULL;
	ListCell   *lc;
	Py_ssize_t	i;
	PyObject   *fdw_instance = state->fdw_instance,
			   *p_pathkeys = PyList_New(0),
			   *p_sortable;

	foreach(lc, deparsed)
	{
		MulticornDeparsedSortGroup *pathkey = (MulticornDeparsedSortGroup *) lfirst(lc);
		PyObject *python_sortkey = getSortKey(pathkey);
		PyList_Append(p_pathkeys, python_sortkey);
		Py_DECREF(python_sortkey);
	}

	p_sortable = PYOBJECT_CALLMETHOD(fdw_instance, "can_sort", "(O)", p_pathkeys);
	errorCheck();
	for (i = 0; i < PySequence_Length(p_sortable); i++)
	{
		PyObject   *p_key = PySequence_GetItem(p_sortable, i);
		MulticornDeparsedSortGroup *md = getDeparsedSortGroup(p_key);
		result = lappend(result, md);
		Py_DECREF(p_key);
	}
	Py_DECREF(p_pathkeys);
	Py_DECREF(p_sortable);
	return result;
}

PyObject *
tupleTableSlotToPyObject(TupleTableSlot *slot, ConversionInfo ** cinfos)
{
	PyObject   *result = PyDict_New();
	TupleDesc	tupdesc = slot->tts_tupleDescriptor;
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc,i);
		bool		isnull;
		Datum		value;
		PyObject   *item;
		AttrNumber	cinfo_idx = attr->attnum - 1;

		if (attr->attisdropped || cinfos[cinfo_idx] == NULL)
		{
			continue;
		}
		value = slot_getattr(slot, i + 1, &isnull);
		if (isnull)
		{
			item = Py_None;
			Py_INCREF(item);
		}
		else
		{
			item = datumToPython(value, cinfos[cinfo_idx]->atttypoid,
								 cinfos[cinfo_idx]);
			errorCheck();
		}
		PyDict_SetItemString(result, cinfos[cinfo_idx]->attrname, item);
		Py_DECREF(item);
	}
	return result;
}

/*
 * Get the rowid column name
 */
char *
getRowIdColumn(PyObject *fdw_instance)
{
	PyObject   *value = PyObject_GetAttrString(fdw_instance, "rowid_column");
	char	   *result;

	errorCheck();
	if (value == Py_None || value == NULL)
	{
	        if (value != NULL) {
		        Py_DECREF(value);
		}
		elog(ERROR, "This FDW does not support the writable API");
	}
	result = PyString_AsString(value);
	Py_DECREF(value);
	return result;
}
