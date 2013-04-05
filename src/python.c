#include <Python.h>
#include "datetime.h"
#include "postgres.h"
#include "multicorn.h"
#include "catalog/pg_user_mapping.h"
#include "miscadmin.h"
#include "utils/numeric.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/array.h"
#include "utils/catcache.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "executor/nodeSubplan.h"

List	   *getOptions(Oid foreigntableid);
PyObject   *optionsListToPyDict(List *options);
bool		compareOptions(List *options1, List *options2);

void		getColumnsFromTable(TupleDesc desc, PyObject **p_columns, List **columns);
bool		compareColumns(List *columns1, List *columns2);

PyObject   *getClass(PyObject *className);
PyObject   *valuesToPySet(List *targetlist);
PyObject   *qualDefsToPyList(List *quallist, ConversionInfo ** cinfo);
PyObject *pythonQual(char *operatorname, Datum dvalue,
		   Oid consttype,
		   ConversionInfo * cinfo,
		   bool is_array,
		   bool use_or);





Datum pyobjectToDatum(PyObject *object, StringInfo buffer,
				ConversionInfo * cinfo);
PyObject   *qualdefToPython(List *qualdef, ConversionInfo ** cinfo);
PyObject *paramDefToPython(List *paramdef, ConversionInfo ** cinfos,
				 Oid typeoid,
				 Datum value);


PyObject   *datumToPython(Datum node, Oid typeoid, ConversionInfo * cinfo);
PyObject   *datumStringToPython(Datum node, ConversionInfo * cinfo);
PyObject   *datumNumberToPython(Datum node, ConversionInfo * cinfo);
PyObject   *datumDateToPython(Datum datum, ConversionInfo * cinfo);
PyObject   *datumTimestampToPython(Datum datum, ConversionInfo * cinfo);
PyObject   *datumIntToPython(Datum datum, ConversionInfo * cinfo);
PyObject   *datumArrayToPython(Datum datum, ConversionInfo * cinfo);
PyObject   *datumByteaToPython(Datum datum, ConversionInfo * cinfo);



void pythonDictToTuple(PyObject *p_value,
				  MulticornExecState * state);
void pythonSequenceToTuple(PyObject *p_value,
					  MulticornExecState * state);

/*	Python to cstring functions */
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

/* Hash table mapping oid to fdw instances */
static HTAB *InstancesHash;

typedef struct CacheEntry
{
	Oid			hashkey;
	PyObject   *value;
	List	   *options;
	List	   *columns;
}	CacheEntry;

/*
 * Utility function responsible for importing, and returning, a class by name
 *
 * Returns a new reference to the class.
 */
PyObject *
getClass(PyObject *className)
{
	PyObject   *p_multicorn = PyImport_ImportModule("multicorn"),
			   *p_class = PyObject_CallMethod(p_multicorn, "get_class", "(O)",
											  className);

	errorCheck();
	Py_DECREF(p_multicorn);
	return p_class;
}

/*
 *	Convert a list of Value nodes containing the column name as a string
 *	to a pyset of python unicode strings.
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
		List	   *qual_def = (List *) lfirst(lc);
		PyObject   *python_qual = qualdefToPython(qual_def, cinfos);

		if (python_qual != NULL)
		{
			PyList_Append(p_quals, python_qual);
			Py_DECREF(python_qual);
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
getClassString(char *className)
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
	MemoryContext savedContext = CurrentMemoryContext;

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
		MemoryContextSwitchTo(savedContext);
		/* DO NOTHING HERE */
	}
	PG_END_TRY();
	return options;
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
	bool		got_module = false;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		PyObject   *pStr = PyString_FromString((char *) defGetString(def));

		if (strcmp(def->defname, "wrapper") == 0)
		{
			got_module = true;
		}
		PyDict_SetItemString(p_options_dict, def->defname, pStr);
		Py_DECREF(pStr);
	}
	if (!got_module)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("wrapper option not found"),
				 errhint("You must set wrapper option to a ForeignDataWrapper"
						 "python class, for example multicorn.csv.CsvFdw")));
	}
	return p_options_dict;
}

bool
compareOptions(List *options1, List *options2)
{
	ListCell   *lc1,
			   *lc2;

	if (options1->length != options2->length)
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
				   *p_dictclass = getClassString("multicorn.ordered_dict.OrderedDict");

		columns_dict = PyObject_CallFunction(p_dictclass, "()");

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = desc->attrs[i];

			if (!att->attisdropped)
			{
				Oid			typOid = att->atttypid;
				char	   *key = NameStr(att->attname);
				char	   *formatted_type = format_type_be(typOid);
				PyObject   *column = PyObject_CallFunction(p_columnclass,
														   "(s,i,s)",
														   key,
														   typOid,
														   formatted_type);
				List	   *columnDef = NULL;

				columnDef = lappend(columnDef, makeString(key));
				columnDef = lappend_oid(columnDef, typOid);
				columnDef = lappend(columnDef, makeString(formatted_type));
				columns_list = lappend(columns_list, columnDef);
				PyMapping_SetItemString(columns_dict, key, column);
				Py_DECREF(column);
			}
		}
		Py_DECREF(p_columnclass);
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

		if (strcmp(strVal(linitial(coldef1)), strVal(linitial(coldef2))) != 0)
		{
			return false;
		}
		if (lsecond_oid(coldef1) != lsecond_oid(coldef2))
		{
			return false;
		}
		if (strcmp(strVal(lthird(coldef1)), strVal(lthird(coldef2))) != 0)
		{
			return false;
		}
	}
	return true;
}

/*
 *	Returns the fdw_instance associated with the foreigntableid.
 *
 *	For performance reasons, it is cached in hash table.
 */
PyObject *
getInstance(Oid foreigntableid)
{
	MemoryContext oldContext = MemoryContextSwitchTo(CacheMemoryContext);
	CacheEntry *entry = NULL;
	bool		found = false;
	List	   *options = getOptions(foreigntableid);
	List	   *columns = NULL;
	PyObject   *p_columns = NULL;
	ForeignTable *ftable = GetForeignTable(foreigntableid);
	Relation	rel = RelationIdGetRelation(ftable->relid);
	TupleDesc	desc = rel->rd_att;
	bool		needInitialization = false;

	/* Initialize hash table if needed. */
	if (InstancesHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(CacheEntry);
		ctl.hash = oid_hash;
		ctl.hcxt = CacheMemoryContext;
		InstancesHash = hash_create("multicorn instances", 32,
									&ctl,
									HASH_ELEM | HASH_FUNCTION);
	}

	entry = hash_search(InstancesHash, &foreigntableid, HASH_ENTER,
						&found);

	if (!found || entry->value == NULL)
	{
		needInitialization = true;
	}
	else
	{
		if (!compareOptions(entry->options, options))
		{
			/* Options have changed, we must purge the cache. */
			Py_DECREF(entry->value);
			needInitialization = true;
		}
		else
		{
			/* Options have not changed, we should look at columns. */
			getColumnsFromTable(desc, &p_columns, &columns);
			if (!compareColumns(columns, entry->columns))
			{
				Py_DECREF(entry->value);
				needInitialization = true;
			}
		}
	}
	if (needInitialization)
	{
		PyObject   *p_options = optionsListToPyDict(options),
				   *p_class = getClass(PyDict_GetItemString(p_options,
															"wrapper"));

		getColumnsFromTable(desc, &p_columns, &columns);
		PyDict_DelItemString(p_options, "wrapper");
		entry->value = PyObject_CallFunction(p_class, "(O,O)", p_options,
											 p_columns);
		entry->options = options;
		entry->columns = columns;
		Py_DECREF(p_class);
		Py_DECREF(p_options);
		Py_DECREF(p_columns);
		errorCheck();
	}
	RelationClose(rel);
	Py_INCREF(entry->value);
	MemoryContextSwitchTo(oldContext);
	return entry->value;
}

/*
 *	Returns the relation estimated size, in term of number of rows and width.
 *	This is done by calling the getRelSize python method.
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
	p_rows_and_width = PyObject_CallMethod(state->fdw_instance, "get_rel_size",
										   "(O,O)", p_quals, p_targets_set);
	errorCheck();
	p_rows = PyNumber_Long(PyTuple_GetItem(p_rows_and_width, 0));
	p_width = PyNumber_Int(PyTuple_GetItem(p_rows_and_width, 1));
	p_startup_cost = PyNumber_Long(
			   PyObject_GetAttrString(state->fdw_instance, "_startup_cost"));
	*rows = PyLong_AsDouble(p_rows);
	*width = (int) PyInt_AsLong(p_width);
	state->startupCost = (int) PyInt_AsLong(p_startup_cost);
	Py_DECREF(p_rows);
	Py_DECREF(p_width);
	Py_DECREF(p_rows_and_width);
	Py_DECREF(p_targets_set);
	Py_DECREF(p_quals);
}

PyObject *
qualdefToPython(List *qualdef, ConversionInfo ** cinfos)
{
	int			arrayindex = list_nth_int(qualdef, 0);
	char	   *operatorname = strVal(((Const *) list_nth(qualdef, 1)));
	ConversionInfo *cinfo = cinfos[arrayindex];
	Const	   *cvalue = list_nth(qualdef, 2);
	bool		is_array = list_nth_int(qualdef, 3),
				use_or = list_nth_int(qualdef, 4);

	return pythonQual(operatorname, cvalue->constvalue,
					  cvalue->consttype, cinfo, is_array, use_or);
}

PyObject *
paramDefToPython(List *paramdef, ConversionInfo ** cinfos,
				 Oid typeoid,
				 Datum value)
{
	int			arrayindex = list_nth_int(paramdef, 0);
	char	   *operatorname = strVal(((Const *) list_nth(paramdef, 1)));
	ConversionInfo *cinfo = cinfos[arrayindex];
	bool		is_array = list_nth_int(paramdef, 3),
				use_or = list_nth_int(paramdef, 4);

	return pythonQual(operatorname, value,
					  typeoid, cinfo, is_array, use_or);
}

PyObject *
pythonQual(char *operatorname, Datum dvalue,
		   Oid consttype,
		   ConversionInfo * cinfo,
		   bool is_array,
		   bool use_or)
{
	PyObject   *value = datumToPython(dvalue, consttype,
									  cinfo),
			   *qualClass = getClassString("multicorn.Qual"),
			   *qualInstance,
			   *operator;

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
		operator = Py_BuildValue("(s, O)", operatorname, arrayOpType);

	}
	else
	{

		operator = PyString_FromString(operatorname);
	}
	qualInstance = PyObject_CallFunction(qualClass, "(s,O,O)",
										 cinfo->attrname,
										 operator,
										 value);
	Py_DECREF(value);
	Py_DECREF(operator);
	Py_DECREF(qualClass);
	return qualInstance;
}


/*
 * Execute the query in the python fdw, and returns an iterator.
 */
PyObject *
execute(ForeignScanState *node)
{
	MulticornExecState *state = node->fdw_state;
	PyObject   *p_targets_set,
			   *p_quals,
			   *p_iterable;

	ParamListInfo params = node->ss.ps.state->es_param_list_info;
	ParamExecData *exec_params = node->ss.ps.state->es_param_exec_vals;
	ListCell   *lc;

	/* Transform every object to a suitable python representation */
	p_targets_set = valuesToPySet(state->target_list);
	p_quals = qualDefsToPyList(state->qual_list, state->cinfos);
	/* Do the same thing with params */
	foreach(lc, state->param_list)
	{
		List	   *param_def = (List *) lfirst(lc);
		Param	   *param = (Param *) lthird(param_def);
		Datum		value = 0;
		Oid			type = param->paramtype;
		PyObject   *p_param;

		switch (param->paramkind)
		{
			case PARAM_EXTERN:
				{
					ParamExternData *prm = &params->params[param->paramid];

					value = prm->value;

				}
				break;
			case PARAM_EXEC:
				{
					ParamExecData prm = exec_params[param->paramid];

					if (exec_params[param->paramid].isnull)
					{
						value = 0;
					}
					else
					{
						value = prm.value;
					}
				}
				break;
			default:
				break;
		}
		if (value)
		{
			p_param = paramDefToPython(param_def,
									   state->cinfos,
									   type,
									   value);
			PyList_Append(p_quals, p_param);
			Py_DECREF(p_param);
		}
	}
	p_iterable = PyObject_CallMethod(state->fdw_instance,
									 "execute",
									 "(O,O)",
									 p_quals,
									 p_targets_set);
	if (p_iterable == Py_None)
	{
		state->p_iterator = p_iterable;
	}
	else
	{
		state->p_iterator = PyObject_GetIter(p_iterable);
	}
	Py_DECREF(p_targets_set);
	Py_DECREF(p_quals);
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
	Py_ssize_t	unicode_size;
	char	   *tempbuffer;
	Py_ssize_t	strlength = 0;

	unicode_size = PyUnicode_GET_SIZE(pyobject);
	if (!cinfo || !cinfo->encodingname)
	{
		PyString_AsStringAndSize(pyobject, &tempbuffer, &strlength);
		appendBinaryStringInfo(buffer, tempbuffer, strlength);
	}
	else
	{
		PyObject   *pTempStr;

		pTempStr = PyUnicode_Encode(PyUnicode_AsUnicode(pyobject),
									unicode_size,
									cinfo->encodingname, NULL);
		PyString_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
		appendBinaryStringInfo(buffer, tempbuffer, strlength);
		Py_DECREF(pTempStr);
	}
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
	appendBinaryStringInfo(buffer, tempbuffer, strlength);
}

void
pysequenceToCString(PyObject *pyobject, StringInfo buffer,
					ConversionInfo * cinfo)
{
	/* Its an array */
	Py_ssize_t	i,
				size = PySequence_Size(pyobject);
	PyObject   *p_item;

	appendStringInfoChar(buffer, '{');
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
}

void
pymappingToCString(PyObject *pyobject, StringInfo buffer,
				   ConversionInfo * cinfo)
{
	PyObject   *items = PyMapping_Items(pyobject);
	PyObject   *current_tuple;
	Py_ssize_t	i;
	Py_ssize_t	size = PyList_Size(items);

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
}

void
pydateToCString(PyObject *pyobject, StringInfo buffer,
				ConversionInfo * cinfo)
{
	char	   *tempbuffer;
	Py_ssize_t	strlength = 0;
	PyObject   *formatted_date;

	formatted_date = PyObject_CallMethod(pyobject, "isoformat", "()");
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
	if (PyString_Check(pyobject))
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
	/* Default handling for unknown objects */
	{
		PyObject   *pTempStr = PyObject_Str(pyobject);
		char	   *tempbuffer;
		Py_ssize_t	strlength;

		PyString_AsStringAndSize(pTempStr, &tempbuffer, &strlength);
		appendBinaryStringInfo(buffer, tempbuffer, strlength);
		Py_DECREF(pTempStr);
		return;
	}

}


void
pythonDictToTuple(PyObject *p_value,
				  MulticornExecState * state)
{
	int			i;
	PyObject   *p_object;

	for (i = 0; i < state->numattrs; i++)
	{
		char	   *key;

		if (state->cinfos[i] == NULL)
		{
			continue;
		}
		key = state->cinfos[i]->attrname;
		p_object = PyMapping_GetItemString(p_value, key);

		if (p_object != NULL && p_object != Py_None)
		{
			resetStringInfo(state->buffer);
			state->values[i] = pyobjectToDatum(p_object,
											   state->buffer,
											   state->cinfos[i]);
			if (state->values[i] == (Datum) NULL)
			{
				state->nulls[i] = true;
			}
			else
			{
				state->nulls[i] = false;
			}
		}
		else
		{
			/* "KeyError", doesnt matter. */
			PyErr_Clear();
			state->values[i] = (Datum) NULL;
			state->nulls[i] = true;
		}
		if (p_object != NULL)
		{
			Py_DECREF(p_object);
		}
	}
}

void
pythonSequenceToTuple(PyObject *p_value,
					  MulticornExecState * state)
{
	int			i;

	if (PySequence_Size(p_value) != state->attinmeta->tupdesc->natts)
	{
		elog(ERROR, "The python backend did not return a valid sequence");
		return;
	}
	else
	{
		for (i = 0; i < state->numattrs; i++)
		{
			PyObject   *p_object;

			if (state->cinfos[i] == NULL)
			{
				continue;
			}
			p_object = PySequence_GetItem(p_value, i);
			resetStringInfo(state->buffer);
			state->values[i] = pyobjectToDatum(p_object, state->buffer,
											   state->cinfos[i]);
			if (state->values[i] == (Datum) NULL)
			{
				state->nulls[i] = true;
			}
			else
			{
				state->nulls[i] = false;
			}
			errorCheck();
			Py_DECREF(p_object);
		}
	}
}

/*
 * Convert a python result (a sequence or a dictionary) to a tupletableslot.
 */
void
pythonResultToTuple(PyObject *p_value,
					MulticornExecState * state)
{
	if (PyMapping_Check(p_value))
	{
		pythonDictToTuple(p_value, state);
	}
	else
	{

		if (PySequence_Check(p_value))
		{
			pythonSequenceToTuple(p_value, state);
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
			/* Special case, since the value is already a byte string. */
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

	temp = TextDatumGetCString(datum);
	size = strlen(temp);
	if (!cinfo || !cinfo->encodingname)
	{
		result = PyString_FromString(temp);
	}
	else
	{
		result = PyUnicode_Decode(temp, size, cinfo->encodingname,
								  NULL);
	}
	return result;
}

PyObject *
datumNumberToPython(Datum datum, ConversionInfo * cinfo)
{
	ssize_t		numvalue = (ssize_t) DatumGetNumeric(datum);
	char	   *tempvalue = (char *) DirectFunctionCall1(numeric_out, numvalue);
	PyObject   *buffer = PyString_FromString(tempvalue),
			   *value = PyFloat_FromString(buffer, NULL);

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
	timestamp2tm(DatumGetTimestamp(datum), NULL, pg_tm_value, &fsec, NULL, NULL);
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
	return PyInt_FromLong(DatumGetInt32(datum));
}

PyObject *
datumArrayToPython(Datum datum, ConversionInfo * cinfo)
{
	ArrayIterator iterator = array_create_iterator(DatumGetArrayTypeP(datum),
												   0);
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
			pyitem = datumToPython(elem, cinfo->atttypoid, cinfo);
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
	char	   *str = VARDATA(txt);
	size_t		size = VARSIZE(txt) - VARHDRSZ;

	return PyBytes_FromStringAndSize(str, size);
}


PyObject *
datumToPython(Datum datum, Oid type, ConversionInfo * cinfo)
{
	HeapTuple	tuple;
	Form_pg_type typeStruct;

	if (!datum)
	{
		Py_INCREF(Py_None);
		return Py_None;
	}
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
				return datumArrayToPython(datum, cinfo);
			}
			/* Defaults to NULL */
			return NULL;
	}
}

/*
 * Call the path_keys method from the python implementation,
 * and convert the result to a list of "tuples" (list) of the form:
 *
 * - Bitmapset of attnums
 * - Cost (integer)
 */
List *
pathKeys(MulticornPlanState * state)
{
	List	   *result = NULL;
	Py_ssize_t	i;
	PyObject   *fdw_instance = state->fdw_instance,
			   *p_pathkeys;

	p_pathkeys = PyObject_CallMethod(fdw_instance, "get_path_keys", "()");
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
		item = lappend_int(item, rows);
		result = lappend(result, item);
		Py_DECREF(p_keys);
		Py_DECREF(p_cost);
		Py_DECREF(p_cost_long);
		Py_DECREF(p_item);
	}
	Py_DECREF(p_pathkeys);
	return result;
}
