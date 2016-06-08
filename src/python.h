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



#ifndef PG_PYTHON_H
#define PG_PYTHON_H

const char * getPythonEncodingName(void);
char * PyUnicode_AsPgString(PyObject *p_unicode);

#if PY_MAJOR_VERSION >= 3
    PyObject * PyString_FromStringAndSize(const char *s, Py_ssize_t size);
    PyObject * PyString_FromString(const char *s);
    char * PyString_AsString(PyObject *unicode);
    int PyString_AsStringAndSize(PyObject *obj, char **buffer, Py_ssize_t *length);
#endif   /* PY_MAJOR_VERSION >= 3 */

PyObject * getClass(PyObject *className);
void appendBinaryStringInfoQuote(StringInfo buffer, char *tempbuffer, Py_ssize_t strlength, bool need_quote);
PyObject * valuesToPySet(List *targetlist);
PyObject * qualDefsToPyList(List *qual_list, ConversionInfo ** cinfos);
PyObject * getClassString(const char *className);
List * getOptions(Oid foreigntableid);
UserMapping * multicorn_GetUserMapping(Oid userid, Oid serverid);
PyObject * optionsListToPyDict(List *options);
bool compareOptions(List *options1, List *options2);
void getColumnsFromTable(TupleDesc desc, PyObject **p_columns, List **columns);
bool compareColumns(List *columns1, List *columns2);
CacheEntry * getCacheEntry(Oid foreigntableid);
PyObject * getInstance(Oid foreigntableid);
void getRelSize(MulticornPlanState * state, PlannerInfo *root, double *rows, int *width);
PyObject * qualdefToPython(MulticornConstQual * qualdef, ConversionInfo ** cinfos);
PyObject * pythonQual(char *operatorname, PyObject *value, ConversionInfo * cinfo, bool is_array, bool use_or, Oid typeoid);
PyObject  * getSortKey(MulticornDeparsedSortGroup *key);
MulticornDeparsedSortGroup * getDeparsedSortGroup(PyObject *sortKey);
PyObject * execute(ForeignScanState *node, ExplainState *es);
void pynumberToCString(PyObject *pyobject, StringInfo buffer, ConversionInfo * cinfo);
void pyunicodeToCString(PyObject *pyobject, StringInfo buffer, ConversionInfo * cinfo);
void pystringToCString(PyObject *pyobject, StringInfo buffer, ConversionInfo * cinfo);
void pysequenceToCString(PyObject *pyobject, StringInfo buffer, ConversionInfo * cinfo);
void pymappingToCString(PyObject *pyobject, StringInfo buffer, ConversionInfo * cinfo);
void pydateToCString(PyObject *pyobject, StringInfo buffer, ConversionInfo * cinfo);
void pyobjectToCString(PyObject *pyobject, StringInfo buffer, ConversionInfo * cinfo);
void pyunknownToCstring(PyObject *pyobject, StringInfo buffer, ConversionInfo * cinfo);
void pythonDictToTuple(PyObject *p_value, TupleTableSlot *slot, ConversionInfo ** cinfos, StringInfo buffer);
void pythonSequenceToTuple(PyObject *p_value, TupleTableSlot *slot, ConversionInfo ** cinfos, StringInfo buffer);
void pythonResultToTuple(PyObject *p_value, TupleTableSlot *slot, ConversionInfo ** cinfos, StringInfo buffer);
Datum pyobjectToDatum(PyObject *object, StringInfo buffer, ConversionInfo * cinfo);
PyObject * datumStringToPython(Datum datum, ConversionInfo * cinfo);
PyObject * datumUnknownToPython(Datum datum, ConversionInfo * cinfo, Oid type);
PyObject * datumNumberToPython(Datum datum, ConversionInfo * cinfo);
PyObject * datumDateToPython(Datum datum, ConversionInfo * cinfo);
PyObject * datumTimestampToPython(Datum datum, ConversionInfo * cinfo);
PyObject * datumIntToPython(Datum datum, ConversionInfo * cinfo);
PyObject * datumArrayToPython(Datum datum, Oid type, ConversionInfo * cinfo);
PyObject * datumByteaToPython(Datum datum, ConversionInfo * cinfo);
PyObject * datumToPython(Datum datum, Oid type, ConversionInfo * cinfo);
List * pathKeys(MulticornPlanState * state);
List * canSort(MulticornPlanState * state, List *deparsed);
PyObject * tupleTableSlotToPyObject(TupleTableSlot *slot, ConversionInfo ** cinfos);
char * getRowIdColumn(PyObject *fdw_instance);

#endif
