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
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "mb/pg_wchar.h"
#include <Python.h>


PG_MODULE_MAGIC;

typedef struct MulticornState
{
    AttInMetadata *attinmeta;
    PyObject *columns_list;
    int rownum;
    PyObject *pIterator;
} MulticornState;

extern Datum multicorn_handler( PG_FUNCTION_ARGS);
extern Datum multicorn_validator( PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1( multicorn_handler );
PG_FUNCTION_INFO_V1( multicorn_validator );

/*
 * FDW functions declarations
 */
static FdwPlan *multicorn_plan( Oid foreign_table_id, PlannerInfo *root, RelOptInfo *base_relation );
static void multicorn_explain( ForeignScanState *node, ExplainState *es);
static void multicorn_begin( ForeignScanState *node, int eflags);
static TupleTableSlot *multicorn_iterate( ForeignScanState *node );
static void multicorn_rescan( ForeignScanState *node );
static void multicorn_end( ForeignScanState *node );

/*
   Helpers
   */
static void multicorn_error_check(void);
static void init_if_needed(void);
void multicorn_get_options( Oid foreign_table_id, PyObject *options_dict, char **module );
void multicorn_get_attributes_name( TupleDesc desc, PyObject* list );
void multicorn_extract_conditions( ForeignScanState * node, PyObject* list, PyObject* multicorn_module );
PyObject* multicorn_constant_to_python( Const* constant, Form_pg_attribute attribute );
void multicorn_report_exception(PyObject* pErrType, PyObject* pErrValue, PyObject* pErrTraceback);
PyObject* multicorn_get_instance(Relation rel);
HeapTuple BuildTupleFromCStringsWithSize(AttInMetadata *attinmeta, char **values, ssize_t* sizes);
void  multicorn_get_columns(List *columnlist, TupleDesc desc, PyObject *);
void  multicorn_get_column(Expr* expr, TupleDesc desc, PyObject* list);
const char* get_encoding_from_attribute( Form_pg_attribute attribute );


HeapTuple pysequence_to_postgres_tuple( TupleDesc desc, PyObject *pyseq );
HeapTuple pydict_to_postgres_tuple( TupleDesc desc, PyObject *pydict );
ssize_t pyobject_to_cstring( PyObject *pyobject, Form_pg_attribute attribute, char** buffer );

const char* DATE_FORMAT_STRING = "%Y-%m-%d";

PyObject* TABLES_DICT;

Datum
multicorn_handler( PG_FUNCTION_ARGS)
{
    FdwRoutine *fdw_routine = makeNode( FdwRoutine );

    fdw_routine->PlanForeignScan = multicorn_plan;
    fdw_routine->ExplainForeignScan = multicorn_explain;
    fdw_routine->BeginForeignScan = multicorn_begin;
    fdw_routine->IterateForeignScan = multicorn_iterate;
    fdw_routine->ReScanForeignScan = multicorn_rescan;
    fdw_routine->EndForeignScan = multicorn_end;

    PG_RETURN_POINTER( fdw_routine );
}

Datum
multicorn_validator( PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL( true );
}

static FdwPlan *
multicorn_plan(  Oid foreign_table_id,
        PlannerInfo *root,
        RelOptInfo  *base_relation )
{
    FdwPlan *fdw_plan;
    fdw_plan = makeNode( FdwPlan );
    fdw_plan->startup_cost = 10;
    base_relation->rows = 1;
    fdw_plan->total_cost = 15;
    return fdw_plan;
}

static void
multicorn_explain( ForeignScanState *node, ExplainState *es)
{
    /* TODO: calculate real values */
    ExplainPropertyText( "Foreign multicorn", "multicorn", es);

    if ( es->costs)
    {
        ExplainPropertyLong( "Foreign multicorn cost", 10.5, es);
    }
}

static void
multicorn_begin( ForeignScanState *node, int eflags)
{
    /*  TODO: do things if necessary */
    AttInMetadata  *attinmeta;
    Relation        rel = node->ss.ss_currentRelation;
    MulticornState *state;
    PyObject       *pModule, *pArgs, *pValue, 
                   *pObj, *pMethod, *pConds, *pName,
                   *colList;
    init_if_needed();
    pName = PyUnicode_FromString( "multicorn" );
    pModule = PyImport_Import( pName );
    attinmeta = TupleDescGetAttInMetadata( rel->rd_att );
    pObj = multicorn_get_instance(rel);
    state = ( MulticornState * ) palloc(sizeof( MulticornState ));
    state->rownum = 0;
    state->attinmeta = attinmeta;
    node->fdw_state = ( void * ) state;
    pArgs = PyTuple_New( 2 );
    pConds = PyList_New( 0 );
    multicorn_extract_conditions( node, pConds, pModule );
    PyTuple_SetItem( pArgs, 0, pConds);
    colList = PySet_New(NULL);
    Py_INCREF(colList);
    multicorn_get_columns(node->ss.ps.targetlist, rel->rd_att, colList);
    multicorn_get_columns(node->ss.ps.qual, rel->rd_att, colList);
    PyTuple_SetItem( pArgs, 1, colList);
    multicorn_error_check();
    pMethod = PyObject_GetAttrString( pObj, "execute" );
    pValue = PyObject_CallObject( pMethod, pArgs);
    Py_DECREF( pMethod );
    Py_DECREF( pArgs);
    multicorn_error_check();
    state->pIterator = PyObject_GetIter( pValue );
    multicorn_error_check();
    Py_DECREF( pValue );
    Py_DECREF( pModule );
}


static TupleTableSlot *
multicorn_iterate( ForeignScanState *node )
{
    TupleTableSlot  *slot = node->ss.ss_ScanTupleSlot;
    MulticornState  *state = ( MulticornState * ) node->fdw_state;
    HeapTuple        tuple;
    PyObject        *pValue, *pArgs, *pIterator, *pyStopIteration,
                    *pErrType, *pErrValue, *pErrTraceback;

    ExecClearTuple( slot );

    pArgs = PyTuple_New( 0 );
    pIterator = state->pIterator;
    Py_DECREF( pArgs);
    if ( pIterator == NULL ) {
        PyErr_Print(  );
        return slot;
    }

    pValue = PyIter_Next( pIterator );
    PyErr_Fetch(&pErrType, &pErrValue, &pErrTraceback);
    if ( pErrType ) {
        pyStopIteration = PyObject_GetAttrString(PyImport_Import( PyUnicode_FromString("exceptions")),
                "StopIteration");
        if ( PyErr_GivenExceptionMatches(pErrType, pyStopIteration) ){
            /* "Normal" stop iteration */
            return slot;
        } else {
            multicorn_report_exception(pErrType, pErrValue, pErrTraceback);
        }
    }
    if ( pValue == NULL || pValue == Py_None) {
        return slot;
    }

    if ( PyMapping_Check( pValue )) {
        tuple = pydict_to_postgres_tuple( node->ss.ss_currentRelation->rd_att, pValue );
    } else { 
        if ( PySequence_Check( pValue )) {
            tuple = pysequence_to_postgres_tuple( node->ss.ss_currentRelation->rd_att, pValue );
        } else {
            elog( ERROR, "Cannot transform anything else than mappings and sequences to rows" );
            return slot;
        }
    }
    Py_DECREF( pValue );
    ExecStoreTuple( tuple, slot, InvalidBuffer, false );
    state->rownum++;
    return slot;
}

static void
multicorn_rescan( ForeignScanState *node )
{
    MulticornState *state = ( MulticornState * ) node->fdw_state;
    state->rownum = 0;
}

static void
multicorn_end( ForeignScanState *node )
{
    MulticornState *state = ( MulticornState * ) node->fdw_state;
    Py_DECREF(state->pIterator );
}

static void
multicorn_error_check()
{
    PyObject *pErrType, *pErrValue, *pErrTraceback;
    PyErr_Fetch(&pErrType, &pErrValue, &pErrTraceback);
    if ( pErrType ) {
        multicorn_report_exception(pErrType, pErrValue, pErrTraceback);
    }
}

void
multicorn_get_options( Oid foreign_table_id, PyObject *pOptions, char **module )
{
    ForeignTable    *f_table;
    ForeignServer   *f_server;
    List            *options;
    ListCell        *lc;
    bool             got_module = false;
    PyObject        *pStr;

    f_table = GetForeignTable( foreign_table_id );
    f_server = GetForeignServer( f_table->serverid );

    options = NIL;
    options = list_concat( options, f_table->options);
    options = list_concat( options, f_server->options);

    foreach( lc, options) {

        DefElem *def = ( DefElem * ) lfirst( lc );

        if (strcmp( def->defname, "wrapper" ) == 0 ) {
            *module = ( char* ) defGetString( def );
            got_module = true;
        } else {
            pStr = PyString_FromString( (char* )defGetString( def ));
            PyDict_SetItemString( pOptions, def->defname, pStr );
            Py_DECREF( pStr );
        }
    }
    if ( !got_module ) {
        ereport( ERROR,
                ( errcode( ERRCODE_FDW_OPTION_NAME_NOT_FOUND ),
                  errmsg( "wrapper option not found" ),
                  errhint( "You must set wrapper option to a ForeignDataWrapper python class, for example multicorn.csv.CsvFdw" )) );
    }
}


HeapTuple
pydict_to_postgres_tuple( TupleDesc desc, PyObject *pydict )
{
    HeapTuple      tuple;
    AttInMetadata *attinmeta = TupleDescGetAttInMetadata( desc );
    PyObject      *pStr;
    char          *key;
    char         **tup_values;
    int            i, natts;
    ssize_t* sizes;
    char * buffer;
    natts = desc->natts;
    tup_values = ( char ** ) palloc(sizeof( char * ) * natts);
    sizes = (ssize_t*) palloc(sizeof(ssize_t) * natts);
    for ( i = 0; i< natts; i++ ) {
        key = NameStr( desc->attrs[i]->attname );
        if ( PyMapping_HasKeyString( pydict, key) ) {
            pStr = PyMapping_GetItemString( pydict, key );
            multicorn_error_check();
            sizes[i] = pyobject_to_cstring( pStr, desc->attrs[i], &buffer );
            if (sizes[i] < 0){
                tup_values[i] = NULL;
                sizes[i] = 0;
            }else {
                tup_values[i] = (char *) malloc(sizeof (char) * (sizes[i]+ 1));
                memcpy(tup_values[i], buffer, sizes[i] + 1);
            }
            multicorn_error_check();
            Py_DECREF( pStr );
        } else {
            sizes[i] = 0;
            tup_values[i] = NULL;
        }
    }
    tuple = BuildTupleFromCStringsWithSize( attinmeta, tup_values, sizes);
    return tuple;
}

HeapTuple
pysequence_to_postgres_tuple( TupleDesc desc, PyObject *pyseq )
{
    HeapTuple      tuple;
    AttInMetadata *attinmeta = TupleDescGetAttInMetadata( desc );
    char         **tup_values;
    int i, natts;
    PyObject      *pStr;
    ssize_t* sizes;
    char * buffer;

    natts = desc->natts;
    sizes = (ssize_t*) palloc(sizeof(ssize_t) * natts);
    if ( PySequence_Size( pyseq ) != natts) {
        elog( ERROR, "The python backend did not return a valid sequence" );
        return NULL;
    } else {
        tup_values = ( char ** ) palloc(sizeof( char * ) * natts);
        for( i = 0; i < natts; i++ ) {
            pStr = PySequence_GetItem( pyseq, i );
            multicorn_error_check();
            sizes[i] = pyobject_to_cstring( pStr, desc->attrs[i], &buffer );
            if (sizes[i] < 0){
                tup_values[i] = NULL;
                sizes[i] = 0;
            }else {
                tup_values[i] = (char *) malloc(sizeof (char) * (sizes[i]+ 1));
                memcpy(tup_values[i], buffer, sizes[i] + 1);
            }
            multicorn_error_check();
            Py_DECREF( pStr );
        }
        tuple = BuildTupleFromCStringsWithSize( attinmeta, tup_values, sizes);
        return tuple;
    }
}

const char* 
get_encoding_from_attribute( Form_pg_attribute attribute )
{
    HeapTuple          tp;
    Form_pg_collation  colltup;
    const char        *encoding_name;
    tp = SearchSysCache1( COLLOID, ObjectIdGetDatum( attribute->attcollation ));
    if ( !HeapTupleIsValid( tp ))
        return "ascii";
    colltup = ( Form_pg_collation ) GETSTRUCT( tp );
    ReleaseSysCache( tp );
    if ( colltup->collencoding == -1 ) {
        /* No encoding information, do stupid things */
        encoding_name = GetDatabaseEncodingName();
    } else {
        encoding_name = ( char* ) pg_encoding_to_char( colltup->collencoding );
    }
    if(strcmp(encoding_name, "SQL_ASCII") == 0){
        encoding_name = "ascii";
    }
    return encoding_name;
}

ssize_t 
pyobject_to_cstring( PyObject *pyobject, Form_pg_attribute attribute, char**buffer )
{
    PyObject *date_module = PyImport_Import( PyUnicode_FromString( "datetime" ));
    PyObject *date_cls = PyObject_GetAttrString( date_module, "date" );
    PyObject *pStr;
    Py_ssize_t strlength = 0;
    
    
    if ( PyNumber_Check( pyobject )) {
        PyString_AsStringAndSize( PyObject_Str( pyobject ), buffer, &strlength);
        return strlength;
    }
    if ( pyobject == NULL || pyobject == Py_None ) {
        return -1;
    }
    if ( PyUnicode_Check( pyobject )) {
        Py_ssize_t         unicode_size;
        const char * encoding_name = get_encoding_from_attribute( attribute );
        unicode_size = PyUnicode_GET_SIZE( pyobject );
        if ( !encoding_name ) {
            PyString_AsStringAndSize( pyobject, buffer, &strlength );
        } else {
            PyString_AsStringAndSize( PyUnicode_Encode( PyUnicode_AsUnicode( pyobject ), unicode_size, 
                        encoding_name, NULL ), buffer, &strlength);
        }
        multicorn_error_check();
        return strlength;
    }
    if ( PyString_Check( pyobject )) {
        PyString_AsStringAndSize( pyobject, buffer, &strlength);
        multicorn_error_check();
        return strlength;
    }
    if ( PyObject_IsInstance( pyobject, date_cls) ) {
        PyObject *date_format_method = PyObject_GetAttrString( pyobject, "strftime" );
        PyObject *pArgs = PyTuple_New( 1 );
        PyObject *formatted_date = PyObject_CallObject( date_format_method, pArgs);
        Py_DECREF( pArgs);
        Py_DECREF( date_format_method );
        pStr = PyString_FromString( DATE_FORMAT_STRING );
        PyTuple_SetItem( pArgs, 0, pStr );
        Py_DECREF( pStr );
        PyString_AsStringAndSize( formatted_date , buffer, &strlength);
        return strlength;
    }
    Py_DECREF( date_module );
    Py_DECREF( date_cls);
    PyString_AsStringAndSize( pyobject, buffer, &strlength);
    return strlength;
}

// Appends the columns names as python strings to the given python list
void
multicorn_get_attributes_name( TupleDesc desc, PyObject * list )
{
    char       *key;
    Py_ssize_t  i, natts;
    natts = desc->natts;
    for( i = 0; i< natts; i++ ) {
        key = NameStr( desc->attrs[i]->attname );
        PyList_Append( list, PyString_FromString( key ));
    }
}


void
multicorn_extract_conditions( ForeignScanState * node, PyObject* list, PyObject* multicorn_module )
{
    if ( node->ss.ps.plan->qual ) {
        ListCell   *lc;
        PyObject   *qual_class = PyObject_GetAttrString( multicorn_module, "Qual" );
        PyObject   *args;
        List       *quals = list_copy( node->ss.ps.qual );
        TupleDesc  tupdesc = node->ss.ss_currentRelation->rd_att;
        foreach ( lc, quals) {
            ExprState   *xpstate = lfirst( lc );
            Node        *nodexp = ( Node * ) xpstate->expr;
            if ( nodexp != NULL && IsA( nodexp, OpExpr )) {
                OpExpr          *op = ( OpExpr * ) nodexp;
                Node            *left, *right;
                Index            varattno;
                char            *key;
                PyObject        *val;
                HeapTuple        tp;
                Form_pg_operator operator_tup;
                if ( list_length( op->args) == 2 ) {
                    left = list_nth( op->args, 0 );
                    right = list_nth( op->args, 1 );
                    if ( IsA( right, RelabelType )) {
                        right = ( Node* ) ( (RelabelType * ) right )->arg;
                    }
                    if ( IsA( left, RelabelType )) {
                        left = ( Node* ) ( (RelabelType * ) left )->arg;
                    }
                    if ( IsA( left, Var )) {
                        varattno = ( (Var * ) left )->varattno;
                        multicorn_error_check();
                        Assert( 0 < varattno && varattno <= tupdesc->natts);
                        key = NameStr( tupdesc->attrs[varattno - 1]->attname );
                        tp = SearchSysCache1( OPEROID, ObjectIdGetDatum( op->opno ));
                        if ( !HeapTupleIsValid( tp ))
                            elog( ERROR, "cache lookup failed for operator %u", op->opno );
                        operator_tup = ( Form_pg_operator ) GETSTRUCT( tp );
                        ReleaseSysCache( tp );
                        if ( IsA( right, Const )) {
                            val = multicorn_constant_to_python( (Const * ) right, tupdesc->attrs[varattno -1] );
                            args = PyTuple_New( 3 );
                            PyTuple_SetItem( args, 0, PyString_FromString( key ));
                            PyTuple_SetItem( args, 1, PyString_FromString( NameStr( operator_tup->oprname )) );
                            PyTuple_SetItem( args, 2, val );
                            multicorn_error_check();
                            PyList_Append( list, PyObject_CallObject( qual_class, args) );
                            multicorn_error_check();
                            Py_DECREF( args);
                        }
                    }
                }
            }
        }
    }
}

PyObject*
multicorn_constant_to_python( Const* constant, Form_pg_attribute attribute )
{
    PyObject* result;
    if ( constant->consttype == 25 ) {
        /* Its a string */
        const char * encoding_name;
        char * value;
        Py_ssize_t size;
        value = TextDatumGetCString( constant->constvalue );
        size = strlen( value );
        encoding_name = get_encoding_from_attribute( attribute );
        if ( !encoding_name ) {
            result = PyString_FromString( value );
            multicorn_error_check();
        } else {
            result = PyUnicode_Decode( value, size, encoding_name, NULL );
            multicorn_error_check();
        }
    } else if ( constant->consttype == 1700 ) {
        /* Its a numeric */
        char*  number;
        number = ( char* ) DirectFunctionCall1( numeric_out, (ssize_t) DatumGetNumeric( constant->constvalue ));
        result = PyFloat_FromString( PyString_FromString( number ), NULL );
    } else if ( constant->consttype == 23 ) {
        long number;
        number = DatumGetInt32( constant->constvalue );
        result = PyInt_FromLong( number );
    } else {
        result = PyString_FromString( "NA" );
    }
    return result;
}

void
multicorn_report_exception(PyObject* pErrType, PyObject* pErrValue, PyObject* pErrTraceback)
{
    char    *errName, *errValue;
    PyObject *traceback_list;
    PyObject *tracebackModule = PyImport_Import(PyString_FromString("traceback"));
    PyObject *format_exception = PyObject_GetAttrString(tracebackModule, "format_exception");
    PyObject *newline = PyString_FromString("\n");
    PyErr_NormalizeException(&pErrType, &pErrValue, &pErrTraceback);
    errName = PyString_AsString(PyObject_GetAttrString(pErrType, "__name__"));
    errValue = PyString_AsString(PyObject_Str(pErrValue));
    traceback_list = PyObject_CallObject(format_exception, Py_BuildValue("(O,O,O)", pErrType, pErrValue, pErrTraceback));
    ereport(ERROR, (errmsg("Error in python: %s", errName),
                errdetail("%s", errValue),
                errdetail_log("%s", PyString_AsString(PyObject_CallObject(PyObject_GetAttrString(newline, "join"), Py_BuildValue("(O)", traceback_list))))));
}


static void
init_if_needed(){
    if ( TABLES_DICT == NULL ) {
        /* TODO: managed locks and things */
        Py_Initialize(  );
        TABLES_DICT = PyDict_New(  );
    }
}

PyObject*
multicorn_get_instance(Relation rel)
{
    PyObject       *pName, *pModule, *pArgs, *pOptions,
                   *pFunc, *pClass, *pObj, *pColumns,
                   *pTableId;
    Oid            tablerelid;
    char           *module;
    init_if_needed();
    tablerelid = RelationGetRelid( rel );
    pTableId = PyInt_FromSsize_t( tablerelid );
    pName = PyUnicode_FromString( "multicorn" );
    pModule = PyImport_Import( pName );
    multicorn_error_check();
    Py_DECREF( pName );
    if ( PyMapping_HasKey( TABLES_DICT, pTableId )) {
        pObj = PyDict_GetItem( TABLES_DICT, pTableId );
    } else {
        pOptions = PyDict_New(  );
        multicorn_get_options( tablerelid, pOptions, &module );
        if ( pModule != NULL ) {
            pFunc = PyObject_GetAttrString( pModule, "get_class" );
            multicorn_error_check();
            pArgs = PyTuple_New( 1 );
            pName = PyString_FromString( module );
            PyTuple_SetItem( pArgs, 0, pName );
            pClass = PyObject_CallObject( pFunc, pArgs);
            multicorn_error_check();

            Py_DECREF( pArgs);
            Py_DECREF( pFunc );
            pArgs = PyTuple_New( 2 );
            pColumns = PyList_New( 0 );
            multicorn_get_attributes_name( rel->rd_att, pColumns);
            PyTuple_SetItem( pArgs, 0, pOptions);
            PyTuple_SetItem( pArgs, 1, pColumns);
            /* Py_DECREF( pName ); -> Make the pg crash -> ??*/
            pObj = PyObject_CallObject( pClass, pArgs);
            multicorn_error_check();
            PyDict_SetItem( TABLES_DICT, pTableId, pObj );
            multicorn_error_check();
            Py_DECREF( pArgs);
            Py_DECREF( pOptions);
            Py_DECREF( pClass);
            Py_DECREF( pObj );
        } else {
            PyErr_Print(  );
            elog( ERROR, "Failed to load module" );
            return NULL;
        }
    }
    return pObj;
}


HeapTuple
BuildTupleFromCStringsWithSize(AttInMetadata *attinmeta, char **values, ssize_t* sizes)
{
    TupleDesc	tupdesc = attinmeta->tupdesc;
    int			natts = tupdesc->natts;
    Datum	   *dvalues;
    bool	   *nulls;
    int			i;
    HeapTuple	tuple, typeTuple;
    Oid typeoid;
    Oid element_type;

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
            if (typeoid == BYTEAOID || typeoid == TEXTOID){
                dvalues[i] = PointerGetDatum(cstring_to_text_with_len(values[i], sizes[i]));
                SET_VARSIZE(dvalues[i], sizes[i] + sizeof(ssize_t));
            } else {
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
multicorn_get_columns(List *columnslist, TupleDesc desc, PyObject *result){
    Expr *current_expr;
    ListCell *cell;
    foreach(cell, columnslist){
        current_expr = (Expr*) lfirst(cell);
        multicorn_get_column(current_expr, desc, result);
    }
}

void
multicorn_get_column(Expr* expr, TupleDesc desc, PyObject* list){
    ListCell *cell;
    char * key;
    if ( expr == NULL) {
        return ;
    }
    switch ( expr->type ) {

        case T_ExprState:
            multicorn_get_column(((ExprState *) expr)->expr, desc, list);
            break;

        case T_GenericExprState:
            multicorn_get_column(((GenericExprState*) expr)->xprstate.expr, desc, list);
            break;

        case T_FuncExprState:
            foreach(cell, ((FuncExprState*) expr)->args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_Var:
            key = NameStr( desc->attrs[((Var *)expr)->varattno-1]->attname );
            if (key != NULL){
                PySet_Add(list, PyString_FromString(key));
            }
            break;

        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
        case T_ScalarArrayOpExpr:
        case T_FuncExpr:
            foreach(cell, ((OpExpr*) expr)->args){
                multicorn_get_column((Expr*)lfirst(cell), desc, list);
            }
            break;

        case T_Const:
        case T_Param:
        case T_CaseTestExpr:
        case T_CoerceToDomainValue:
        case T_CurrentOfExpr:
            break;

        case T_TargetEntry:
            if (((TargetEntry *)expr)->resorigcol == 0){
                multicorn_get_column(((TargetEntry *)expr)->expr, desc, list);
            } else {
                key = NameStr( desc->attrs[((TargetEntry *)expr)->resorigcol-1]->attname );
                if (key != NULL) {
                    PySet_Add(list, PyString_FromString(key));
                }
            }
            break;

        case T_RelabelType:
            multicorn_get_column(((RelabelType *)expr)->arg, desc, list);
            break;

        case T_RestrictInfo:
            multicorn_get_column(((RestrictInfo *)expr)->clause, desc, list);
            break;

        case T_Aggref:
            foreach(cell, ((Aggref *)expr)->args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            foreach(cell, ((Aggref *)expr)->aggorder){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            foreach(cell, ((Aggref *)expr)->aggdistinct){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_WindowFunc:
            foreach(cell, ((WindowFunc *)expr)->args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_ArrayRef:
            foreach(cell, ((ArrayRef *)expr)-> refupperindexpr){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            foreach(cell, ((ArrayRef *)expr)-> reflowerindexpr){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            multicorn_get_column(((ArrayRef *)expr)->refexpr, desc, list);
            multicorn_get_column(((ArrayRef *)expr)->refassgnexpr, desc, list);
            break;

        case T_BoolExpr:
            foreach(cell, ((BoolExpr *)expr)->args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_SubPlan:
            foreach(cell, ((SubPlan *)expr)->args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_AlternativeSubPlan:
            // Do not really know what to do
            break;

        case T_NamedArgExpr:
            multicorn_get_column(((NamedArgExpr *)expr)->arg, desc, list);
            break;

        case T_FieldSelect:
            multicorn_get_column(((FieldSelect *)expr)->arg, desc, list);
            break;

        case T_CoerceViaIO:
            multicorn_get_column(((CoerceViaIO *)expr)->arg, desc, list);
            break;

        case T_ArrayCoerceExpr:
            multicorn_get_column(((ArrayCoerceExpr *)expr)->arg, desc, list);
            break;

        case T_ConvertRowtypeExpr:
            multicorn_get_column(((ConvertRowtypeExpr *)expr)->arg, desc, list);
            break;

        case T_CollateExpr:
            multicorn_get_column(((CollateExpr *)expr)->arg, desc, list);
            break;

        case T_CaseExpr:
            foreach(cell, ((CaseExpr *)expr)-> args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            multicorn_get_column(((CaseExpr *)expr)->arg, desc, list);
            multicorn_get_column(((CaseExpr *)expr)->defresult, desc, list);
            break;

        case T_CaseWhen:
            multicorn_get_column(((CaseWhen *)expr)->expr, desc, list);
            multicorn_get_column(((CaseWhen *)expr)->result, desc, list);
            break;

        case T_ArrayExpr:
            foreach(cell, ((ArrayExpr *)expr)->elements){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_RowExpr:
            foreach(cell, ((RowExpr *)expr)->args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_RowCompareExpr:
            foreach(cell, ((RowCompareExpr *)expr)->largs){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            foreach(cell, ((RowCompareExpr *)expr)->rargs){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_CoalesceExpr:
            foreach(cell, ((CoalesceExpr *)expr)->args ){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_MinMaxExpr:
            foreach(cell, ((MinMaxExpr *)expr)->args ){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_XmlExpr:
            foreach(cell, ((XmlExpr *)expr)->args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            foreach(cell, ((XmlExpr *)expr)->named_args){
                multicorn_get_column((Expr *)lfirst(cell), desc, list);
            }
            break;

        case T_NullTest:
            multicorn_get_column(((NullTest *)expr)->arg, desc, list);
            break;

        case T_BooleanTest:
            multicorn_get_column(((BooleanTest *)expr)->arg, desc, list);
            break;

        case T_CoerceToDomain:
            multicorn_get_column(((CoerceToDomain *)expr)->arg, desc, list);
            break;

        default:
            ereport(ERROR, 
                    (errmsg("Unknown node type %d", expr->type)));
    }
}
