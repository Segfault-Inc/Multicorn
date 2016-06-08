#include "multicorn.h"
#include "bytesobject.h"
#include "access/xact.h"


#ifndef PG_ERRORS_H
#define PG_ERRORS_H

void errorCheck(void);
void reportException(PyObject *pErrType, PyObject *pErrValue, PyObject *pErrTraceback);

#endif
