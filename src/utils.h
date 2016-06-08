



#ifndef PG_UTILS_H
#define PG_UTILS_H


struct module_state
{
	PyObject   *error;
};

static PyObject * log_to_postgres(PyObject *self, PyObject *args, PyObject *kwargs);
static PyObject * py_check_interrupts(PyObject *self, PyObject *args, PyObject *kwargs);

#if PY_MAJOR_VERSION >= 3
PyObject * PyInit__utils(void);
#else
void init_utils(void);
#endif


#endif
