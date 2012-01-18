EXTENSION    = multicorn
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
PYEXEC = python
PYmajor 		 = $(shell python -V 2>&1 | cut -d " " -f 2  | cut -d "." -f 1)
ifeq ($(PYmajor), 3)
	PYEXEC = python2
endif
PYVERSION    = 2.7
DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))
DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-language=plpgsql
MODULE_big     = multicorn
OBJS = src/multicorn.o
SHLIB_LINK   = -lpython$(PYVERSION)
PG_CONFIG    = `which pg_config`
PG91         = $(shell $(PG_CONFIG) --version | grep -qE " 8\.| 9\.0" && echo no || echo yes)
PG_CPPFLAGS  = -I/usr/include/python$(PYVERSION) $(python_includespec) $(CPPFLAGS)
PROFILE      = -lpython$(PYVERSION)
ifeq ($(PG91),yes)
all: sql/$(EXTENSION)--$(EXTVERSION).sql

ifndef NO_PYTHON
install: python_code
endif

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

python_code: setup.py
	$(PYEXEC) ./setup.py install

release-zip: all
	git archive --format zip --prefix=multicorn-$(EXTVERSION)/ --output ./multicorn-$(EXTVERSION).zip master

DATA = $(wildcard sql/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
