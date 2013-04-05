EXTENSION    = multicorn
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
PYEXEC = python
PYmajor 		 = $(shell python -V 2>&1 | cut -d " " -f 2  | cut -d "." -f 1)
ifeq ($(PYmajor), 3)
	PYEXEC = python2
endif
PYVERSION    = 2.7
PY_CONFIG    = $(shell which python$(PYVERSION)-config)
PY_INCLUDES  = $(shell $(PY_CONFIG) --includes)
DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))
DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-language=plpgsql
MODULE_big     = multicorn
OBJS =  src/errors.o src/python.o src/query.o src/multicorn.o
SHLIB_LINK = $(shell $(PY_CONFIG) --ldflags)
PG_CONFIG    = `which pg_config`
PG91         = $(shell $(PG_CONFIG) --version | grep -qE " 8\.| 9\.0" && echo no || echo yes)
PG_CPPFLAGS  = $(PY_INCLUDES) $(python_includespec) $(CPPFLAGS)
PROFILE      = -lpython$(PYVERSION)
ifeq ($(PG91),yes)
all: preflight-check sql/$(EXTENSION)--$(EXTVERSION).sql

ifndef NO_PYTHON
install: python_code
endif

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

preflight-check:
	./preflight-check.sh

python_code: setup.py
	cp ./setup.py ./setup--$(EXTVERSION).py
	sed -e "s/__VERSION__/$(EXTVERSION)-dev/g" -i ./setup--$(EXTVERSION).py
	$(PYEXEC) ./setup--$(EXTVERSION).py install
	rm ./setup--$(EXTVERSION).py

release-zip: all
	git archive --format zip --prefix=multicorn-$(EXTVERSION)/ --output ./multicorn-$(EXTVERSION).zip HEAD
	unzip ./multicorn-$(EXTVERSION).zip
	rm ./multicorn-$(EXTVERSION).zip
	sed -e "s/__VERSION__/$(EXTVERSION)/g" -i  ./multicorn-$(EXTVERSION)/META.json  ./multicorn-$(EXTVERSION)/setup.py  ./multicorn-$(EXTVERSION)/python/multicorn/__init__.py
	zip -r ./multicorn-$(EXTVERSION).zip ./multicorn-$(EXTVERSION)/
	rm ./multicorn-$(EXTVERSION) -rf

DATA = $(wildcard sql/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql ./multicorn-$(EXTVERSION).zip
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
