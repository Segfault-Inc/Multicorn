MODULE_big   = multicorn
OBJS         =  src/errors.o src/python.o src/query.o src/multicorn.o


DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))

DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-language=plpgsql



EXTENSION    = multicorn
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

all: preflight-check sql/$(EXTENSION)--$(EXTVERSION).sql

install: python_code

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

preflight-check:
	./preflight-check.sh


python_code: setup.py
	cp ./setup.py ./setup--$(EXTVERSION).py
	sed -i -e "s/__VERSION__/$(EXTVERSION)-dev/g" ./setup--$(EXTVERSION).py
	$(PYTHON) ./setup--$(EXTVERSION).py install
	rm ./setup--$(EXTVERSION).py

release-zip: all
	git archive --format zip --prefix=multicorn-$(EXTVERSION)/ --output ./multicorn-$(EXTVERSION).zip HEAD
	unzip ./multicorn-$(EXTVERSION).zip
	rm ./multicorn-$(EXTVERSION).zip
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./multicorn-$(EXTVERSION)/META.json  ./multicorn-$(EXTVERSION)/setup.py  ./multicorn-$(EXTVERSION)/python/multicorn/__init__.py
	zip -r ./multicorn-$(EXTVERSION).zip ./multicorn-$(EXTVERSION)/
	rm ./multicorn-$(EXTVERSION) -rf

DATA = $(wildcard sql/*--*.sql)
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql ./multicorn-$(EXTVERSION).zip
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifdef PYTHON_OVERRIDE
	override PYTHON = ${PYTHON_OVERRIDE}
endif

ifeq (${PYTHON}, )
	override PYTHON = python
endif

PY_VERSION = $(shell ${PYTHON} --version 2>&1 | awk '{ print substr($$2,1,3)}')
PYTHON_CONFIG ?= python-config-${PY_VERSION}
PY_LIBSPEC = $(shell ${PYTHON_CONFIG} --libs)
PY_INCLUDESPEC = $(shell ${PYTHON_CONFIG} --includes)

SHLIB_LINK = $(PY_LIBSPEC) $(PY_ADDITIONAL_LIBS) $(filter -lintl,$(LIBS))
PG_CPPFLAGS  = $(PY_INCLUDESPEC) $(CPPFLAGS)
override CPPFLAGS := $(PG_CPPFLAGS) $(CPPFLAGS)
