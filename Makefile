EXTENSION    = multicorn
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

PYVERSION    = $(shell python2 -V 2>&1 | cut -d " " -f 2  | cut -d "." -f 1,2)
DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))
DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-language=plpgsql
MODULES      = $(patsubst %.c,%,$(wildcard src/multicorn.c))
PG_CONFIG    = `which pg_config`
PG91         = $(shell $(PG_CONFIG) --version | grep -qE " 8\.| 9\.0" && echo no || echo yes)
PG_CPPFLAGS  = -I/usr/include/python$(PYVERSION) $(python_includespec) $(CPPFLAGS)
PROFILE      = -lpython$(PYVERSION)
ifeq ($(PG91),yes)
all: sql/$(EXTENSION)--$(EXTVERSION).sql

install: python_code


sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

python_code: setup.py
	python2 ./setup.py install

release-zip: all
	git archive --format zip --prefix=multicorn-$(EXTVERSION)/ --output ./multicorn-$(EXTVERSION).zip master

DATA = $(wildcard sql/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
