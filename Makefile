MODULE_big   = multicorn
OBJS         =  src/errors.o src/python.o src/query.o src/multicorn.o


DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))

DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-language=plpgsql

SHLIB_LINK = $(python_libspec) $(python_additional_libs) $(filter -lintl,$(LIBS))
PG_CPPFLAGS  = $(python_includespec) $(CPPFLAGS)

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
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
