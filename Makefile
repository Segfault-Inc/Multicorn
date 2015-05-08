srcdir       = .
MODULE_big   = multicorn
OBJS         =  src/errors.o src/python.o src/query.o src/multicorn.o


DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))

DOCS         = $(wildcard $(srcdir)/doc/*.md)

EXTENSION    = multicorn
EXTVERSION   = $(shell grep default_version $(srcdir)/$(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

all: preflight-check sql/$(EXTENSION)--$(EXTVERSION).sql

directories.stamp:
	[ -d sql ] || mkdir sql
	[ -d src ] || mkdir src
	touch $@

$(OBJS): directories.stamp

install: python_code 

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql directories.stamp
	cp $< $@

preflight-check:
	$(srcdir)/preflight-check.sh

python_code: setup.py
	cp $(srcdir)/setup.py ./setup--$(EXTVERSION).py
	sed -i -e "s/__VERSION__/$(EXTVERSION)-dev/g" ./setup--$(EXTVERSION).py
	$(PYTHON) ./setup--$(EXTVERSION).py install
	rm ./setup--$(EXTVERSION).py

rpm: setup.py
	@echo "About to make RPM"
	cp $(srcdir)/setup.py ./setup--$(EXTVERSION).py
	sed -i -e "s/__VERSION__/$(EXTVERSION)-dev/g" ./setup--$(EXTVERSION).py
	$(PYTHON) ./setup--$(EXTVERSION).py bdist --format=rpm
	rm ./setup--$(EXTVERSION).py


release-zip: all
	git archive --format zip --prefix=multicorn-$(EXTVERSION)/ --output ./multicorn-$(EXTVERSION).zip HEAD
	unzip ./multicorn-$(EXTVERSION).zip
	rm ./multicorn-$(EXTVERSION).zip
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./multicorn-$(EXTVERSION)/META.json  ./multicorn-$(EXTVERSION)/setup.py  ./multicorn-$(EXTVERSION)/python/multicorn/__init__.py
	zip -r ./multicorn-$(EXTVERSION).zip ./multicorn-$(EXTVERSION)/
	rm ./multicorn-$(EXTVERSION) -rf

coverage:
	lcov -d . -c -o lcov.info
	genhtml --show-details --legend --output-directory=coverage --title=PostgreSQL --num-spaces=4 --prefix=./src/ `find . -name lcov.info -print`

DATA = sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql ./multicorn-$(EXTVERSION).zip directories.stamp
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
REGRESS      = virtual_tests

include $(PGXS)

with_python_no_override = no

ifeq ($(with_python),yes)
	with_python_no_override = yes
endif

ifdef PYTHON_OVERRIDE
	with_python_no_override = no
endif


ifeq ($(with_python_no_override),yes)
	SHLIB_LINK = $(python_libspec) $(python_additional_libs) $(filter -lintl,$(LIBS))
	override CPPFLAGS := -I. -I$(srcdir) $(python_includespec) $(CPPFLAGS)
	override PYTHON = python${python_version}
else
	ifdef PYTHON_OVERRIDE
		override PYTHON = ${PYTHON_OVERRIDE}
	endif

	ifeq (${PYTHON}, )
		override PYTHON = python
	endif


	python_version = $(shell ${PYTHON} --version 2>&1 | cut -d ' ' -f 2 | cut -d '.' -f 1-2)
	PYTHON_CONFIG ?= python${python_version}-config

	PY_LIBSPEC = $(shell ${PYTHON_CONFIG} --libs)
	PY_INCLUDESPEC = $(shell ${PYTHON_CONFIG} --includes)
	PY_CFLAGS = $(shell ${PYTHON_CONFIG} --cflags)
	PY_LDFLAGS = $(shell ${PYTHON_CONFIG} --ldflags)
	SHLIB_LINK += $(PY_LIBSPEC) $(PY_LDFLAGS) $(PY_ADDITIONAL_LIBS) $(filter -lintl,$(LIBS))
	override PG_CPPFLAGS  := $(PY_INCLUDESPEC) $(PG_CPPFLAGS)
	override CPPFLAGS := $(PG_CPPFLAGS) $(CPPFLAGS)
endif

ifeq ($(PORTNAME),darwin)
	override LDFLAGS += -undefined dynamic_lookup -bundle_loader $(shell $(PG_CONFIG) --bindir)/postgres
endif

PYTHON_TEST_VERSION ?= $(python_version)
PG_TEST_VERSION ?= $(MAJORVERSION)
SUPPORTS_WRITE=$(shell expr ${PG_TEST_VERSION} \>= 9.3)
SUPPORTS_IMPORT=$(shell expr ${PG_TEST_VERSION} \>= 9.5)

TESTS        = $(wildcard test-$(PYTHON_TEST_VERSION)/sql/multicorn*.sql)
ifeq (${SUPPORTS_WRITE}, 1)
  TESTS += $(wildcard test-$(PYTHON_TEST_VERSION)/sql/write*.sql)
endif
ifeq (${SUPPORTS_IMPORT}, 1)
  TESTS += $(wildcard test-$(PYTHON_TEST_VERSION)/sql/import*.sql)
endif

REGRESS      = $(patsubst test-$(PYTHON_TEST_VERSION)/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test-$(PYTHON_TEST_VERSION) --load-language=plpgsql

$(info Python version is $(python_version))
