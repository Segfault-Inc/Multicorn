FROM postgres:11
RUN apt-get update && apt-get install -y \
	build-essential \
	clang \
	libpython2.7-dev \
	python2.7-dev \
	postgresql-client-11 \
	postgresql-server-dev-11 \
	postgresql-plpython-11 \
	python-setuptools

ENV PGUSER postgres
ENV PYTHON_OVERRIDE python2.7
ENV LC_ALL C.UTF-8
