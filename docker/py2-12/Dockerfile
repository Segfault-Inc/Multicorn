FROM postgres:12
RUN apt-get update && apt-get install -y \
	build-essential \
	clang \
	libpython2.7-dev \
	python2.7-dev \
	postgresql-client-12 \
	postgresql-server-dev-12 \
	python-setuptools

ENV PGUSER postgres
ENV PYTHON_OVERRIDE python2.7
ENV LC_ALL C.UTF-8
