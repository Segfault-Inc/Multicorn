FROM postgres:10
RUN apt-get update && apt-get install -y \
	build-essential \
	clang \
	libpython3.5-dev \
	python3.5-dev \
	postgresql-client-10 \
	postgresql-server-dev-10 \
	postgresql-plpython3-10 \
	python3-setuptools

ENV PGUSER postgres
ENV PYTHON_OVERRIDE python3.5
ENV LC_ALL C.UTF-8
