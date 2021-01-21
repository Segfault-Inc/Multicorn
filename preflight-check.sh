#!/bin/bash

PG_CONFIG=$(which pg_config)
PY_VERSION=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')

if [ -z "${PG_CONFIG}" ]; then
  echo "No pg_config found in your path."
  echo "Please check if you installed the PostgreSQL development packages."
  exit 1
fi

if [ ! -x "${PG_CONFIG}" ]; then
  echo "No pg_config found in your path."
  echo "Please check if you installed the PostgreSQL development packages."
  exit 1
fi

if ! hash python3; then
    echo "python3 is not installed"
    exit 2
fi

ver=$(python -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
if [ "$PY_VERSION" -lt "34" ]; then
    echo "This script requires python 3.4 or greater"
    exit 1
fi
