#!/bin/bash

PG_CONFIG=$(which pg_config)
PY_VERSION=$(python --version 2>&1 | awk '{ print substr($2,1,3)}')

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

if [ ${PY_VERSION} != "2.7" ]; then
  echo "Found Python $PY_VERSION, but 2.7 is required."
  exit 2
fi
