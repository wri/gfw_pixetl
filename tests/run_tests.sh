#!/usr/bin/env bash

set -e

#python /usr/local/app/tests/startup.py
/usr/local/app/wait_for_postgres.sh pytest -vv --cov-report term --cov-report xml:/usr/local/app/tests/cobertura.xml --cov=gfw_pixetl "$@"
