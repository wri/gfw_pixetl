#!/usr/bin/env bash

docker build . -t globalforestwatch/pixetl
docker tag globalforestwatch/pixetl:latest globalforestwatch/pixetl:pre
docker push globalforestwatch/pixetl