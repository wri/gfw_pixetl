#!/usr/bin/env bash

cur_dir=$(dirname "$0")
${cur_dir}/build.sh

docker tag globalforestwatch/pixetl:latest globalforestwatch/pixetl:pre
docker push globalforestwatch/pixetl