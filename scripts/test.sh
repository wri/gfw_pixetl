#!/usr/bin/env bash

#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
#pixetl treecover_density_2010 -v v1.6 -s raster -f threshold --subset 50S_000E
#
set -e


while getopts k:s: option
do
case "${option}"
in
k) AWS_ACCESS_KEY_ID=${OPTARG};;
s) AWS_SECRET_ACCESS_KEY=${OPTARG};;
esac
done

cur_dir=$(dirname "$0")
${cur_dir}/build.sh

docker run -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} -v /tmp:/tmp --entrypoint pytest globalforestwatch/pixetl  --cov-report term --cov-report xml --cov=gfw_pixetl tests/  # pragma: allowlist secret
