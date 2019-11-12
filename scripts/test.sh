#!/usr/bin/env bash

#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
#pixetl treecover_density_2010 -v v1.6 -s raster -f threshold --subset 50S_000E
#
set -e

cur_dir=$(dirname "$0")
${cur_dir}/build.sh

docker run -e AWS_ACCESS_KEY=${{ secrets.aws_key_dev }} -e AWS_SECRET_ACCESS_KEY=${{ secrets.aws_secret_dev }} -v /tmp:/tmp --entrypoint pytest globalforestwatch/pixetl  # pragma: allowlist secret
