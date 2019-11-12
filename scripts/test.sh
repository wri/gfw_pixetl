#!/usr/bin/env bash

#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
#pixetl treecover_density_2010 -v v1.6 -s raster -f threshold --subset 50S_000E
#


docker build . -t globalforestwatch/pixetl
docker run -v /tmp:/tmp --entrypoint pytest globalforestwatch/pixetl
