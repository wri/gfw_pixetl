# GFW pixETL

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/bcfab5fc23544a389081825d3e2420fa)](https://www.codacy.com/gh/wri/gfw_pixetl/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=wri/gfw_pixetl&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/bcfab5fc23544a389081825d3e2420fa)](https://www.codacy.com/gh/wri/gfw_pixetl/dashboard?utm_source=github.com&utm_medium=referral&utm_content=wri/gfw_pixetl&utm_campaign=Badge_Coverage)

PixETL reads raster and vector source data and converts data into Cloud Optimized GeoTIFF (without overviews) clipped to specified grid sizes.

It will upload all tiles to GFW data lake following GFW naming convention.

# Installation

`./scripts/setup`

# Dependencies
- GDAL 2.4.x or 3.x
- libpq-dev

# Usage

```bash
 ██████╗ ███████╗██╗    ██╗    ██████╗ ██╗██╗  ██╗███████╗████████╗██╗
██╔════╝ ██╔════╝██║    ██║    ██╔══██╗██║╚██╗██╔╝██╔════╝╚══██╔══╝██║
██║  ███╗█████╗  ██║ █╗ ██║    ██████╔╝██║ ╚███╔╝ █████╗     ██║   ██║
██║   ██║██╔══╝  ██║███╗██║    ██╔═══╝ ██║ ██╔██╗ ██╔══╝     ██║   ██║
╚██████╔╝██║     ╚███╔███╔╝    ██║     ██║██╔╝ ██╗███████╗   ██║   ███████╗
 ╚═════╝ ╚═╝      ╚══╝╚══╝     ╚═╝     ╚═╝╚═╝  ╚═╝╚══════╝   ╚═╝   ╚══════╝

Usage: pixetl [OPTIONS] LAYER_JSON

  LAYER_JSON: Layer specification in JSON

Options:
  -d, --dataset TEXT                        Name of dataset to process  [required]
  -v, --version TEXT                        Version of dataset to process  [required]
  --subset TEXT                             Subset of tiles to process
  -o, --overwrite                           Overwrite existing tile in output location
  --help                                    Show this message and exit.
```

## Example

```bash
pixetl -d umd_tree_cover_density_2000 -v v1.6 '{"source_type": "raster", "pixel_meaning": "percent", "data_type": "uint8", "nbits": 7, "grid": "10/40000", "source_uri": "s3://gfw-files/2018_update/tcd_2000/tiles.geojson", "resampling": "average"}'
```

## Layer JSON
You define layer sources in JSON as the one required argument

Layer source definitions follow this pattern

```json
{
    "option": "value",
    ...
}
```

Supported Options:

### Raster Sources:

| Option            | Mandatory | Description |
|-------------------|-----------|-------------|
| source_type       | yes       | Always "raster" |
| pixel_meaning     | yes       | A string indicating the value represented by pixel. This can either be a field name or a unit. Always use lower caps, unless when specifying a unit that uses capital letters |
| data_type         | yes       | Data type of output file (boolean, uint8, int8, uint16, int16, uint32, int32, float32, float64) |
| grid              | yes       | Grid size of output dataset
| no_data           | no        | Integer value, for float datatype use `NAN`. If left out or set to `null` output dataset will have no `no_data` value |
| nbits             | no        | Max number of bits used for given datatype |
| source_uri        | yes       | List of URIs of source folders or tiles.geojson file(s) |
| resampling        | no        | Resampling method (nearest, mod, avg, etc), default `nearest |
| calc              | no        | Numpy expression to transform array. Use namespace `np`, not `numpy` when using numpy functions. When using multiple input bands, reference each band with uppercase letter in alphabetic order (A,B,C,..). To output multiband raster, wrap list of bands in a masked array ie `np.ma.array([A, B, C])`. |
| symbology         | no        | Add optional symbology to the output raster |
| compute_stats     | no        | Compute band statistics and add to tiles.geojson |
| compute_histogram | no        | Compute band histograms and add to tile.geojson |
| process_locally   | no        | When set to True, forces PixETL to download all source files prior to processing. Default `False` |
| photometric       | no        | Color interpretations of bands |

_NOTE:_

File listed in `source_uri` must be stored on S3 and accessible to PixETL. The file path must use the s3 protocol (`s3://`).
File content must be of format `geoJSON`. The geojson must contain a `FeatureColletion` where each feature represents one geoTIFF file.
The feature geometry describes the extent of the geoTIFF, the property `name` the path to the geotiff using GDAL `vsi` notation.
You can reference file hosted on S3 (`/vsis3/`), GCS  (`/vsigs/`) or anywhere else accessible over http  (`/vsicurl/`)
You can use the `pixetl_prep` script to generate the tile.geojson file.

GeoTIFFs hosted on S3 must be accessible by the AWS profile used by PixETL.
When referencing geotiffs hosted on GCS, you must set the ENV variable `GOOGLE_APPLICATION_CREDENTIALS` which points to
a `json` file in the file system which holds the GCS private key of the google service account you will use to access the data.

You can store the private key in AWS Secret Manager. In that case set `AWS_GCS_KEY_SECRET_ARN` to specify the secret id
together with `GOOGLE_APPLICATION_CREDENTIALS`. PixETL with then attempt to download the private key from AWS Secret Manager
and store it the `json` file specified.

**Goolge Cloud Storage support is experimental only. It __should__ work as documented, but we don't have the tools in place
to fully test this feature locally. The only way we can test right now is with integration tests after we deployed code in staging.
For local tests to past __AWS_GCS_KEY_SECRET_ARN__ must NOT be set as we currently have issues running tests with a second moto server on Github Actions.**

For example here is a pretty-printed sample raster layer definition followed
by the command that one would issue to process it:
```json
{
     "source_type": "raster",
     "pixel_meaning": "percent",
     "data_type": "uint8",
     "nbits": 7,
     "grid": "10/40000",
     "source_uri": "s3://gfw-files/2018_update/tcd_2000/tiles.geojson",
     "resampling": "average"
 }
```

### Vector Sources

| Option            | Mandatory | Description |
|-------------------|-----------|-------------|
| source_type       | yes       | Always "vector" |
| pixel_meaning     | yes       | Field in source table used for pixel value |
| data_type         | yes       | Data type of output file (boolean, uint, int, uint16, int16, uint32, int32, float32, float64) |
| grid              | yes       | Grid size of output dataset
| no_data           | no        | Integer value to use for no data value. |
| nbits             | no        | Max number of bits used for given datatype |
| order             | no        | How to order field values of source table (asc, desc) |
| rasterize_method  | no        | How to rasterize tile (value or count). `value` uses value from table, `count` counts number of features intersecting with pixel |
| calc              | no        | PostgreSQL expression (ie `CASE` to use to reformat input values |
| symbology         | no        | Add optional symbology to the output raster |
| compute_stats     | no        | Compute band statistics and add to tiles.geojson |
| compute_histogram | no        | Compute band histograms and add to tile.geojson |

_NOTE_:

Source files must be loaded into a PostgreSQL database prior to running this pipeline.
PixETL will look for a PostgreSQL schema named after the `dataset` and a table named after the `version`.
Make sure, geometries are of type `Polygon` or `MultiPolygon` and valid before running PixETL.

PixETL will look for a field named after `pixel_meaning` parameter. This field must be a field of type integer.
If you need to reference a non-integer field, make use of the `calc` parameter. Use a PostGreSQL `CASE` expression
to map desires field values to integer values.

When using vector sources, PixETL will need access to the PostgreSQL database.
Use the standard [PostgreSQL environment variables](https://www.postgresql.org/docs/11/libpq-envars.html) to configure the connection.

## Run with Docker

This is probably the easiest way to run PixETL locally since you won't need to install any of the required dependencies.
The master branch of this repo is linked to Dockerhub image `globalforestwatch/pixetl:latest`.
You can either pull from here, or build you own local image using the provided dockerfile.

```bash
docker build . -t globalforestwatch/pixetl
```

Make sure you map a local directory to container's /tmp directory if you want to monitor temporary files created.
Make sure you set all required ENV vars (see above).
Also make sure that your docker container has all the required AWS privileges.

```bash
docker run -it -v /tmp:/tmp -v $HOME/.aws:/root/.aws:ro -e AWS_PROFILE=gfw-dev globalforestwatch/pixetl [OPTIONS] NAME  # pragma: allowlist secret
```

## RUN in AWS Batch

The terraform module in this repo will add a PixETL owned compute environment to AWS Batch.
The compute environment will make use of EC2 instance which come with ephemeral storage
(ie instance of the instance families `r5d`, `r5ad`, `r5nd`).
A bootstrap script on the instance will mount one of the ephemeral storage devices as folder `/tmp`.
A second ephemeral storage device (if available) will be mounted as swap space.
Any other available ephemeral storage device of the instance will be ignored.

The swap space is only a safety net.
AWS Batch kills a tasks without further notice if it uses more than the allocated memory.
The swap space allows the batch job to use more memory than allocated. However, the job will become VERY slow.
It is hence always the best strategy to keep the Memory/CPU ration high, in particular when working with `float` data types.

Terraform will also create a PixETL job definition, which reference the docker image (hosted on AWS ECR)
and set required ENV variables for the container.
In case you want to run jobs using a vector source, you will have to set the PostgreSQL ENV vars manually.

The job definition also maps the `/tmp` volume of the host ec2 instance to the `/tmp` folder of the docker container.
Hence, any data written to `/tmp` inside the docker container will persist on the ec2 instance.

PixETL will create a subfolder in `/tmp` using the BatchJOB ID to name space the data of a given job. \
PixETL will write all temporary for a given job into that subfolder.
It will also clean up temporary data during runtime to avoid filling up the disc space.
This strategy should avoid running out of disc space. However, in some scenarios you might still experience issues.
For example if multiple jobs where killed by Batch (due to using too much memory) before PixETL was able to clean up.
The EC2 instance will stay available of other scheduled jobs and if this happens multiple times, the discs fills up,
and eventually you run out of space.

The AWS IAM role used for the docker container should have all the required permissions to run PixETL.

When creating a new PixETL job, it will be easiest to specify the job parameter using the JSON format,
not the default space-separated format. The entrypoint of the docker image used is `pixetl` and you will only need to
specify the CLI options and arguments, not the binary itself.
When using the JSON format, you will have to escape the quotes inside= the `LAYER_OPTION` objects

`["-d", "umd_tree_cover_density_2000", "-v", "v1.6", "{\"source_type\": \"raster\", \"pixel_meaning\": \"percent\", \"data_type\": \"uint8\", \"nbits\": 7, \"grid\": \"10/40000\", \"source_uri\": \"s3://gfw-files/2018_update/tcd_2000/tiles.geojson\", \"resampling\": \"average\"}"]`

# pixetl_prep
