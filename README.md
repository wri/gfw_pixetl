# GFW pixETL

[![Maintainability](https://api.codeclimate.com/v1/badges/6eacebaf99305fb1bd1b/maintainability)](https://codeclimate.com/github/wri/gfw_pixetl/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/6eacebaf99305fb1bd1b/test_coverage)](https://codeclimate.com/github/wri/gfw_pixetl/test_coverage)

Reads source files and converts data into Cloud Optimized GeoTIFF (without overviews) clipped to specified grid size.

Upload all tiles to GFW data lake following GFW naming convention.

### Raster sources
If source layer consists of multiple tiles you must provide a URI to a VRT which includes all tiles. Make sure the extent of the VRT is align with desired output grid.

### Vector sources
Source files must be loaded into Postgres/ Aurora database prior to running this pipeline. Geometries must be validated and clipped to your output grid. There must be a column present called tile_id__{grid} which lists in which tile given geometry falls. You must make sure that the value field you specify is a numeric field. Values will be used while rasterizing geometry.

# Usage

### CLI
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

### Docker
Make sure you map a local directory to container's /tmp directory.

Also add you aws credentials as environment variables.

Use same Options and Name as listed above
```bash

docker build . -t globalforestwatch/pixetl
docker run -it -v /tmp:/tmp -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=xxx globalforestwatch/pixetl [OPTIONS] NAME  # pragma: allowlist secret

```

### AWS Batch

Create a new Job Definition and add it to the pixETL Job Queue. This will make sure you will use a EC2 instance with ephemeral-storage.
Link to globalforestwatch/pixetl docker container on Docker hub

```
PixETLDefinition:
    Properties:
      ContainerProperties:
        MountPoints:
          - ContainerPath: /tmp
            SourceVolume: /tmp
        Volumes:
          - Host:
              SourcePath: /tmp
            Name: /tmp

```

# Data sources
You define layer sources in JSON as the one required argument

Layer source definitions follow this pattern

```json
{
    "option": "value",
    ...
}
```

Supported Options:

Raster Sources:

| Option        | Mandatory | Description |
|---------------|-----------|-------------|
| source_type   | yes       | Always "raster" |
| pixel_meaning | yes       | Field/ Value represented by pixel |
| data_type     | yes       | Data type of output file (boolean, uint, int, uint16, int16, uint32, int32, float32, float64) |
| grid          | yes       | Grid size of output dataset
| no_data       | no        | No data value (true or false)|
| nbits         | no        | Max number of bits used for given datatype |
| uri           | yes       | URI of source file on S3 |
| resampling    | no        | Resampling method (nearest, mod, avg, etc) |
| calc          | no        | Numpy calculation to be performed on the tile. Use same syntax as for [gdal_calc](https://gdal.org/programs/gdal_calc.html) . Refer to tile as `A` |

Vector Sources

| Option           | Mandatory | Description |
|------------------|-----------|-------------|
| source_type      | yes       | Always "vector" |
| pixel_meaning    | yes       | Field in source table used for pixel value |
| data_type        | yes       | Data type of output file (boolean, uint, int, uint16, int16, uint32, int32, float32, float64) |
| grid             | yes       | Grid size of output dataset
| no_data          | no        | No data value (true or false)|
| nbits            | no        | Max number of bits used for given datatype |
| order            | no        | How to order field values of source table (asc, desc) |
| rasterize_method | no        | How to rasterize tile (value or count). `value` uses value from table, `count` counts number of features intersecting with pixel |

For example here is a pretty-printed sample raster layer definition followed
by the command that one would issue to process it:
{
     "source_type": "raster",
     "pixel_meaning": "percent",
     "data_type": "uint",
     "nbits": 7,
     "grid": "1/4000",
     "uri": "s3://gfw-files/2018_update/tcd_2000/tiles.geojson",
     "resampling": "average",
 }

```bash
pixetl -d umd_tree_cover_density_2000 -v v1.6 '{"source_type": "raster", "pixel_meaning": "percent", "data_type": "uint", "nbits": 7, "grid": "1/4000", "uri": "s3://gfw-files/2018_update/tcd_2000/tiles.geojson", "resampling": "average"}'
```

# Extending ETL

Don't find the ETL you are looking for?

Create a subclass of Layer and write your own ETL.
