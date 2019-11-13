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

Usage: pixetl [OPTIONS] NAME

  NAME: Name of dataset

Options:
  -v, --version TEXT                        Version of dataset
  -s, --source_type [raster|vector]         Type of input file(s)
  -f, --field TEXT                          Field represented in output dataset
  -g, --grid_name [3x3|10x10|30x30|90x90]   Grid size of output dataset
  -e, --env [dev|prod]                      Environment
  -o, --overwrite                           Overwrite existing tile in output location
  -d, --debug                               Log debug messages
  -w, --cwd                                 Work directory (default /tmp)
  --help                                    Show this message and exit.
```

### Docker
Make sure you map a local directory to container's /tmp directory.

Also add you aws credentials as environment variables.

Use dame Options and Name as listed above
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
You can define layer sources in `.yaml` files located in `fixures/` sub directory.

Layer source definitions follow this pattern

```yaml
wdpa_protected_areas:           # Layer name
    -                           # Default source description
        field: iucn_cat  
        order: desc  
        data_type: uint  
        nbits: 2  
     -                          # Optional alternative source description
        field: is
        ...

```

Supported Options:

Raster Sources:

| Option | Mandatory | Description |
|--------|-----------|-------------|
| field | yes | Field/ Value represented by pixel |
| src_uri | yes | URI of source file on S3 |
| data_type | yes | data type of output file (boolean, uint, int, uint16, int16, uint32, int32, float32, float64) |
| no_data | no | No data value |
| nbits | no | Max number of bits used for given datatype |
| resampling | no | resampling method (nearest, mod, avg, etc) |
| single_tile | no | source file is single file |
| calc | no | Numpy calculation to be performed on the tile. Use same syntax as for [gdal_calc](https://gdal.org/programs/gdal_calc.html) . Refer to tile as `A` |

Vector Sources

| Option | Mandatory | Description |
|--------|-----------|-------------|
| field| yes | Field in source table used for pixel value |
| data_type | yes | data type of output file (boolean, uint, int, uint16, int16, uint32, int32, float32, float64) |
| no_data | no | No data value |
| nbits | no | Max number of bits used for given datatype |
| order | no | how to order field values of source table (asc, desc) |
| rasterize_method | no | how to rasterize tile (value/ count). `Value` uses value from table, `count` counts number of features intersecting with pixel |


# Extending ETL

Don't find the ETL you are looking for?

Create a subclass of Layer and write your own ETL.