# GFW Pixel ETL


Reads source files and converts data into Cloud Optimized GeoTIFF (without overviews) clipped to specified grid size.

Upload all tiles to GFW data lake following GFW naming convention.

Raster sources: If source layer consists of multiple tiles you must provide a URI to a VRT which includes all tiles. Make sure the extent of the VRT is align with desired output grid.

Vector sources: Source files must be already loaded into Postgres/ Aurora database. Geometries must be validated and clipped to your output grid. There must be a column present called tile_id__{grid} which lists in which tile given geometry falls. You must make sure that the value field you specify is a numeric field. Values will be used while rasterizing geometry.

# Usage

CLI
```
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
  --help                                    Show this message and exit.
```

# Data sources
You can define layer sources in `.yaml` files located in `fixures/` sub directory.

Layer sources definition follow this pattern

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
|field| yes | Field/ Value represented by pixel |
| src_uri | yes | URI of source file on S3 |
| data_type | yes | data type of output file (boolean, uint, int, uint16, int16, uint32, int32, float32, float64) |
| no_data | no | No data value |
| nbits | no | Max number of bits used for given datatype |
| resampling | no | resampling method (nearest, mod, avg, etc) |
| single_tile | no | source file is single file |


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