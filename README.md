# GFW Tile preprocessing

Preprocess GFW tiles for SPARK pipeline.

For existing rasters, make sure they are all saved as Cloud Optimized GeoTIFF (without overviews).

For vector layers, slice up original geometries with 10x10 degree grid and simplify to 0.0001 degree. Save as materialized view. Rasterize geometries to Cloud Optimized GeoTiff

Upload all tiles to S3.

# Usage

```bash
prep_tiles.py [-h]
              [--layer {loss,tcd,co2_pixel,primary_forest,ifl,gadm2,wdpa,plantations,logging,mining}]
```

# Add new layers or update existing once
You can define layers in the `SRC` variable in `prep_tiles.py`.

Raster layers have the following options:
 - `type`: Must by "raster"
 - `src`: path to source file. For remote files on s3 use GDAL synax `/vsis3/...`
 - `target`: path to target location on S3. Use AWS syntax `S3://...`,
 - `data_type`: GDAL pixel type `Byte`, `UInt16`, ...
 -  `nodata`: No data value for output file


Vector layers have the following options:
 - `type`: Must by "vector"
 - `src`: path to source file. For remote files on s3 use GDAL synax `/vsis3/...`
 - `target`: path to target location on S3. Use AWS syntax `S3://...`,
 - `data_type`: GDAL pixel type `Byte`, `UInt16`, ...
 -  `nodata`: No data value for output file
 - `oid`: ID field which should be used for output raster. Use `None` if you want to create a binary raster.


