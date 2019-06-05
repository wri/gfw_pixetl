# GFW Tile preprocessing

Preprocess GFW tiles for SPARK pipeline.

For existing rasters, make sure they are all saved as Cloud Optimized GeoTIFF (without overviews).

For vector layers, slice up original geometries with 10x10 degree grid and simplify to 0.0001 degree. Save as materialized view. Rasterize geometries to Cloud Optimized GeoTiff

Upload all tiles to S3.

# Usage

## On a spot machine
`git clone https://github.com/wri/gfw_tile_prep` -- cloned repo fine
`cd gfw_tile_prep` -- changed directory fine
`git checkout carbon_flux_model` -- switched to carbon_flux_model branch fine
`cd gfw_tile_prep` -- changed directory fine
`python3 prep_tiles.py --layer gross_annual_removals_carbon` -- did not run correctly because it doesn't have the required packages installed
`pip install -e .` -- installing the required packages doesn't work in this folder
`cd ..` -- step out one folder to install required packages
`pip install -e .` -- did not complete successfully because of an error installing parellelpipe
`ps aux | grep -i apt` -- for some reason, this showed only `ubuntu    26685  0.0  0.0  12944   916 pts/0    S+   01:42   0:00 grep --color=auto -i apt` this time, not several `apt` processes. Thus, I didn't have to shut any `apt` processes down, which is probably why this why this worked. Command came from: https://askubuntu.com/questions/827212/which-is-the-process-using-apt-get-lock/827239
`sudo apt-get update` -- from Thomas's Slack message for installing python3. 
`sudo apt-get install python3 python3-pip` -- from Thomas's Slack message for installing python3. Worked fine- didn't get messages about locks existing and didn't stall at unpacking stage.
`pip3 install -e .` -- everything installed fine
`cd gfw_tile_prep` -- changed directory fine
`nano prep_tiles.py` and change the number of processes to 15 or whatever number is appropriate
`prep_tiles.py --layer gross_annual_removals_carbon` --runs fine

BTW, https://itsfoss.com/could-not-get-lock-error/ is helpful for dealing with apt locking errors, though I don't think I ever actually resolved the locking error.


Install required packages:
`pip install -e .` in the gfw_tile_prep folder

To run the tile prep script:
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


