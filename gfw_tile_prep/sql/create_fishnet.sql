CREATE OR REPLACE VIEW fishnet_10_10 AS
  SELECT row_number() OVER() as gid, row, col, ST_SetSRID(geom, 4326) as geom
  FROM ST_CreateFishnet(18, 36, 10, 10, -180, -90) AS cells;