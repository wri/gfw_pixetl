CREATE MATERIALIZED VIEW {table_name}_xy AS

SELECT  row_number() OVER() AS gid,
        a.{id_field} AS id,
        g.row,
        g.col,
        ST_Intersection(ST_SimplifyPreserveTopology(a.geom, 0.0001), g.geom) AS geom
  FROM {table_name} a, fishnet_10_10 g
    WHERE st_intersects(a.geom, g.geom)