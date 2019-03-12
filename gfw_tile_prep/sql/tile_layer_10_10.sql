DROP MATERIALIZED VIEW IF EXISTS {layer}_10_10;

CREATE MATERIALIZED VIEW {layer}_10_10 AS
  SELECT  row_number() OVER() AS gid,
          {oid} AS oid,
          g.row,
          g.col,
          ST_Intersection(ST_SimplifyPreserveTopology(a.geom, 0.0001), g.geom) AS geom
    FROM {layer} a, fishnet_10_10 g
      WHERE ST_Intersects(a.geom, g.geom)