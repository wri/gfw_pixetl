from gfw_pixetl.layers import VectorSrcLayer
from gfw_pixetl.tiles import VectorSrcTile


def test_vector_tile_sql(VECTOR_LAYER, GEOSTORE_TABLE):
    assert isinstance(VECTOR_LAYER, VectorSrcLayer)

    tile = VectorSrcTile("10N_010E", VECTOR_LAYER.grid, VECTOR_LAYER)
    sql = tile.compose_query()

    assert "".join(str(sql).split()) == "".join(
        """

    SELECT
        biomass + 1 AS "Mg_ha-1",
        CASE
            WHEN st_geometrytype(
                    st_intersection(
                        geom,
                        ST_MakeEnvelope(
                            10.0,
                            0.0,
                            20.0,
                            10.0,
                            4326)
                    )) = \'ST_GeometryCollection\'::text
            THEN st_collectionextract(
                    st_intersection(
                        geom,
                        ST_MakeEnvelope(
                            10.0,
                            0.0,
                            20.0,
                            10.0,
                            4326)
                    ), 3)
            ELSE st_intersection(geom,
                    st_intersection(
                        geom,
                        ST_MakeEnvelope(
                            10.0,
                            0.0,
                            20.0,
                            10.0,
                            4326)
                    ))
        END AS geom
        FROM whrc_aboveground_biomass_stock_2000.v4
        WHERE ST_Intersects(
                geom,
                ST_MakeEnvelope(
                    10.0,
                    0.0,
                    20.0,
                    10.0,
                    4326)
                    )
            AND 1=1
        GROUP BY carbon
        ORDER BY biomass + 1 DESC
        """.split()
    )
