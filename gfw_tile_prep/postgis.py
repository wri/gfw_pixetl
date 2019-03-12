import subprocess as sp
import psycopg2 as pg
import os


def import_vector(layer, **kwargs):
    src = kwargs["src"]
    pg_conn = kwargs["pg_conn"]

    cmd = [
        "ogr2ogr",
        "-overwrite",
        "-t_srs",
        "EPSG:4326",
        "-f",
        "PostgreSQL",
        pg_conn,
        src,
        "-nln",
        layer,
    ]
    sp.check_call(cmd)


def prep_layers(layer, **kwargs):
    host = kwargs["host"]
    dbname = kwargs["dbname"]
    dbuser = kwargs["dbuser"]
    password = kwargs["password"]
    if not kwargs["oid"]:
        oid = "1"
    else:
        oid = "a." + kwargs["oid"]

    dir = os.path.dirname(__file__)

    conn = pg.connect(
        "dbname='{}' "
        "user='{}' "
        "host='{}' "
        "password='{}'".format(dbname, dbuser, host, password)
    )
    cur = conn.cursor()

    with open(os.path.join(dir, "sql/fishnet_function.sql", "r")) as f:
        sql = f.read()

    cur.execute(sql)

    with open(os.path.join(dir, "sql/create_fishnet.sql", "r")) as f:
        sql = f.read()

    cur.execute(sql)

    with open(os.path.join(dir, "sql/tile_layer_10_10.sql", "r")) as f:
        sql = f.read()

    cur.execute(sql.format(layer=layer, oid=oid))

    conn.commit()
    conn.close()
