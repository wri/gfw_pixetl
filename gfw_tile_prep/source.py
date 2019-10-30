from gfw_tile_prep import get_module_logger

logger = get_module_logger(__name__)


class Source(object):
    pass


class VectorSource(Source):
    class PgConn(object):
        db_host = "localhost"
        db_port = 5432
        db_name = "gadm"
        db_user = "postgres"
        db_password = "postgres"  # TODO: make a secret call
        pg_conn = "PG:dbname={} port={} host={} user={} password={}".format(
            db_name, db_port, db_host, db_user, db_password
        )

    format = "vector"
    conn = PgConn()

    def __init__(self, table_name):
        self.table_name = table_name


class RasterSource(Source):
    format = "raster"

    def __init__(self, uri, type):
        self.uri = uri
        self.type = type
