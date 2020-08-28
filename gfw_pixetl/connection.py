from gfw_pixetl.settings.globals import (
    READER_DBNAME,
    READER_HOST,
    READER_PASSWORD,
    READER_PORT,
    READER_USERNAME,
)


class PgConn(object):
    db_host = READER_HOST
    db_port = READER_PORT
    db_name = READER_DBNAME
    db_user = READER_USERNAME
    db_password = READER_PASSWORD

    def pg_conn(self):
        return f"PG:dbname={self.db_name} port={self.db_port} host={self.db_host} user={self.db_user} password={self.db_password}"
