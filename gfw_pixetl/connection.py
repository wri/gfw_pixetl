from gfw_pixetl.settings.globals import (
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_USERNAME,
)


class PgConn(object):
    db_host = DB_HOST
    db_port = DB_PORT
    db_name = DB_NAME
    db_user = DB_USERNAME
    db_password = DB_PASSWORD

    def pg_conn(self):
        return f"PG:dbname={self.db_name} port={self.db_port} host={self.db_host} user={self.db_user} password={self.db_password}"
