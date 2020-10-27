from gfw_pixetl.settings import GLOBALS


class PgConn(object):
    db_host = GLOBALS.db_host
    db_port = GLOBALS.db_port
    db_name = GLOBALS.db_name
    db_user = GLOBALS.db_username
    db_password = GLOBALS.db_password

    def pg_conn(self):
        return f"PG:dbname={self.db_name} port={self.db_port} host={self.db_host} user={self.db_user} password={self.db_password}"
