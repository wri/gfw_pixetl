from gfw_pixetl.settings.globals import SETTINGS


class PgConn(object):
    db_host = SETTINGS.db_host
    db_port = SETTINGS.db_port
    db_name = SETTINGS.db_name
    db_user = SETTINGS.db_username
    db_password = SETTINGS.db_password

    def pg_conn(self):
        return f"PG:dbname={self.db_name} port={self.db_port} host={self.db_host} user={self.db_user} password={self.db_password}"
