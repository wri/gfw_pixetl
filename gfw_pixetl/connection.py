class PgConn(object):
    db_host = "localhost"
    db_port = 5432
    db_name = "gadm"
    db_user = "postgres"
    db_password = "postgres"  # pragma: allowlist secret

    def pg_conn(self):
        return f"PG:dbname={self.db_name} port={self.db_port} host={self.db_host} user={self.db_user} password={self.db_password}"
