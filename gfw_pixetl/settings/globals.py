import os

from typing import Optional

from gfw_pixetl.utils import Secret

READER_USERNAME: Optional[str] = os.environ.get("DB_USER_RO", None)
_password: Optional[str] = os.environ.get("DB_PASSWORD_RO", None)
READER_PASSWORD: Optional[Secret] = Secret(_password) if _password else None
READER_HOST: Optional[str] = os.environ.get("DB_HOST_RO", None)
_port: Optional[str] = os.environ.get("DB_PORT_RO", None)
READER_PORT: Optional[int] = int(_port) if _port else None
READER_DBNAME: Optional[str] = os.environ.get("DATABASE_RO", None)
