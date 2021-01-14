DIALECT_PG = 'pg'
DIALECT_MYSQL = "mysql"

import chalicelib.parameters as params
import chalicelib.exceptions as exceptions
import pg8000
import ssl
import socket
import os

_who_type_map = {
    DIALECT_PG: {
        params.ITEM_VERSION: 'integer',
        params.LAST_UPDATE_ACTION: 'string',
        params.LAST_UPDATE_DATE: 'datetime',
        params.LAST_UPDATED_BY: 'string',
        params.DELETED: 'boolean',
        params.ITEM_MASTER_ID: 'string'
    }
}

_who_col_map = {
    DIALECT_PG: {
        params.ITEM_VERSION: "item_version",
        params.LAST_UPDATE_ACTION: "last_update_action",
        params.LAST_UPDATE_DATE: "last_update_date",
        params.LAST_UPDATED_BY: "last_updated_by",
        params.DELETED: "deleted",
        params.ITEM_MASTER_ID: "item_master_id"
    }
}

_who_invert = {}
_who_invert[DIALECT_PG] = {v: k for k, v in _who_col_map.get(DIALECT_PG).items()}


class RdbmsEngineType:
    _dialect = None

    def __init__(self, dialect: str):
        if dialect not in [DIALECT_MYSQL, DIALECT_PG]:
            raise exceptions.InvalidArgumentsException(f"Unknown Dialect {dialect}")
        else:
            self._dialect = dialect

    def get_connection(self, cluster_user: str, cluster_address: str, cluster_port: int, database: str, pwd: str,
                       ssl: bool):
        if self._dialect == DIALECT_PG:
            pid = str(os.getpid())
            conn = None

            # connect to the database
            conn = pg8000.connect(user=cluster_user, host=cluster_address, port=cluster_port,
                                  database=database,
                                  password=pwd,
                                  ssl_context=ssl.create_default_context() if ssl is True else None,
                                  timeout=None, tcp_keepalive=True, application_name=params.AWS_DATA_API_NAME)

            # Enable keepalives manually until pg8000 supports it
            # For future reference: https://github.com/mfenniak/pg8000/issues/149
            # TCP keepalives still need to be configured appropriately on OS level as well
            conn._usock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            conn.autocommit = True

            return conn
        else:
            raise exceptions.UnimplementedFeatureException()

    def get_who_col_map(self) -> dict:
        return _who_col_map.get(self._dialect)

    def get_who_type_map(self) -> dict:
        return _who_type_map.get(self._dialect)

    def get_who_column_keys(self) -> dict:
        return self.get_who_type_map().keys()

    def generate_who_cols(self) -> list:
        if self._dialect == DIALECT_PG:
            return [
                f"{self.get_who_col_map().get(params.ITEM_VERSION)} int not null default 1",
                f"{self.get_who_col_map().get(params.LAST_UPDATE_ACTION)} varchar(15) null",
                f"{self.get_who_col_map().get(params.LAST_UPDATE_DATE)} timestamp with time zone not null",
                f"{self.get_who_col_map().get(params.LAST_UPDATED_BY)} varchar(60) not null",
                f"{self.get_who_col_map().get(params.DELETED)} boolean not null default FALSE",
                f"{self.get_who_col_map().get(params.ITEM_MASTER_ID)} varchar null"
            ]

    def get_who(self, value) -> str:
        map = self.get_who_col_map()
        if value in map:
            return map.get(value)
        else:
            return value

    def who_column_update(self, caller_identity: str, version_increment: bool = True):
        map = self.get_who_col_map()
        clauses = [
            f"{map.get(params.LAST_UPDATE_ACTION)} = '{params.ACTION_UPDATE}'",
            f"{map.get(params.LAST_UPDATE_DATE)} = CURRENT_TIMESTAMP",
            f"{map.get(params.LAST_UPDATED_BY)} = '{caller_identity}'"
        ]

        if version_increment is True:
            clauses.append(
                f"{map.get(params.ITEM_VERSION)} = {map.get(params.ITEM_VERSION)}+1")

        return clauses

    def who_column_list(self):
        map = self.get_who_col_map()
        return [
            map.get(params.ITEM_VERSION),
            map.get(params.LAST_UPDATE_ACTION),
            map.get(params.LAST_UPDATE_DATE),
            map.get(params.LAST_UPDATED_BY)
        ]

    def who_column_insert(self, caller_identity: str):
        if self._dialect == DIALECT_PG:
            return [
                f"0",
                f"'{params.ACTION_CREATE}'",
                f"CURRENT_TIMESTAMP",
                f"'{caller_identity}'"
            ]
        else:
            raise exceptions.UnimplementedFeatureException()
