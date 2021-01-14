DIALECT_PG = 'pg'
DIALECT_MYSQL = "mysql"

import chalicelib.utils as utils
import chalicelib.parameters as params
import chalicelib.exceptions as exceptions
import pg8000
import ssl
import json
import socket
import os
import traceback
from pg8000.exceptions import ProgrammingError

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
    _logger = None
    _sql_helper = None

    def __init__(self, dialect: str):
        if dialect not in [DIALECT_MYSQL, DIALECT_PG]:
            raise exceptions.InvalidArgumentsException(f"Unknown Dialect {dialect}")
        else:
            self._dialect = dialect

        self._logger = utils.setup_logging()

        # load the sql statement helper
        with open(os.path.join(os.path.dirname(__file__), f'sql_fragments_{self._dialect}.json'),
                  'r') as f:
            self._sql_helper = json.load(f)

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

    def run_commands(self, conn, commands: list) -> list:
        '''Function to run one or more commands that will return at most one record. For statements that return
        multiple records, use underlying cursor directly.
        '''
        cursor = conn.cursor()
        counts = []
        rows = []

        def _add_output():
            try:
                rowcount = cursor.rowcount
                counts.append(rowcount)
                r = cursor.fetchall()

                if r is not None and r != ():
                    rows.extend(r)
                else:
                    rows.append(None)
            except ProgrammingError as e:
                if 'no result set' in str(e):
                    counts.append(0)
                    rows.append(None)
                else:
                    counts.append(0)
                    rows.append(e)

        for c in commands:
            if c is not None:
                try:
                    if c.count(';') > 1:
                        subcommands = c.split(';')

                        for s in subcommands:
                            if s is not None and s != '':
                                self._logger.debug(s)
                                cursor.execute(s.replace("\n", ""))
                                _add_output()
                    else:
                        cursor.execute(c)
                        _add_output()
                except pg8000.exceptions.IntegrityError as ie:
                    pass
                except Exception as e:
                    # cowardly bail on errors
                    conn.rollback()
                    print(traceback.format_exc())
                    counts.append(0)
                    rows.append(e)
            else:
                counts.append(0)
                rows.append(None)

        cursor.close()
        return counts, rows

    def verify_table(self, conn, table_ref: str, table_schema: dict, pk_name: str) -> None:
        if self._dialect == DIALECT_PG:
            try:
                cursor = conn.cursor()
                query = f"select count(9) from {table_ref}"
                self._logger.debug(query)
                cursor.execute(query)
                res = cursor.fetchone()
                self._logger.info(f"Bound to existing table {table_ref}")
            except ProgrammingError as pe:
                if "not exist" in str(pe):
                    # table doesn't exist so create it based on the current schema
                    self.create_table_from_schema(conn, table_ref, table_schema, pk_name)
                else:
                    raise exceptions.DetailedException(pe.message)
        else:
            raise exceptions.UnimplementedFeatureException()

    def create_index(self, conn, table_ref: str, column_name: str) -> None:
        index_name = f"{table_ref}_{column_name}"
        statement = f"create index {index_name} on {table_ref} ({column_name})"

        self.run_commands(conn, [statement])

        self._logger.info(f"Created new Index {index_name}")

    def verify_indexes(self, conn, table_ref: str, indexes: list) -> None:
        if indexes is not None:
            for i in indexes:
                sql = self.get_sql("VerifyIndexOnColumn") % (table_ref, i)

                counts, index_exists = self.run_commands(conn, [sql])

                if index_exists[0] == () or index_exists[0] is None:
                    self.create_index(conn, table_ref, i)

    def create_table_from_schema(self, conn, table_ref: str, table_schema: dict, pk_name: str) -> bool:
        column_spec = []
        prop = table_schema.get('properties')

        for p in prop.keys():
            column_spec.append(
                f"{p} {utils.json_to_pg(p_name=p, p_spec=prop.get(p), p_required=table_schema.get('required'), pk_name=pk_name)}")

        column_spec.extend(self.generate_who_cols())

        # synthesize the create table statement
        statement = f"create table if not exists {table_ref}({','.join(column_spec)})"

        self.run_commands(conn, [statement])

        self._logger.info(f"Created new Table {table_ref}")

        return True

    def get_sql(self, name):
        sql = self._sql_helper.get(name)
        if sql is None:
            raise exceptions.InvalidArgumentsException(f"Unable to look up SQL {name}")
        else:
            return sql
