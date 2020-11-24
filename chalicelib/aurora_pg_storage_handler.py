import chalicelib.utils as utils
import chalicelib.parameters as params
import chalicelib.exceptions as exceptions
import boto3
import os
import pg8000
from pg8000.exceptions import ProgrammingError
import ssl
import socket
import traceback


class AuroraPostgresStorageHandler:
    _region = None
    _glue_client = None
    _sts_client = None
    _control_table = None
    _resource_table_name = None
    _metadata_table_name = None
    _pk_name = None
    _delete_mode = None
    _allow_runtime_delete_mode_change = False
    _table_indexes = []
    _meta_indexes = []
    _schema_loaded = False
    _resource_schema = None
    _schema_validator = None
    _schema_validation_refresh_hitcount = None
    _schema_dependent_hit_count = 0
    _crawler_rolename = None
    _catalog_database = None
    _allow_non_itemmaster_writes = None
    _strict_occv = False
    _gremlin_address = None
    _gremlin_endpoint = None
    _deployed_account = None
    _cluster_address = None
    _cluster_port = None
    _cluster_user = None
    _cluster_db = None
    _db_conn = None
    _ssl = False

    def _run_commands(self, commands:list) -> list:
        cursor = self._db_conn.cursor()
        output = []
        for c in commands:
            if c is not None:
                try:
                    if c.count(';') > 1:
                        subcommands = c.split(';')

                        for s in subcommands:
                            if s is not None and s != '':
                                cursor.execute(s.replace("\n", ""))
                                output.append(cursor.fetchall())
                    else:
                        cursor.execute(c)
                        output.append(cursor.fetchall())
                except Exception as e:
                    # cowardly bail on errors
                    self._db_conn.rollback()
                    print(traceback.format_exc())
                    output.append(e)
            else:
                output.append(None)

        return output

    def _get_pg_conn(self, pwd: str):
        pid = str(os.getpid())
        conn = None

        # connect to the database
        conn = pg8000.connect(user=self._cluster_user, host=self._cluster_address, port=self._cluster_port,
                              database=self._cluster_db,
                              password=pwd,
                              ssl_context=ssl.create_default_context() if self._ssl is True else None,
                              timeout=None, tcp_keepalive=True, application_name=params.AWS_DATA_API_NAME)
        self._logger.debug('Connect [%s] %s:%s:%s:%s' % (
            pid, self._cluster_address, self._cluster_port, self._cluster_db, self._cluster_user))

        # Enable keepalives manually until pg8000 supports it
        # For future reference: https://github.com/mfenniak/pg8000/issues/149
        # TCP keepalives still need to be configured appropriately on OS level as well
        conn._usock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        conn.autocommit = True

        return conn

    def _extract_type_spec(self, p_name, p_spec):
        '''Convert a JSON type to a Postgres type with nullability spec
        '''
        base = None
        if p_spec is not None:
            p_type = p_spec.get("type")

            if p_type.lower() == 'string':
                base = 'varchar'
            elif p_type.lower() == 'number':
                base = 'double precision'
            elif p_type.lower() == 'integer':
                base = 'integer'
            elif p_type.lower() == 'boolean':
                base = 'char(1) NOT NULL DEFAULT 0'
                return base
            else:
                raise exceptions.UnimplementedFeatureException(f"Type {p_type} not translatable to RDBMS types")

            # process null/not null
            req = self._resource_schema.get('required')

            if p_name.lower() == self._pk_name:
                base += ' NOT NULL PRIMARY KEY'
            elif p_name in req:
                base += ' NOT NULL'
            else:
                base += ' NULL'

            return base
        else:
            raise exceptions.DetailedException("Unable to render None Type Spec")

    def _create_table_from_schema(self, table_ref: str, table_schema: dict):
        column_spec = []
        prop = table_schema.get('properties')

        for p in prop.keys():
            column_spec.append(f"{p} {self._extract_type_spec(p, prop.get(p))}")

        # synthesize the create table statement
        statement = f"create table if not exists {table_ref}({','.join(column_spec)})"

        self._logger.debug(statement)

        self._run_commands([statement])

        return True

    def _verify_table(self, table_ref: str, table_schema: dict):
        try:
            cursor = self._db_conn.cursor()
            cursor.execute(f"select count(9) from {table_ref}")
            res = cursor.fetchone()
        except ProgrammingError as e:
            if "not exist" in str(e):
                # table doesn't exist so create it based on the current schema
                ok = self._create_table_from_schema(table_ref, table_schema)
            else:
                raise exceptions.DetailedException(e.message)

    def __init__(self, table_name, primary_key_attribute, region, delete_mode, allow_runtime_delete_mode_change,
                 table_indexes, metadata_indexes, crawler_rolename,
                 catalog_database, allow_non_itemmaster_writes, strict_occv, gremlin_address, deployed_account,
                 pitr_enabled=None, kms_key_arn=None, schema_validation_refresh_hitcount=None, **kwargs):
        # setup class logger
        self._logger = utils.setup_logging()

        global log
        log = self._logger

        # setup foundation properties
        self._region = region
        self._resource_table_name = table_name
        self._metadata_table_name = f"{table_name}_{params.METADATA}"
        self._pk_name = primary_key_attribute
        self._deployed_account = deployed_account

        # resolve connection details
        self._cluster_address = kwargs.get(params.CLUSTER_ADDRESS)
        self._cluster_port = kwargs.get(params.CLUSTER_PORT)
        self._cluster_user = kwargs.get(params.DB_USERNAME)
        self._cluster_db = kwargs.get(params.DB_NAME)
        self._cluster_pstore = kwargs.get(params.DB_USERNAME_PSTORE_ARN)
        self._ssl = kwargs.get(params.DB_USE_SSL)

        # pick up schemas to push table structure
        self._resource_schema = kwargs.get(params.CONTROL_TYPE_RESOURCE_SCHEMA)
        self._metadata_schema = kwargs.get(params.CONTROL_TYPE_METADATA_SCHEMA)

        if self._resource_schema is None:
            raise exceptions.InvalidArgumentsException(
                "Relational Storage Handler requires a JSON Schema to initialise")

        if self._cluster_pstore is None:
            raise exceptions.InvalidArgumentsException(
                "Unable to connect to Target Cluster Database without SSM Parameter Store Password ARN")

        # extract the password from ssm
        _pstore_client = boto3.client('ssm', region_name=self._region)
        _password_response = _pstore_client.get_parameter(Name=self._cluster_pstore)
        if _password_response is None:
            raise exceptions.DetailedException(
                "Unable to connect to SSM Parameter Store")
        else:
            _pwd = _password_response.get('Parameter').get('Value')

            # connect to the database
            self._db_conn = self._get_pg_conn(pwd=_pwd)

            # verify the table exists
            self._verify_table(self._resource_table_name, self._resource_schema)
            self._verify_table(self._metadata_table_name, self._metadata_schema)

    def check(self, id: str):
        pass

    def list_items(self, **kwargs):
        pass

    def get_usage(self, table_name: str):
        pass

    def get(self, id: str):
        pass

    def get_metadata(self, id: str):
        pass

    def restore(self, id: str, caller_identity: str):
        pass

    def delete(id: str, caller_identity: str, **kwargs):
        pass

    def update_item(self, id: str, caller_identity: str, **kwargs):
        pass

    def drop_table(self, table_name: str, do_export: bool):
        pass

    def find(self, **kwargs):
        pass

    def get_streams(self):
        raise exceptions.UnimplementedFeatureException()

    def item_master_update(self, caller_identity: str, **kwargs):
        pass

    def remove_resource_attributes(self, id: str,
                                   resource_attributes: list,
                                   caller_identity: str):
        pass

    def disconnect(self):
        self._db_conn.close()
