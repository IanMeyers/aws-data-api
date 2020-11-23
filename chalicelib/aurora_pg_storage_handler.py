import chalicelib.utils as utils
import chalicelib.parameters as params
import chalicelib.exceptions as exceptions
import boto3
import os
import pg8000
import ssl
import socket
import traceback


class AuroraPostgresStorageHandler:
    _region = None
    _glue_client = None
    _sts_client = None
    _resource_table = None
    _metadata_table = None
    _control_table = None
    _table_name = None
    _pk_name = None
    _delete_mode = None
    _allow_runtime_delete_mode_change = False
    _table_indexes = []
    _meta_indexes = []
    _schema_loaded = False
    _schema = None
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

    def _run_commands(self, commands):
        cursor = self._db_conn.cursor()

        for c in commands:
            if c is not None:
                try:
                    if c.count(';') > 1:
                        subcommands = c.split(';')

                        for s in subcommands:
                            if s is not None and s != '':
                                cursor.execute(s.replace("\n", ""))
                    else:
                        cursor.execute(c)
                except Exception as e:
                    # cowardly bail on errors
                    self._db_conn.rollback()
                    print(traceback.format_exc())
                    return False

        return True

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
        self._table_name = table_name
        self._pk_name = primary_key_attribute
        self._deployed_account = deployed_account

        # resolve connection details
        self._cluster_address = kwargs.get(params.CLUSTER_ADDRESS)
        self._cluster_port = kwargs.get(params.CLUSTER_PORT)
        self._cluster_user = kwargs.get(params.DB_USERNAME)
        self._cluster_db = kwargs.get(params.DB_NAME)
        self._cluster_pstore = kwargs.get(params.DB_USERNAME_PSTORE_ARN)
        self._ssl = kwargs.get(params.DB_USE_SSL)

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