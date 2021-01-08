import chalicelib.utils as utils
import chalicelib.parameters as params
import chalicelib.exceptions as exceptions
import os
import pg8000
from pg8000.exceptions import ProgrammingError
import ssl
import socket
import traceback
import json
import fastjsonschema

_who_col_map = {
    params.ITEM_VERSION: "item_version",
    params.LAST_UPDATE_ACTION: "last_update_action",
    params.LAST_UPDATE_DATE: "last_update_date",
    params.LAST_UPDATED_BY: "last_updated_by",
    params.DELETED: "deleted",
    params.ITEM_MASTER_ID: "item_master_id"
}

_who_type_map = {
    params.ITEM_VERSION: 'integer',
    params.LAST_UPDATE_ACTION: 'string',
    params.LAST_UPDATE_DATE: 'datetime',
    params.LAST_UPDATED_BY: 'string',
    params.DELETED: 'boolean',
    params.ITEM_MASTER_ID: 'string'
}

_who_invert = {v: k for k, v in _who_col_map.items()}


class DataAPIStorageHandler:
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
    _sql_helper = None
    _extended_config = None

    def _get_sql(self, name):
        sql = self._sql_helper.get(name)
        if sql is None:
            raise exceptions.InvalidArgumentsException(f"Unable to look up SQL {name}")
        else:
            return sql

    def _run_commands(self, commands: list) -> list:
        '''Function to run one or more commands that will return at most one record. For statements that return
        multiple records, use underlying cursor directly.
        '''
        cursor = self._db_conn.cursor()
        counts = []
        rows = []

        def _add_output():
            try:
                counts.append(cursor.rowcount)
                r = cursor.fetchall()

                if r is not None and r != ():
                    rows.append(r[0])
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
                                cursor.execute(s.replace("\n", ""))
                                _add_output()
                    else:
                        cursor.execute(c)
                        _add_output()
                except pg8000.exceptions.IntegrityError as ie:
                    pass
                except Exception as e:
                    # cowardly bail on errors
                    self._db_conn.rollback()
                    print(traceback.format_exc())
                    counts.append(0)
                    rows.append(e)
            else:
                counts.append(0)
                rows.append(None)

        cursor.close()
        return counts, rows

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

    def _generate_who_cols(self):
        return [
            f"{_who_col_map.get(params.ITEM_VERSION)} int not null default 1",
            f"{_who_col_map.get(params.LAST_UPDATE_ACTION)} varchar(15) null",
            f"{_who_col_map.get(params.LAST_UPDATE_DATE)} timestamp with time zone not null",
            f"{_who_col_map.get(params.LAST_UPDATED_BY)} varchar(60) not null",
            f"{_who_col_map.get(params.DELETED)} boolean not null default FALSE",
            f"{_who_col_map.get(params.ITEM_MASTER_ID)} varchar null"
        ]

    def _create_table_from_schema(self, table_ref: str, table_schema: dict) -> bool:
        column_spec = []
        prop = table_schema.get('properties')

        for p in prop.keys():
            column_spec.append(
                f"{p} {utils.json_to_pg(p_name=p, p_spec=prop.get(p), p_required=self._resource_schema.get('required'), pk_name=self._pk_name)}")

        column_spec.extend(self._generate_who_cols())

        # synthesize the create table statement
        statement = f"create table if not exists {table_ref}({','.join(column_spec)})"

        self._logger.debug(statement)

        self._run_commands([statement])

        self._logger.info(f"Created new Table {table_ref}")

        return True

    def _verify_catalog(self, table_ref: str) -> None:
        # setup a glue connection and crawler for this database and table
        args = {
            params.EXTENDED_CONFIG: self._extended_config,
            params.CLUSTER_ADDRESS: self._cluster_address,
            params.CLUSTER_PORT: self._cluster_port,
            params.DB_NAME: self._cluster_db,
            params.DB_USERNAME: self._cluster_user,
            params.DB_USERNAME_PSTORE_ARN: self._cluster_pstore
        }
        # interface uses kwargs as this method supports both dynamo and rds based crawlers
        utils.verify_crawler(table_name=table_ref, crawler_rolename=self._crawler_rolename,
                             catalog_db=self._catalog_database,
                             datasource_type=params.RDS_PG_STORAGE_HANDLER,
                             deployed_account=self._deployed_account,
                             region=self._region,
                             logger=self._logger,
                             crawler_prefix=f'PG-{self._cluster_address.split(".")[0]}',
                             **args)

    def _verify_table(self, table_ref: str, table_schema: dict) -> None:
        try:
            cursor = self._db_conn.cursor()
            cursor.execute(f"select count(9) from {table_ref}")
            res = cursor.fetchone()
        except ProgrammingError as pe:
            if "not exist" in str(pe):
                # table doesn't exist so create it based on the current schema
                self._create_table_from_schema(table_ref, table_schema)
            else:
                raise exceptions.DetailedException(pe.message)
        except Exception as e:
            self._logger.error(e)

    def _create_index(self, table_ref: str, column_name: str) -> None:
        index_name = f"{table_ref}_{column_name}"
        statement = f"create index {index_name} on {table_ref} ({column_name})"

        self._run_commands([statement])

        self._logger.info(f"Created new Index {index_name}")

    def _verify_indexes(self, table_ref: str, indexes: list) -> None:
        if indexes is not None:
            for i in indexes:
                sql = self._get_sql("VerifyIndexOnColumn") % (table_ref, i)

                counts, index_exists = self._run_commands([sql])

                if index_exists[0] == () or index_exists[0] is None:
                    self._create_index(table_ref, i)

    def _extract_type(self, input):
        if type(input) == str:
            set_val = f"'{input}'"
        elif type(input) == bool:
            if input is True:
                set_val = 1
            else:
                set_val = 0
        else:
            set_val = input

        return set_val

    def _who_column_update(self, caller_identity: str, version_increment: bool = True):
        clauses = [
            f"{_who_col_map.get(params.LAST_UPDATE_ACTION)} = '{params.ACTION_UPDATE}'",
            f"{_who_col_map.get(params.LAST_UPDATE_DATE)} = CURRENT_TIMESTAMP",
            f"{_who_col_map.get(params.LAST_UPDATED_BY)} = '{caller_identity}'"
        ]

        if version_increment is True:
            clauses.append(f"{_who_col_map.get(params.ITEM_VERSION)} = {_who_col_map.get(params.ITEM_VERSION)}+1")

        return clauses

    def _who_column_list(self):
        return [
            _who_col_map.get(params.ITEM_VERSION),
            _who_col_map.get(params.LAST_UPDATE_ACTION),
            _who_col_map.get(params.LAST_UPDATE_DATE),
            _who_col_map.get(params.LAST_UPDATED_BY)
        ]

    def _who_column_insert(self, caller_identity: str):
        return [
            f"0",
            f"'{params.ACTION_CREATE}'",
            f"CURRENT_TIMESTAMP",
            f"'{caller_identity}'"
        ]

    def _synthesize_update(self, input: dict, caller_identity: str, version_increment: bool = True) -> list:
        ''' Generate a valid list of update clauses from an input dict. For example:

        {"a":1, "b":2} becomes ["a = 1", "b=2"]

        :param input:
        :return:
        '''
        output = []
        for k in input.keys():
            set_val = self._extract_type(input.get(k))

            output.append(f"{k} = {set_val}")

        # now add the 'who' column and item version updates
        output.extend(self._who_column_update(caller_identity, version_increment))

        return output

    def _create_update_statement(self, table_ref: str, pk_name: str, input: dict, item_id: str,
                                 caller_identity: str, version_increment: bool = True,
                                 check_delete: bool = True) -> str:
        updates = ",".join(self._synthesize_update(input, caller_identity, version_increment))
        statement = f"update {table_ref} set {updates} where {pk_name} = '{item_id}'"

        if check_delete is True:
            statement = statement + f" and {_who_col_map.get(params.DELETED)} = FALSE"

        return statement

    def _synthesize_insert(self, pk_name: str, pk_value: str, input: dict, caller_identity) -> tuple:
        '''Generate a valid list of insert clauses from an input dict. For example:

        {"a":1, "b":"blah"} becomes [1, "blah"]

        :param input:
        :return:
        '''
        columns = [pk_name]
        columns.extend(list(input.keys()))

        values = [self._extract_type(pk_value)]
        for k in input.keys():
            values.append(self._extract_type(input.get(k)))

        columns.extend(self._who_column_list())
        values.extend(self._who_column_insert(caller_identity=caller_identity))

        return columns, values

    def _create_insert_statement(self, table_ref: str, pk_name: str, pk_value: str, input: dict,
                                 caller_identity: str) -> str:
        insert = self._synthesize_insert(pk_name=pk_name, pk_value=pk_value, input=input,
                                         caller_identity=caller_identity)
        columns = ",".join(insert[0])

        # generate a string list as the values statement may be multi-type
        values = [str(x) for x in insert[1]]

        return f'insert into {table_ref} ({columns}) values ({",".join(values)})'

    def __init__(self, table_name, primary_key_attribute, region, delete_mode, allow_runtime_delete_mode_change,
                 table_indexes, metadata_indexes, schema_validation_refresh_hitcount, crawler_rolename,
                 catalog_database, allow_non_itemmaster_writes, strict_occv, deployed_account,
                 pitr_enabled=None, kms_key_arn=None, logger=None, extended_config=None,
                 **kwargs):

        # setup class logger
        if logger == None:
            self._logger = utils.setup_logging()
        else:
            self._logger = logger

        global log
        log = self._logger

        # load the sql statement helper
        with open(os.path.join(os.path.dirname(__file__), 'sql_fragments_pg.json'), 'r') as f:
            self._sql_helper = json.load(f)

        # setup foundation properties
        self._region = region
        self._resource_table_name = table_name.lower()

        # allow override of the metadata table name
        if params.OVERRIDE_METADATA_TABLENAME in kwargs:
            self._metadata_table_name = kwargs.get(params.OVERRIDE_METADATA_TABLENAME)
        else:
            self._metadata_table_name = f"{self._resource_table_name}_{params.METADATA}".lower()

        self._pk_name = primary_key_attribute
        self._deployed_account = deployed_account
        self._extended_config = extended_config
        self._crawler_rolename = crawler_rolename
        self._catalog_database = catalog_database
        self._delete_mode = delete_mode

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

        # create schema validators
        if self._resource_schema is not None:
            self._resource_validator = fastjsonschema.compile(self._resource_schema)
        if self._metadata_schema is not None:
            self._metadata_validator = fastjsonschema.compile(self._metadata_schema)

        if self._resource_schema is None:
            raise exceptions.InvalidArgumentsException(
                "Relational Storage Handler requires a JSON Schema to initialise")

        if self._cluster_pstore is None:
            raise exceptions.InvalidArgumentsException(
                "Unable to connect to Target Cluster Database without SSM Parameter Store Password ARN")
        else:
            # extract the password from ssm
            _pwd = utils.get_encrypted_parameter(parameter_name=self._cluster_pstore,
                                                 region=self._region)

            # connect to the database
            self._db_conn = self._get_pg_conn(pwd=_pwd)
            self._logger.info(f"Connected to {self._cluster_address}:{self._cluster_port} as {self._cluster_user}")

            # verify the resource table, indexes, and catalog registry exists
            self._verify_table(self._resource_table_name, self._resource_schema)
            self._verify_indexes(self._resource_table_name, table_indexes)
            self._verify_catalog(self._resource_table_name)

            # verify the metadata table, indexes, and catalog registry exists
            self._verify_table(self._metadata_table_name, self._metadata_schema)
            self._verify_indexes(self._metadata_table_name, metadata_indexes)

    def check(self, id: str) -> bool:
        statement = f"select count(9) from {self._resource_table_name} where {self._pk_name} = '{id}' and {_who_col_map.get(params.DELETED)} = FALSE"

        counts, rows = self._run_commands([statement])

        record = rows[0]
        if record is not None and record != () and record[0] != 0:
            return True
        else:
            raise exceptions.ResourceNotFoundException()

    def list_items(self, **kwargs):
        limit = kwargs.get(params.QUERY_PARAM_LIMIT)

        pass

    def get_usage(self, table_name: str):
        pass

    def get_resource(self, id: str, only_attributes: list = None, not_attributes: list = None):
        schema = self._resource_schema.get("properties")

        # only add the attributes needed, or the schema keys
        if only_attributes is None:
            columns = list(schema.keys())
        else:
            columns = only_attributes

        # remove the not attributes
        if not_attributes is not None:
            for n in not_attributes:
                del columns[n]

        statement = f"select {','.join(columns)} from {self._resource_table_name} where {self._pk_name} = '{id}' and {_who_col_map.get(params.DELETED)} = FALSE"
        counts, records = self._run_commands([statement])

        if records is not None and len(records) == 1:
            return utils.pivot_resultset_into_json(records, columns, schema)
        elif records is not None and len(records) > 1:
            raise exceptions.DetailedException("O(1) lookup of Resource returned multiple rows")
        elif records is None:
            raise exceptions.ResourceNotFoundException()

    def _generate_column_list(self, base_columns: list, prefix: str = None) -> dict:
        '''
        for a given list of base columns, return these columns plus the who columns. data is returned as a dict where
        keys is the requested columns and values are the prefixed sql columns (which may be the same)
        '''
        columns = base_columns

        # add the named who columns to be returned
        columns.extend(list(_who_col_map.keys()))

        out = {}

        def _resolve_who(value):
            if value in _who_col_map:
                return _who_col_map.get(value)
            else:
                return value

        # add the prefix to sql columns
        for c in columns:
            if prefix is not None:
                out[c] = "{prefix}.{alias} as {alias}".format(prefix=prefix, alias=_resolve_who(c))
            else:
                out[c] = _resolve_who(c)

        return out

    def get(self, id: str, suppress_meta_fetch: bool = False, only_attributes: list = None,
            not_attributes: list = None):
        output = {params.RESOURCE: self.get_resource(id, only_attributes, not_attributes)}

        if suppress_meta_fetch is False:
            try:
                meta = self.get_metadata(id=id)

                if meta is not None:
                    output[params.METADATA] = meta

            except exceptions.ResourceNotFoundException:
                pass

        return output

    def get_metadata(self, id: str):
        # validate that the object isn't deleted
        self.check(id)

        schema = self._metadata_schema.get("properties")

        # create the type map for what will be returned
        type_map = _who_type_map
        for k, v in schema.items():
            type_map[k] = v.get("type")

        cols = self._generate_column_list(base_columns=list(schema.keys()), prefix='a')

        # implement delete check by joining to the resource table on the primary key
        statement = f"select {','.join(cols.values())} from {self._metadata_table_name} a, {self._resource_table_name} b where a.{self._pk_name} = '{id}' and a.{self._pk_name} = b.{self._pk_name} and b.{_who_col_map.get(params.DELETED)} = FALSE"
        counts, records = self._run_commands([statement])

        if records is not None and len(records) == 1 and records[0] is not None:
            return utils.pivot_resultset_into_json(records, list(cols.keys()), type_map)
        elif records is not None and len(records) > 1:
            raise exceptions.DetailedException("O(1) lookup of Metadata returned multiple rows")
        elif records is None or records[0] is None:
            raise exceptions.ResourceNotFoundException()

    def _create_restore_statement(self, id: str, caller_identity: str):
        return self._create_update_statement(table_ref=self._resource_table_name, pk_name=self._pk_name,
                                             input={_who_col_map.get(params.DELETED): False},
                                             item_id=id, caller_identity=caller_identity, version_increment=False,
                                             check_delete=False)

    def restore(self, id: str, caller_identity: str):
        restore = self._create_restore_statement(id, caller_identity)
        counts, rows = self._run_commands([restore])

        return True if counts is not None and counts[0] > 0 else False

    def _delete_record(self, table_name: str, item_id: str):
        delete_stmt = f"delete from {table_name} where {self._pk_name} = '{item_id}'"

        counts, records = self._run_commands([delete_stmt])

        return True if counts is not None and counts[0] > 0 else False

    def _delete_metadata(self, id: str):
        return self._delete_record(table_name=self._metadata_table_name, item_id=id)

    def delete(self, id: str, caller_identity: str, **kwargs):
        response = {}

        if params.METADATA in kwargs:
            if len(kwargs.get(params.METADATA)) == 0:
                # hard delete the metadata record - there is no soft delete
                response[params.METADATA] = {
                    params.DATA_MODIFIED: self._delete_metadata(id)
                }
            else:
                # just delete the specified attributes
                response[params.METADATA] = {
                    params.DATA_MODIFIED: self.remove_metadata_attributes(id=id,
                                                                          metadata_attributes=kwargs.get(
                                                                              params.METADATA),
                                                                          caller_identity=caller_identity)
                }

        if kwargs is None or kwargs == {} or params.RESOURCE in kwargs:
            if params.RESOURCE not in kwargs or len(kwargs.get(params.RESOURCE)) == 0:
                if self._delete_mode == params.DELETE_MODE_SOFT:
                    # perform a soft delete and reflect that only the resource will have been deleted in the response§
                    update = self._create_update_statement(table_ref=self._resource_table_name, pk_name=self._pk_name,
                                                           input={_who_col_map.get(params.DELETED): True},
                                                           item_id=id, caller_identity=caller_identity)
                    counts, records = self._run_commands([update])

                    if counts is not None and counts[0] > 0:
                        response[params.RESOURCE] = {
                            params.DATA_MODIFIED: True
                        }
                    else:
                        response[params.RESOURCE] = {
                            params.DATA_MODIFIED: False
                        }
                elif self._delete_mode == params.DELETE_MODE_HARD:
                    # remove the metadata
                    response[params.METADATA] = {
                        params.DATA_MODIFIED: self._delete_metadata(id)
                    }

                    # delete the database record
                    response[params.RESOURCE] = {
                        params.DATA_MODIFIED: self._delete_record(table_name=self._resource_table_name, item_id=id)
                    }
                else:
                    # tombstone deletions not supported in rdbms due to nullability constraints
                    raise exceptions.UnimplementedFeatureException("Cannot Tombstone Delete in RDBMS")
            else:
                # remove resource attributes only
                response[params.RESOURCE] = {
                    params.DATA_MODIFIED: self.remove_resource_attributes(id=id, resource_attributes=kwargs.get(
                        params.RESOURCE), caller_identity=caller_identity)
                }

        return response

    def drop_table(self, table_name, do_export=True):
        pass

    def _execute_merge(self, table_ref: str, pk_name: str, id: str, caller_identity, **kwargs):
        update = self._create_update_statement(table_ref=table_ref, pk_name=pk_name,
                                               input=kwargs,
                                               item_id=id, caller_identity=caller_identity)
        counts, records = self._run_commands([update])

        if counts[0] == 0:
            # update statement didn't work, so insert the value
            insert = self._create_insert_statement(table_ref=table_ref, pk_name=pk_name,
                                                   pk_value=id, input=kwargs, caller_identity=caller_identity)

            counts, records = self._run_commands([insert])

            if counts[0] == 1:
                return {
                    params.DATA_MODIFIED: True
                }
            else:
                raise exceptions.DetailedException("Unable to insert or update Resource")
        else:
            return {
                params.DATA_MODIFIED: True
            }

    def update_item(self, id: str, caller_identity: str, **kwargs) -> bool:
        ''' Method ot merge an item into the table. We will first attempt to update an existing item, and when that
        fails we will insert a new item

        :param id:
        :param caller_identity:
        :param kwargs:
        :return:
        '''
        response = {}

        if params.METADATA in kwargs:
            metadata = kwargs.get(params.METADATA)

            if self._metadata_validator is not None:
                self._metadata_validator(metadata)

            response[params.METADATA] = self._execute_merge(table_ref=self._metadata_table_name, id=id,
                                                            pk_name=self._pk_name,
                                                            caller_identity=caller_identity, **metadata)

        if params.RESOURCE in kwargs:
            resource = kwargs.get(params.RESOURCE)
            if self._resource_validator is not None:
                self._resource_validator(resource)

            response[params.RESOURCE] = self._execute_merge(table_ref=self._resource_table_name, id=id,
                                                            pk_name=self._pk_name,
                                                            caller_identity=caller_identity, **resource)

        return response

    def find(self, **kwargs):
        pass

    def get_streams(self):
        raise exceptions.UnimplementedFeatureException()

    def item_master_update(self, caller_identity: str, **kwargs):
        pass

    def _remove_attributes_from_table(self, item_id: str, attribute_list: list, table_name: str, caller_identity: str):
        # generate the update statement setting each attribute to NULL
        update_attribute_clauses = []
        for r in attribute_list:
            update_attribute_clauses.append(f"{r} = null")

        # add who column update statements
        update_attribute_clauses.extend(
            self._who_column_update(caller_identity=caller_identity, version_increment=True))

        # create the update statement
        update_statement = f"update {table_name} set {','.join(update_attribute_clauses)} where {self._pk_name} = '{item_id}'"

        counts, rows = self._run_commands(commands=[update_statement])

        return True if counts is not None and counts[0] > 0 else False

    def remove_metadata_attributes(self, id: str, metadata_attributes: list, caller_identity: str):
        return self._remove_attributes_from_table(item_id=id, attribute_list=metadata_attributes,
                                                  table_name=self._metadata_table_name, caller_identity=caller_identity)

    def remove_resource_attributes(self, id: str,
                                   resource_attributes: list,
                                   caller_identity: str):
        return self._remove_attributes_from_table(item_id=id, attribute_list=resource_attributes,
                                                  table_name=self._resource_table_name, caller_identity=caller_identity)

    def disconnect(self):
        self._db_conn.close()
