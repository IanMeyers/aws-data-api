import fastjsonschema
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
import botocore
from botocore.exceptions import ClientError
import json
import time
from decimal import Decimal
from chalicelib.exceptions import *
from chalicelib.dynamo_expression_handler import DynamoUpdateExpressionHandler
from chalicelib.dynamo_table_utils import DynamoTableUtils
import chalicelib.dynamo_table_utils as dtu
import chalicelib.utils as utils
import chalicelib.parameters as params
from chalicelib.schema_cache_entry import SchemaCacheEntry

log = None

def validate_params(**kwargs):
    pass

def table_scan(table, apply_filters=None):
    filters = []
    ean = {}
    eav = {}

    args = {'Select': 'ALL_ATTRIBUTES'}

    if apply_filters is not None:
        for k, v in apply_filters.items():
            filters.append(f"#{k} = :{k}")
            ean[f"#{k}"] = k
            eav[f":{k}"] = v

        args[dtu.FE] = " AND ".join(filters)
        args[dtu.EAN] = ean
        args[dtu.EAV] = eav

    return table.scan(**args)


class DataAPIStorageHandler:
    _region = None
    _glue_client = None
    _dynamo_client = None
    _dynamo_resource = None
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
    _schema_cache = {}
    _crawler_rolename = None
    _catalog_database = None
    _allow_non_itemmaster_writes = None
    _strict_occv = False
    _dynamo_utils = None
    _deployed_account = None

    def __init__(self, table_name, primary_key_attribute, region, delete_mode, allow_runtime_delete_mode_change,
                 table_indexes, metadata_indexes, schema_validation_refresh_hitcount, crawler_rolename,
                 catalog_database, allow_non_itemmaster_writes, strict_occv, deployed_account,
                 pitr_enabled=None, kms_key_arn=None, logger=None, extended_config=None, **kwargs):
        # setup class logger
        if logger is None:
            self._logger = utils.setup_logging()
        else:
            self._logger = logger

        global log
        log = self._logger

        self._region = region
        self._table_name = table_name
        self._pk_name = primary_key_attribute
        self._deployed_account = deployed_account

        # setup dynamoDB resources
        self._dynamo_client = boto3.client('dynamodb', region_name=region)
        self._dynamo_resource = boto3.resource("dynamodb", region_name=region)
        self._dynamo_utils = DynamoTableUtils(region=self._region, logger=self._logger)

        self._sts_client = boto3.client("sts", region_name=region)
        self._delete_mode = delete_mode
        self._allow_runtime_delete_mode_change = allow_runtime_delete_mode_change
        self._schema_validation_refresh_hitcount = schema_validation_refresh_hitcount
        self._crawler_rolename = crawler_rolename
        self._catalog_database = catalog_database
        self._allow_non_itemmaster_writes = allow_non_itemmaster_writes

        if strict_occv is not None and isinstance(strict_occv, bool):
            self._strict_occv = strict_occv
        else:
            self._strict_occv = utils.strtobool(strict_occv)

        # setup the tables used for data storage
        if table_indexes is not None:
            self._table_indexes = table_indexes.split(',')

        if metadata_indexes is not None:
            self._meta_indexes = metadata_indexes.split(',')

        self._resource_table = self._setup_table(self._table_name, self._table_indexes, params.RESOURCE, pitr_enabled,
                                                 kms_key_arn)
        self._metadata_table = self._setup_table(utils.get_metaname(self._table_name), self._meta_indexes,
                                                 params.METADATA, pitr_enabled, kms_key_arn)

        # create/verify the control table
        self._control_table = self._dynamo_utils.verify_control_table()

        log.info(f'DynamoDB Storage Handler for {self._table_name} Online')

    # default table implementation method for data API tables
    def _create_default_table(self, table_name, pitr_enabled=None, kms_key_arn=None):
        args = {'AttributeDefinitions': [
            {
                'AttributeName': self._pk_name,
                'AttributeType': 'S'
            }
        ],
            'TableName': table_name,
            'KeySchema': [
                {
                    'AttributeName': self._pk_name,
                    'KeyType': 'HASH'
                },
            ],
            'BillingMode': 'PAY_PER_REQUEST',
            'StreamSpecification': {
                'StreamEnabled': True,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            }
        }

        if kms_key_arn is not None:
            args['SSESpecification'] = {
                'Enabled': True,
                'SSEType': 'KMS',
                'KMSMasterKeyId': kms_key_arn
            }

        log.debug("Creating new DDB Table")
        log.debug(args)
        self._dynamo_client.create_table(**args)

        self._dynamo_utils.wait_until_table_active(table_name)

        # turn on point in time recovery snapshotting if requested
        if pitr_enabled is not None:
            log.info(f"Activating Point in Time Recovery on {table_name}")

            # do PITR enable in a loop as there's a race condition after table creation where you can't turn it on,
            # but you also can't check the PITR status with DescribeTable :(
            enabled = False
            while enabled is False:
                try:
                    self._dynamo_client.update_continuous_backups(
                        TableName=table_name,
                        PointInTimeRecoverySpecification={
                            'PointInTimeRecoveryEnabled': pitr_enabled
                        }
                    )
                    enabled = True
                except self._dynamo_client.exceptions.ContinuousBackupsUnavailableException:
                    pass
            self._dynamo_utils.wait_until_table_active(table_name)

        return self._dynamo_resource.Table(table_name)

    # helper function to generate the name of a secondary index
    def _get_indexname(self, table_name, attr_name):
        return table_name + "-" + attr_name

    def _get_gsi_create(self, index_name, primary_attribute_name, secondary_attribute_name=None):
        gsi_create = {
            'Create': {
                'IndexName': index_name,
                'KeySchema': [
                    {
                        'AttributeName': primary_attribute_name,
                        'KeyType': 'HASH'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
        }

        if secondary_attribute_name is not None:
            gsi_create['Create']['KeySchema'].append({
                'AttributeName': secondary_attribute_name,
                'KeyType': 'RANGE'
            })

        log.debug(gsi_create)

        return gsi_create

    # method which validates or creates that the ItemMasterID index is present on a given table
    def _get_item_master_index(self, table_name):
        return {
            'IndexName': f"{self._table_name}-{params.ITEM_MASTER_ID}",
            'PrimaryKey': params.ITEM_MASTER_ID,
            'SortKey': self._pk_name
        }

    def _map_index_config_type(self, type):
        if type.lower() == 'number' or type.lower()[0:3] == 'int':
            return 'N'
        elif type.lower()[0:3] == 'bin':
            return 'B'
        else:
            return 'S'

    # method to setup all secondary indexes on a data api table given a set of index attributes and gsi's already existing
    def _configure_indexes(self, table_name, existing_gsis, indexes):
        # process each attribute requested as an index
        for i in indexes:
            index_name = i['IndexName']
            # check if the index already exists
            index_status = self._dynamo_utils.get_index_status(table_name, index_name)

            if index_status is not None:
                self._dynamo_utils.wait_until_index_active(table_name, index_name)
            else:
                attr = []
                gsi = []

                # add the attribute definitions
                if "DataType" in i and i.get("DataType") is not None:
                    set_type = self._map_index_config_type(i.get('DataType'))
                else:
                    set_type = "S"
                attr.append(
                    {
                        'AttributeName': i['PrimaryKey'],
                        'AttributeType': set_type
                    },
                )

                sort_key = None
                if 'SortKey' in i:
                    sort_key = i['SortKey']
                    attr.append(
                        {
                            'AttributeName': sort_key,
                            'AttributeType': set_type
                        },
                    )

                # add the GSI create clause
                gsi.append(self._get_gsi_create(index_name, i['PrimaryKey'], sort_key))

                log.info(f'Creating index {index_name} for table {table_name}')
                log.debug(attr)
                log.debug(gsi)

                # update the table
                try:
                    self._dynamo_client.update_table(TableName=table_name,
                                                     AttributeDefinitions=attr,
                                                     GlobalSecondaryIndexUpdates=gsi
                                                     )

                    # wait for table status to return to 'Active'
                    self._dynamo_utils.wait_until_index_active(table_name, index_name)
                except ClientError as ce:
                    log.error(ce)
                    raise ce

    def _do_table_backup(self, table_name):
        export_date = utils.get_time_now()
        response = self._dynamo_client.create_backup(
            TableName=table_name,
            BackupName=f"final-{export_date}"
        )

        if response is None:
            raise DetailedException("Final Backup Request Failed")
        else:
            backup_arn = response.get("BackupDetails").get("BackupArn")

            # wait for the backup to finish
            while True:
                response = self._dynamo_client.describe_backup(BackupArn=backup_arn)

                status = response.get("BackupDescription").get("BackupDetails").get("BackupStatus")

                if status == 'AVAILABLE':
                    return backup_arn
                else:
                    time.sleep(5)

    # method to drop a table
    def drop_table(self, table_name, do_export=True):
        if do_export is True:
            self._do_table_backup(table_name)

        self._dynamo_client.delete_table(TableName=table_name)
        self._dynamo_utils.wait_until_table_gone(table_name)

    # method which configures a data api table, including secondary indexes and Glue crawler
    def _setup_table(self, table_name, index_config, table_type, pitr_enabled=None, kms_key_arn=None):
        this_table = None
        table_desc = None
        new_table = False
        try:
            table_desc = self._dynamo_client.describe_table(TableName=table_name)
            this_table = self._dynamo_resource.Table(table_name)

            # make sure the table is active
            self._dynamo_utils.wait_until_table_active(table_name)
        except self._dynamo_client.exceptions.ResourceNotFoundException:
            log.info(f'Creating new DynamoDB table: {table_name}')
            new_table = True
            this_table = self._create_default_table(table_name, pitr_enabled, kms_key_arn)

        indexes = []
        if table_type == params.RESOURCE:
            indexes.append(self._get_item_master_index(table_name))

        if index_config is not None:
            # extract the current set of GSI's, as the create will fail if you duplicate one
            current_gsis = None
            if table_desc is not None and 'Table' in table_desc and 'GlobalSecondaryIndexes' in table_desc['Table']:
                current_gsis = table_desc['Table']['GlobalSecondaryIndexes']

            for attribute in index_config:
                self._logger.debug(f"Processing Index Configuration {attribute}")
                index_type = "string"
                if "=" in attribute:
                    tokens = attribute.split('=')
                    attribute_name = tokens[0]
                    index_type = tokens[1]
                else:
                    attribute_name = attribute

                indexes.append({
                    'IndexName': self._get_indexname(table_name, attribute_name),
                    'PrimaryKey': attribute_name,
                    'DataType': index_type
                })
            self._configure_indexes(table_name, current_gsis, indexes)

        # configure a Glue crawler for this table
        if new_table is True:
            if self._crawler_rolename is not None and self._catalog_database is not None:
                log.info(f'Creating Glue Crawler for {table_name}')
                utils.verify_crawler(table_name=table_name, crawler_rolename=self._crawler_rolename,
                                     catalog_db=self._catalog_database)
            else:
                if self._crawler_rolename is None:
                    err_reason = "No Crawler Role Name was provided"
                else:
                    err_reason = "No Catalog Database was provided"
                log.info(f'Not Configuring Glue Crawler for {table_name} as {err_reason}')

        return this_table

    # public interface method to verify that an object exists in the data API
    def check(self, id):
        item = self._fetch_item(self._resource_table, id)

        if item is not None:
            return {"Exists": True}
        else:
            raise ResourceNotFoundException()

    # method to fetch an item by ID from the data or metadata API tables
    def _fetch_item(self, table, id, force=False, only_attributes: list = None):
        args = {'Key': {
            self._pk_name: id
        }
        }

        if only_attributes is not None:
            set_whitelist = only_attributes.copy()
            # add the primary key, as the response will be nonsensical without it
            set_whitelist.append(self._pk_name)

            log.debug(f'Adding attribute whitelist {set_whitelist}')
            args['AttributesToGet'] = set_whitelist

        log.debug(f'Base Fetch from {table} Args: {args}')
        item = table.get_item(**args)

        if params.ITEM in item:
            if params.DELETED not in item[params.ITEM] or item[params.ITEM][params.DELETED] == 0 or force:
                return item[params.ITEM]
        else:
            return None

    # public method to retrieve a data or metadata Item from its respective table
    def get(self, id, suppress_meta_fetch: bool = False, only_attributes: list = None,
            not_attributes: list = None):
        log.debug(f"Storage Handler GET of Item {id}")

        item = self._fetch_item(table=self._resource_table, id=id, only_attributes=only_attributes)

        if item is None:
            raise ResourceNotFoundException(f"Invalid ID {id}")
        else:
            # filter out not_attributes
            if not_attributes is not None:
                log.debug(f"Filtering Attributes {not_attributes}")
                for attr in not_attributes:
                    if attr in item:
                        del item[attr]

            if suppress_meta_fetch is not None and suppress_meta_fetch is True:
                log.debug("Suppressing Item Metadata Retrieval")
                return self._structure_item(id, item, None)
            else:
                meta = self._fetch_meta(id)

                return self._structure_item(id, item, meta)

    # method to fetch metadata for a given item by ID
    def _fetch_meta(self, id):
        # just fetch the metadata
        meta = self._fetch_item(self._metadata_table, utils.get_metaid(id))
        if meta is not None:
            del meta[self._pk_name]

        return meta

    # method which internally structures the data API item for response, given its various facets
    def _structure_item(self, id, resource, metadata):
        if resource is not None or metadata is not None:
            item = {params.ITEM_ARN: utils.get_arn(id, self._table_name, self._deployed_account)}

            if resource is not None:
                item[params.RESOURCE] = resource

            if metadata is not None:
                item[params.METADATA] = metadata

            return item
        else:
            return None

    # public method for extracting metadata given an ID
    def get_metadata(self, id):
        # validate if the item is deleted - will throw ResourceNotFoundException if deleted
        val = self.check(id)
        meta = self._fetch_meta(id)
        return meta

    # internal method for performing an update to a dynamoDB table
    def _simple_update(self, id, update_table, update_expression, caller_identity, update_action):
        args = {
            "Key": {
                self._pk_name: id
            },
            dtu.UE: update_expression,
            "ReturnValues": "UPDATED_NEW"
        }

        if "#del" in update_expression:
            if dtu.EAN not in args:
                args[dtu.EAN] = {}
            args[dtu.EAN]["#del"] = params.DELETED

            if dtu.EAV not in args:
                args[dtu.EAV] = {}
            args[dtu.EAV][":del"] = 1

            if dtu.CE not in args:
                args[dtu.CE] = {}
            args[dtu.CE] = "attribute_not_exists(#del)"

        if ":true" in update_expression:
            if dtu.EAV not in args:
                args[dtu.EAV] = {}
            args[dtu.EAV][":true"] = True

        # add last update and last updated by
        self._dynamo_utils.decorate_update_request(args, caller_identity, update_action)

        try:
            log.debug("Performing DDB Item Update")
            log.debug(args)
            update_table.update_item(**args)

            return True
        except self._dynamo_client.exceptions.ConditionalCheckFailedException:
            # we don't care if it's already deleted
            log.debug("Item update was a delete request but Item already deleted")
            return False
        except Exception as e:
            raise DetailedException(e)

    # public method to restore an object from deleted status. Only valid for non-tombstoned items
    def restore(self, id, caller_identity):
        # fetch the item to determine if it's real
        item = self._fetch_item(table=self._resource_table, id=id, force=True)

        if item is None:
            raise ResourceNotFoundException("Invalid Object Reference")
        else:
            # update the object's deleted flag but only if tombstoned is false and it is currently deleted
            tombstone = Attr("#t")
            args = {
                "Key": {
                    self._pk_name: id
                },
                dtu.UE: "SET #d = :disabled REMOVE #t",
                dtu.CE: "#d = :enabled and (#t = :false or attribute_not_exists(#t))",
                dtu.EAN: {
                    "#t": params.TOMBSTONED,
                    "#d": "deleted"
                },
                dtu.EAV: {
                    ":enabled": 1,
                    ":disabled": 0,
                    ":false": False
                }
            }

            # add last update and last updated by
            self._dynamo_utils.decorate_update_request(args, caller_identity, params.ACTION_RESTORE)

            try:
                log.debug("Performing Item Restore")
                log.debug(args)

                self._resource_table.update_item(**args)

                log.debug("Restore complete. Fetching latest version of item")
                return self._fetch_item(self._resource_table, id)
            except self._dynamo_client.exceptions.ConditionalCheckFailedException:
                raise InvalidArgumentsException("Unable to Restore Tombstoned Resources or Resource is not Deleted")

    def _delete_metadata(self, id):
        # delete all the metadata
        self._metadata_table.delete_item(Key={
            self._pk_name: utils.get_metaid(id)
        })

        return True

    def remove_resource_attributes(self, id: str, resource_attributes: list, caller_identity: str):
        self._logger.debug(f"Performing Resource Attribute Removal of {resource_attributes}")
        return self._simple_update(id, self._resource_table, f'REMOVE {",".join(resource_attributes)}',
                                   caller_identity, params.ACTION_REMOVE_ATTRIBUTE)

    def remove_metadata_attributes(self, id: str, metadata_attributes: list, caller_identity: str):
        self._logger.debug(f"Performing Resource Attribute Removal of {metadata_attributes}")
        return self._simple_update(id, self._metadata_table, f'REMOVE {",".join(metadata_attributes)}',
                                   caller_identity, params.ACTION_REMOVE_ATTRIBUTE)

    # public method to delete an item from the data API
    def delete(self, id: str, caller_identity: str, **kwargs):
        # TODO Move delete logic around resources, metadata, and attribute removal into data api
        log.debug("Storage level Delete")
        log.debug(kwargs)

        delete_resource = True
        delete_metadata = True if self._delete_mode == params.DELETE_MODE_TOMBSTONE else False

        deleted = False

        if params.RESOURCE in kwargs or params.METADATA in kwargs:
            if params.RESOURCE in kwargs and len(kwargs.get(params.RESOURCE)) > 0:
                delete_resource = False
                delete_metadata = False

                # process a resource attribute delete request
                resource_attributes = kwargs.get(params.RESOURCE)

                if params.ITEM_MASTER_ID in resource_attributes:
                    raise InvalidArgumentsException(f"Cannot remove {params.ITEM_MASTER_ID}")
                else:
                    deleted = True
                    return self.remove_resource_attributes(id, resource_attributes, caller_identity)

            # allow the caller to specify only a metadata delete by sending just an empty Metadata list
            if params.METADATA in kwargs and params.RESOURCE not in kwargs:
                delete_resource = False

                if len(kwargs.get(params.METADATA)) > 0:
                    deleted = True
                    return self.remove_metadata_attributes(id, kwargs.get(params.METADATA), caller_identity)
                else:
                    delete_metadata = True

        if delete_metadata is True:
            self._logger.debug("Deleting Item Metadata")
            deleted = self._delete_metadata(id)

        if delete_resource is True:
            self._logger.debug("Deleting Item Resource")
            deleted = self._delete_resource(id, caller_identity, **kwargs)

        return deleted

    def _delete_resource(self, id, caller_identity, **kwargs):
        self._logger.debug("Performing deletion of full Resource")
        if params.DELETE_MODE in kwargs and self._allow_runtime_delete_mode_change is True and kwargs.get(
                params.DELETE_MODE) != self._delete_mode:
            set_delete_mode = kwargs.get(params.DELETE_MODE)
            log.debug(f"Setting Override Delete Mode {set_delete_mode}")
        else:
            if kwargs.get(params.DELETE_MODE, self._delete_mode) != self._delete_mode:
                log.debug(
                    f"Requested override delete mode {kwargs.get(params.DELETE_MODE)}, but System settings do not allow it")

            set_delete_mode = self._delete_mode

        # this is a wholesale delete of the object
        # mark the object as deleted
        upd = "SET #del = :del"

        remove_tokens = []
        if set_delete_mode == params.DELETE_MODE_TOMBSTONE:
            current = self._fetch_item(self._resource_table, id)

            if current is not None:
                # don't remove the primary key, the item version, item master, or update information
                current.pop(self._pk_name, None)
                current.pop(params.ITEM_VERSION, None)
                current.pop(params.ITEM_MASTER_ID, None)
                current.pop(params.TOMBSTONED, None)
                utils.remove_internal_attrs(current)

                # remove all attributes if tombstoning
                for k, v in current.items():
                    remove_tokens.append(k)

                # reflect in the item that it's been tombstoned
                upd = f"{upd}, {params.TOMBSTONED} = :true"
            else:
                #  no current object, so bail and indicate that nothing happened
                raise ResourceNotFoundException()

        # add remove statements for attributes if the object is being tombstoned
        if len(remove_tokens) > 0:
            upd = f"{upd} REMOVE {','.join(remove_tokens)}"

        return self._simple_update(id, self._resource_table, upd, caller_identity, params.ACTION_DELETE)

    def _sanitise_item_lastupdate_info(self, item):
        def rem(val):
            if item is not None and val in item:
                del item[val]

        rem(self._pk_name)
        rem(params.LAST_UPDATE_DATE)
        rem(params.LAST_UPDATED_BY)
        rem(params.LAST_UPDATE_ACTION)
        rem(params.ITEM_VERSION)

    # method which performs an update on a given table through a resource reference
    def _do_update(self, table, resource_id, item, caller_identity, item_version=None, is_resource_table=False,
                   update_constraints=None):
        # remove last update information off the item if it's present
        self._sanitise_item_lastupdate_info(item)

        args = {
            "Key": {
                self._pk_name: str(resource_id)
            }, "ReturnValues": "ALL_NEW"
        }

        update_expression = DynamoUpdateExpressionHandler()

        # generate the attribute name/value pairs with associated reference tables, and the SET portion of the update statement
        update_expression.add_expression_item(params.REMOVE, "#del")
        attribute_names = {"#del": params.DELETED}
        attribute_values = {}

        def _render_obj_as_update(item, handler_target):
            item_list = []

            if item is not None:
                for k, v in item.items():
                    # close over the containing attribute names and values
                    attribute_names[f"#{k}"] = k
                    attribute_values[f":{k}"] = v
                    update_token = f"#{k} = :{k}"
                    update_expression.add_expression_item(handler_target, update_token)

            return item_list

        if item_version is not None:
            _render_obj_as_update({params.ITEM_VERSION: item_version}, params.SET)

        _render_obj_as_update(item, params.SET)

        args[dtu.UE] = update_expression.get_expression()

        # if this is a resource table update, then add the details to check for item master write conditions
        if is_resource_table and not self._allow_non_itemmaster_writes:
            master = Attr(params.ITEM_MASTER_ID)
            args[dtu.CE] = master.not_exists() | master.eq(str(resource_id))

        # add any provided update constraints, or include the ItemVersion if strict occv is enabled
        if is_resource_table and (update_constraints is not None or self._strict_occv):
            conditions = []
            if dtu.CE in args:
                conditions.append(args[dtu.CE])

            if update_constraints is not None:
                condition_list = _render_obj_as_update(update_constraints)
                conditions.append(" AND ".join(condition_list))
            elif self._strict_occv:
                occv_condition = f"attribute_not_exists(#{params.ITEM_VERSION})"
                attribute_names[f"#{params.ITEM_VERSION}"] = params.ITEM_VERSION

                if item_version is not None:
                    occv_condition = occv_condition + " or #{0} = :{0}".format(params.ITEM_VERSION)
                    attribute_values[f":{params.ITEM_VERSION}"] = item[params.ITEM_VERSION]

                conditions.append(occv_condition)

            args[dtu.CE] = " AND ".join(conditions)

            # add the attribute names and values
        args[dtu.EAN] = attribute_names
        args[dtu.EAV] = attribute_values

        # decorate the request with last update date and by
        self._dynamo_utils.decorate_update_request(args, caller_identity, params.ACTION_UPDATE)

        try:
            # run the request through a json encode decode to fix float types to decimal
            # args = json.loads(json.dumps(args), parse_float=Decimal)
            log.debug("Item Update")
            log.debug(args)
            return table.update_item(**args)
        except self._dynamo_client.exceptions.ConditionalCheckFailedException:
            raise ConstraintViolationException(f"Conditional Check Violated: {args[dtu.CE]}")

    # method which implements a json schema refresh
    def _refresh_schema(self, schema_type):
        try:
            target = params.CONTROL_TYPE_RESOURCE_SCHEMA if schema_type == params.RESOURCE else params.CONTROL_TYPE_METADATA_SCHEMA
            entry = self._dynamo_utils.get_control_item(table_ref=self._control_table,
                                                        api_name=self._table_name,
                                                        control_type=target)
            schema = None
            if entry is not None:
                schema = entry.get(target)

            if schema is not None:
                cache_entry = SchemaCacheEntry(entry_type=schema_type, schema=schema,
                                               refresh_count=self._schema_validation_refresh_hitcount)
                self._schema_cache[schema_type] = cache_entry
                log.info(f"Loaded new {schema_type} Schema Validator for {self._table_name}")
            else:
                # place an empty cache entry indicating that there is no schema for this type
                log.debug(f"No {schema_type} Schema found")
                self._schema_cache[schema_type] = None
        except Exception as e:
            raise InvalidArgumentsException(f"Error during creation of {schema_type} Json Schema: {e}")

    def _validate_schema(self, item, schema_type: str, strict_schema: bool = False):
        log.debug(f'Validating {schema_type} Schema. Strict: {strict_schema}')

        # reload the schema from source configuration if there is no cache entry, if it's passed its refresh_hitcount,
        # or if strict schema checking is enabled
        if strict_schema is True or \
                schema_type not in self._schema_cache or \
                (self._schema_cache.get(schema_type) is not None and self._schema_cache.get(
                    schema_type).needs_refresh()):
            log.info(f"Reloading Schema Reference from API Metadata. Strict Validation: {strict_schema}")
            self._refresh_schema(schema_type)
        else:
            log.debug("Not reloading Schema from API Metadata")

        # get the schema from the cache
        schema = self._schema_cache.get(schema_type)

        if schema is not None:
            try:
                schema.validate_item(item)
            except fastjsonschema.exceptions.JsonSchemaException as e:
                raise SchemaViolationException(e)
        else:
            log.debug(f"No validation performed due to no {schema_type} Schema registered")

    # public method to perform an update on a data API item
    def update_item(self, caller_identity, id, **kwargs):
        log.debug(f"Update Item {id}")
        log.debug(kwargs)

        response = {}

        # validate that we have at least 1 of the top level arguments to drive an update
        valid_topargs = [params.METADATA, params.RESOURCE, params.REFERENCES]
        if not any(x in kwargs for x in valid_topargs):
            raise InvalidArgumentsException("Update Request must include {0}, {1], or {2}" % tuple(valid_topargs))

        # validate the schema for metadata and resource
        strict_schema = False if params.STRICT_SCHEMA_VALIDATION not in kwargs else utils.strtobool(kwargs.get(
            params.STRICT_SCHEMA_VALIDATION))

        # validate the JSON schemas for the modifications
        self._validate_schema(item=kwargs.get(params.RESOURCE), schema_type=params.RESOURCE,
                              strict_schema=strict_schema)
        self._validate_schema(item=kwargs.get(params.METADATA), schema_type=params.METADATA,
                              strict_schema=strict_schema)

        # process the metadata update
        if params.METADATA in kwargs:
            resource_id = utils.get_metaid(id)

            self._do_update(self._metadata_table, resource_id, kwargs.get(params.METADATA), caller_identity)

            response[params.METADATA] = {
                params.DATA_MODIFIED: True
            }

        set_constraints = None
        if params.CONSTRAINTS in kwargs:
            set_constraints = kwargs.get(params.CONSTRAINTS)
            log.debug(f"Will create Constraints {set_constraints}")

        # process the resource update
        if params.RESOURCE in kwargs:
            item = kwargs.get(params.RESOURCE)

            log.debug(f"Requested Resource Item State before Update: {json.dumps(item)}")

            # add the ID into the item for validation purposes
            if self._pk_name not in item:
                item[self._pk_name] = str(id)

            # bork early if the request includes the item master ID - this is not allowed
            if params.ITEM_MASTER_ID in item:
                raise InvalidArgumentsException(f"Cannot Update {params.ITEM_MASTER_ID}")

            # remove id from the item so we can use the item to generate the AttributeUpdates parameter
            del item[self._pk_name]

            item_version = kwargs.get(params.ITEM_VERSION)

            # perform the update
            update = self._do_update(table=self._resource_table, resource_id=id, item=item,
                                     caller_identity=caller_identity, item_version=item_version, is_resource_table=True,
                                     update_constraints=set_constraints)

            resource_response = {
                params.DATA_MODIFIED: True
            }

            # add an update warning if the update affected a non-item-master record
            item_master_id = None
            if update is not None and 'Attributes' in update:
                for k, v in update['Attributes'].items():
                    if k == params.ITEM_MASTER_ID:
                        item_master_id = v
                        break

            if item_master_id is not None and item_master_id != id:
                warning = {
                    "Message": "Updated Non Item Master",
                    self._pk_name: id,
                    params.ITEM_MASTER_ID: item_master_id
                }

                resource_response[params.WARNING] = [warning]

            response[params.RESOURCE] = resource_response

        return response

    # public method to associated an ItemMasterID with a data API item
    def item_master_update(self, caller_identity, **kwargs):
        if self._pk_name not in kwargs or params.ITEM_MASTER_ID not in kwargs:
            raise InvalidArgumentsException(f"Request must include {self._pk_name} and {params.ITEM_MASTER_ID}")
        else:
            # check that the item master exists
            item_master_id = kwargs.get(params.ITEM_MASTER_ID)

            if item_master_id is not None:
                item_master = self._fetch_item(self._resource_table, item_master_id)

                if item_master is None:
                    raise ResourceNotFoundException(f"Invalid Item Master Reference {item_master_id}")

            pk = kwargs.get(self._pk_name)
            if pk is not None and ',' in pk:
                values = pk.split(',')
            else:
                values = [pk]

            # add the item master onto each resource record
            update_response = []
            for v in values:
                # base update request
                args = {"Key": {
                    self._pk_name: str(v)
                },
                    'ReturnConsumedCapacity': 'TOTAL',
                    # require that the item already exists with a key match
                    dtu.CE: Attr(self._pk_name).eq(str(v))
                }

                # set update to change the item master if it's provided, or remove it if None
                if item_master_id is not None:
                    args[dtu.UE] = f"SET {params.ITEM_MASTER_ID} = :m"
                    args[dtu.EAV] = {
                        ":m": item_master_id
                    }
                else:
                    args[dtu.UE] = f"remove {params.ITEM_MASTER_ID}"

                # add who updates
                self._dynamo_utils.decorate_update_request(args, caller_identity, params.ACTION_UPDATE)
                response = None
                try:
                    log.debug("Item Master Link Update")
                    log.debug(args)
                    response = self._resource_table.update_item(**args)
                except self._dynamo_client.exceptions.ConditionalCheckFailedException:
                    # raised when the key to be updated was invalid - no problem - will just return false on the update
                    pass

                update_response.append({
                    self._pk_name: str(v),
                    params.DATA_MODIFIED: True if response is not None and 'ConsumedCapacity' in response and
                                                  response.get('ConsumedCapacity').get(
                                                      'CapacityUnits') > 0 else False
                })

            return update_response

    # private method which wraps scan and query API's based upon presence of indexes for the searched elements
    def _perform_query(self, table, last_key, index_attr, search_value, query_filters=None, **kwargs):
        # TODO Add support for parallel query through segments/total_segments args

        self._logger.debug("Storage Handler Query")
        index_name = self._get_indexname(table.name, index_attr)

        args = {
            "IndexName": index_name,
            "Select": 'ALL_ATTRIBUTES',
            "KeyConditionExpression": Key(index_attr).eq(search_value)
        }
        self._add_deleted_filter(args)

        if params.QUERY_PARAM_LIMIT in kwargs:
            args['Limit'] = int(kwargs.get(params.QUERY_PARAM_LIMIT))

        # add the filters to the query
        query_filter, expression_names, expression_values = self._get_filter_expression(query_filters)

        if query_filter is not None:
            self._logger.debug(f"Applying secondary query filter:{query_filter}")
            args[dtu.FE] = f"{args[dtu.FE]} and {query_filter}"
            args[dtu.EAN].update(expression_names)
            args[dtu.EAV].update(expression_values)

        # add the last_key to the scan if provided by the client
        if last_key is not None:
            args[params.EXCLUSIVE_START_KEY] = {
                index_attr: last_key
            }

        log.debug("Table Query")
        log.debug(args)
        page = table.query(**args)

        # just return the first page
        log.info(f"Queried {page['ScannedCount']} items and returned {page['Count']}")

        return {
            params.LAST_EVALUATED_KEY: page[params.LAST_EVALUATED_KEY] if params.LAST_EVALUATED_KEY in page else None,
            'Items': page['Items']}

    def _add_deleted_filter(self, args):
        deleted_filter = "(attribute_not_exists(#deleted) or #deleted <> :deleted)"
        if dtu.EAN not in args:
            args[dtu.EAN] = {}
        args[dtu.EAN]["#deleted"] = params.DELETED

        if dtu.EAV not in args:
            args[dtu.EAV] = {}
        args[dtu.EAV][":deleted"] = 1

        if dtu.FE in args:
            args[dtu.FE] = f"{args[dtu.FE]} and {deleted_filter}"
        else:
            args[dtu.FE] = deleted_filter

        return args

    # method to create a valid dynamo filter expression from a set of supplied filters
    def _get_filter_expression(self, filters: dict) -> tuple:
        filter_expressions = []
        expression_names = {}
        expression_values = {}

        if filters is not None:
            for k, v in filters.items():
                key = f'#{self._dynamo_utils.make_ddb_expressionval(k)}'
                value = f':{self._dynamo_utils.make_ddb_expressionval(k)}'
                filter_expressions.append(f"{key} = {value}")
                expression_names[key] = k
                expression_values[value] = v

            filter_expression = filter_expressions[0] if len(filter_expressions) == 1 else ' AND '.join(
                filter_expressions)

            return filter_expression, expression_names, expression_values
        else:
            return None, None, None

    # method to perform a scan operation against a data or metadata API table, returning a single page of data
    def _perform_scan(self, table, last_key, scan_filters=None, do_limit_in_scan: bool = False, **kwargs):
        self._logger.debug("Storage Handler Scan")
        args = {
            "Select": 'ALL_ATTRIBUTES'
        }

        # add filters for deleted items
        self._add_deleted_filter(args)

        # add parallel scan features if requested
        if params.QUERY_PARAM_SEGMENT in kwargs and kwargs.get(params.QUERY_PARAM_SEGMENT) is not None:
            # check if total segments has also been supplied. if not then throw an error
            if params.QUERY_PARAM_TOTAL_SEGMENTS not in kwargs or kwargs.get(params.QUERY_PARAM_TOTAL_SEGMENTS) is None:
                raise InvalidArgumentsException(
                    f"Use of Parallel Scan requires {params.QUERY_PARAM_SEGMENT} and {params.QUERY_PARAM_TOTAL_SEGMENTS}")
            else:
                try:
                    args['Segment'] = int(kwargs.get(params.QUERY_PARAM_SEGMENT))
                    args['TotalSegments'] = int(kwargs.get(params.QUERY_PARAM_TOTAL_SEGMENTS))
                except ValueError as e:
                    raise InvalidArgumentsException(
                        f"{params.QUERY_PARAM_SEGMENT} and {params.QUERY_PARAM_TOTAL_SEGMENTS} must be Integer type")

        if kwargs.get(params.QUERY_PARAM_LIMIT) is not None and do_limit_in_scan is True:
            args['Limit'] = int(kwargs.get(params.QUERY_PARAM_LIMIT))

        if kwargs.get(params.QUERY_PARAM_CONSISTENT) is not None:
            args['ConsistentRead'] = True

        scan_filter, expression_names, expression_values = self._get_filter_expression(scan_filters)

        if scan_filter is not None:
            self._logger.debug(f"Applying scan filter:{scan_filter}")
            args[dtu.FE] = f"{args[dtu.FE]} and {scan_filter}"
            args[dtu.EAN].update(expression_names)
            args[dtu.EAV].update(expression_values)

        # add the last_key to the scan if provided by the client
        if last_key is not None:
            log.debug(f"Starting Scan from {last_key}")
            args[params.EXCLUSIVE_START_KEY] = {
                self._pk_name: last_key
            }

        try:
            log.debug("DDB Scan")
            log.debug(args)

            page = table.scan(**args)
            log.info(f"Scanned {page['ScannedCount']} items and returned {page['Count']}")

            # filter the result set if we didn't implement it during the scan against the table, or if the response
            # set is larger than the default max limit
            page_items = page.get('Items')
            items = []
            page_last_key = None

            if (do_limit_in_scan is False and params.QUERY_PARAM_LIMIT in kwargs) or \
                    len(page_items) > params.DEFAULT_MAX_RESPONSE_SIZE:
                query_limit = kwargs.get(params.QUERY_PARAM_LIMIT)
                if query_limit is not None:
                    set_limit = int(query_limit)
                else:
                    set_limit = params.DEFAULT_MAX_RESPONSE_SIZE

                log.debug("Applying post-scan Limit Filter")
                for i, item in enumerate(page_items):
                    if i < set_limit:
                        items.append(item)
                        page_last_key = item.get(self._pk_name)
                    else:
                        break
            else:
                items = page_items
                page_last_key = page.get(params.LAST_EVALUATED_KEY)

            log.debug(f"Returning {len(items)} Items")

            return {params.LAST_EVALUATED_KEY: page_last_key,
                    'Items': items}
        except botocore.exceptions.ClientError as ve:
            log.error(ve)
            raise InvalidArgumentsException("Validation Exception while processing query request")

    def list_items(self, **kwargs):
        p = {
            params.QUERY_PARAM_LIMIT: kwargs.get(params.QUERY_PARAM_LIMIT)
        }

        if params.QUERY_PARAM_SEGMENT in kwargs:
            p[params.QUERY_PARAM_SEGMENT] = kwargs.get(params.QUERY_PARAM_SEGMENT)
            p[params.QUERY_PARAM_TOTAL_SEGMENTS] = kwargs.get(params.QUERY_PARAM_TOTAL_SEGMENTS)

        return self._perform_scan(table=self._resource_table, last_key=kwargs.get(params.LAST_EVALUATED_KEY),
                                  do_limit_in_scan=True, **p)

    # public method to perform a search of data or metadata
    def find(self, **kwargs):
        self._logger.debug("Performing Storage Handler Find")
        self._logger.debug(kwargs)

        if kwargs is None:
            raise InvalidArgumentsException(
                f"Find requires PUT request with {params.RESOURCE} or {params.METADATA} search criteria")
        else:
            if kwargs.get(params.RESOURCE) is not None and kwargs.get(params.METADATA) is not None:
                raise InvalidArgumentsException("Find only supports Resource or Metadata search, not both")
            elif params.RESOURCE not in kwargs and params.METADATA not in kwargs:
                raise InvalidArgumentsException("Malformed Find Request")

        index_available = False
        last_key = kwargs.get(params.EXCLUSIVE_START_KEY)

        # TODO add support for query on both metadata and resources at the same time
        if params.RESOURCE in kwargs and kwargs.get(params.RESOURCE) is not None:
            search_table = self._resource_table
            index_attrs = self._table_indexes
            search_doc = kwargs.get(params.RESOURCE)
        elif params.METADATA in kwargs and kwargs.get(params.METADATA) is not None:
            search_table = self._metadata_table
            index_attrs = self._meta_indexes
            search_doc = kwargs.get(params.METADATA)
        else:
            raise UnimplementedFeatureException('Not Implemented')

        self._logger.debug(f"Searching {search_table}")
        self._logger.debug(f"Index Attributes {index_attrs}")
        self._logger.debug(f"Search Parameters {search_doc}")

        # resolve if any of the field values requested are found in the available indexes
        if search_doc is not None:
            for k, v in search_doc.items():
                if k in index_attrs or k == params.ITEM_MASTER_ID:
                    index_available = True
                    search_index_attr = k
                    search_value = v

                    # remove this index value from the search document so that we don't have a duplicate which ddb will
                    # choke on
                    del search_doc[k]
                    break

        if index_available:
            # we'll do an index search on the first index that matched in the search set
            return self._perform_query(table=search_table, last_key=last_key, index_attr=search_index_attr,
                                       search_value=search_value, query_filters=search_doc, **kwargs)
        else:
            # there are no index columns available, so we'll scan the table
            return self._perform_scan(table=search_table, last_key=last_key, scan_filters=search_doc,
                                      do_limit_in_scan=False, **kwargs)

    # public method to return stream information for the data and metadata tables
    def get_streams(self):
        resource_table = self._resource_table.table_arn
        resource_stream = self._resource_table.latest_stream_arn
        metadata_table = self._metadata_table.table_arn
        metadata_stream = self._metadata_table.latest_stream_arn

        return {
            params.RESOURCE_TABLE_ARN: resource_table,
            params.RESOURCE_STREAM_ARN: resource_stream,
            params.METADATA_TABLE_ARN: metadata_table,
            params.METADATA_STREAM_ARN: metadata_stream
        }

    def get_usage(self, table_name):
        try:
            table_desc = self._dynamo_client.describe_table(TableName=table_name)
            if table_desc is not None and 'Table' in table_desc:
                t = table_desc['Table']

                log.debug("Table Usage")
                log.debug(t)

                return {
                    "SizeBytes": t.get("TableSizeBytes"),
                    "Count": t.get("ItemCount")
                }
        except self._dynamo_client.exceptions.ResourceNotFoundException:
            raise ResourceNotFoundException("Invalid Table Name")
