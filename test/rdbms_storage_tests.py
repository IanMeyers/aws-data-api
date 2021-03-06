import unittest
import os
import sys
import tracemalloc

tracemalloc.start()

sys.path.append("../chalicelib")

import chalicelib.parameters as params
import chalicelib.exceptions as exceptions
from chalicelib.rdbms_storage_handler import DataAPIStorageHandler
import chalicelib.rdbms_engine_types as engine_types
import warnings
import json
import uuid
import boto3
import copy

_resource_attr1 = '12345'
_resource_attr2 = 'abc'
_meta_attr1 = _resource_attr1
_meta_attr2 = 9876
_meta_attr3 = True
_API_ALIAS = "RdbmsTest_dev"
_tablename = _API_ALIAS.lower()

_test_resource = {
    params.RESOURCE: {
        "attr1": _resource_attr1,
        "attr2": _resource_attr2
    }
}

_test_metadata = {
    params.METADATA: {
        "meta1": _meta_attr1,
        "meta2": _meta_attr2,
        "meta3": _meta_attr3
    }
}


def _teardown(storage_handler) -> None:
    cursor = storage_handler.run_commands(
        [f"drop table if exists {storage_handler._resource_table_name}",
         f"drop table if exists {storage_handler._metadata_table_name}"])
    storage_handler.disconnect()


def _cleanup_metadata(storage_handler, item_id) -> None:
    cursor = storage_handler.run_commands(
        [f"delete from {storage_handler._metadata_table_name} where {storage_handler._pk_name} = '{item_id}'"])


class RdbmsStorageTests(unittest.TestCase):
    '''
    Tests for the pg_storage_handler implementation of a Data API Back End. This includes both direct interface
    tests as well as internal checks against private methods
    '''
    _cluster_address = os.environ[params.CLUSTER_ADDRESS]
    _cluster_port = 5432
    _cluster_db = 'postgres'
    _cluster_user = 'unittest'
    _storage_handler = None
    _cluster_pstore = "DataApiAuroraPassword"
    _item_id = str(uuid.uuid4())
    _caller_identity = 'bob'
    _override_tablename = 'override_table'

    # load the test schemas
    with open(f"test_resource_schema.json", 'r') as f:
        _resource_schema = json.load(f)
    with open(f"test_metadata_schema.json", 'r') as f:
        _metadata_schema = json.load(f)

    # create a simple extended configuration that binds to the subnets where the database is deployed
    _networking_config = {
        "subnet_ids": ["subnet-dbcc31be",
                       "subnet-0dfe3d65",
                       "subnet-32af6f45"],
        "security_group_ids": ["sg-26ec6a5e"]
    }

    def create_storage_handler(self, with_name: str, resource_schema: dict, metadata_schema: dict,
                               override_metaname: str = None) -> DataAPIStorageHandler:
        other_args = {
            params.CLUSTER_ADDRESS: self._cluster_address,
            params.CLUSTER_PORT: self._cluster_port,
            params.DB_USERNAME: self._cluster_user,
            params.DB_NAME: self._cluster_db,
            params.DB_USERNAME_PSTORE_ARN: "DataApiAuroraPassword",
            params.DB_USE_SSL: False,
            params.CONTROL_TYPE_RESOURCE_SCHEMA: resource_schema,
            params.CONTROL_TYPE_METADATA_SCHEMA: metadata_schema,
            params.RDBMS_DIALECT: engine_types.DIALECT_PG
        }

        other_args.update(self._networking_config)

        if override_metaname is not None:
            other_args[params.OVERRIDE_METADATA_TABLENAME] = override_metaname

        sts_client = boto3.client('sts')
        account = sts_client.get_caller_identity().get('Account')
        handler = DataAPIStorageHandler(table_name=with_name, primary_key_attribute="id",
                                        region="eu-west-1",
                                        delete_mode=params.DELETE_MODE_HARD, allow_runtime_delete_mode_change=True,
                                        table_indexes=["attr2"], metadata_indexes=None,
                                        crawler_rolename="DataAPICrawlerRole",
                                        catalog_database='data-api', allow_non_itemmaster_writes=False,
                                        strict_occv=True,
                                        gremlin_address=None, deployed_account=account,
                                        pitr_enabled=None, kms_key_arn=None,
                                        schema_validation_refresh_hitcount=None,
                                        **other_args)
        return handler

    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

        cls._storage_handler = cls.create_storage_handler(cls, with_name=_API_ALIAS,
                                                          resource_schema=cls._resource_schema,
                                                          metadata_schema=cls._metadata_schema)

    @classmethod
    def tearDownClass(cls) -> None:
        _teardown(cls._storage_handler)

    def test_update_clause(self):
        input = {
            "a": "12345",
            "b": 999,
            "c": False,
            "d": True
        }

        updates = self._storage_handler._json_to_column_list(input, self._caller_identity)

        self.assertEqual(8, len(updates))
        self.assertEqual(updates[0], "a = '12345'")
        self.assertEqual(updates[1], 'b = 999')
        self.assertEqual(updates[2], 'c = 0')
        self.assertEqual(updates[3], 'd = 1')

        update_statement = self._storage_handler._create_update_statement(table_ref='my_table', pk_name="id",
                                                                          input=input, item_id="123",
                                                                          caller_identity=self._caller_identity)

        self.assertEqual(update_statement,
                         "update my_table set a = '12345',b = 999,c = 0,d = 1,last_update_action = 'update',last_update_date = CURRENT_TIMESTAMP,last_updated_by = 'bob',item_version = item_version+1 where id = '123' and deleted = FALSE")

    def test_insert_clause(self):
        input = {
            "a": "12345",
            "b": 999,
            "c": False,
            "d": True
        }

        inserts = self._storage_handler._synthesize_insert(pk_name="id", pk_value=self._item_id, input=input,
                                                           caller_identity=self._caller_identity)

        columns = inserts[0]
        values = inserts[1]

        # check column output - 5 columns provided and 4 who columns
        self.assertEqual(9, len(columns))
        for i, k in enumerate(input):
            self.assertEqual(k, columns[i + 1])

        # check value output
        self.assertEqual(9, len(values))
        self.assertEqual(values[0], f"'{self._item_id}'")
        self.assertEqual(values[1], "'12345'")
        self.assertEqual(values[2], 999)
        self.assertEqual(values[3], 0)
        self.assertEqual(values[4], 1)

        insert = self._storage_handler._create_insert_statement(table_ref='mytable', pk_name="id",
                                                                pk_value=self._item_id, input=input,
                                                                caller_identity=self._caller_identity)
        self.assertEqual(insert,
                         f"insert into mytable (id,a,b,c,d,item_version,last_update_action,last_update_date,last_updated_by) values ('{self._item_id}','12345',999,0,1,0,'create',CURRENT_TIMESTAMP,'{self._caller_identity}')")

    def test_check_no_object(self):
        with self.assertRaises(exceptions.ResourceNotFoundException):
            self._storage_handler.check("xyz")

    def test_put_check_resource(self):
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **_test_resource)
        self.assertTrue(update_response.get(params.RESOURCE).get(params.DATA_MODIFIED))

        # check that the item exists
        item = self._storage_handler.check(id=self._item_id)
        self.assertTrue(item)

    def test_put_get_metadata(self):
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **_test_metadata)
        self.assertTrue(update_response.get(params.METADATA).get(params.DATA_MODIFIED))

        meta = self._storage_handler.get_metadata(id=self._item_id)
        self.assertIsNotNone(meta)
        self.assertEqual(meta.get("meta1"), _meta_attr1)
        self.assertEqual(meta.get("meta2"), _meta_attr2)
        self.assertEqual(meta.get("meta3"), _meta_attr3)

        # cleanup metadata
        _cleanup_metadata(self._storage_handler, self._item_id)

    def test_get_item(self):
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **_test_resource)
        self.assertTrue(update_response.get(params.RESOURCE).get(params.DATA_MODIFIED))

        # get the item back
        item = self._storage_handler.get(id=self._item_id).get(params.RESOURCE)

        self.assertEqual(item.get("attr1"), _resource_attr1)

    def test_bound_to_existing(self):
        resource_ovrr = "test_override_table"
        metadata_ovrr = "test_override_meta"
        handler = self.create_storage_handler(with_name=resource_ovrr,
                                              resource_schema=self._resource_schema,
                                              metadata_schema=self._metadata_schema,
                                              override_metaname=metadata_ovrr)

        self.assertEqual(resource_ovrr, handler._resource_table_name)
        self.assertEqual(metadata_ovrr, handler._metadata_table_name)
        _teardown(handler)

    def test_item_update(self):
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **_test_resource)
        self.assertTrue(update_response.get(params.RESOURCE).get(params.DATA_MODIFIED))

        # modify the test resource
        upd = "updated_test_value"
        resource = copy.deepcopy(_test_resource)
        resource["attr2"] = upd
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **resource)
        self.assertTrue(update_response.get(params.RESOURCE).get(params.DATA_MODIFIED))

        # check that the update worked
        item = self._storage_handler.get(id=self._item_id)
        self.assertTrue(item.get(params.RESOURCE).get("attr2"), upd)

    def test_meta_update(self):
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **_test_metadata)
        self.assertTrue(update_response.get(params.METADATA).get(params.DATA_MODIFIED))

        # modify the test metadata
        newval = _meta_attr2 + 1
        meta = copy.deepcopy(_test_metadata)
        meta[params.METADATA]["meta2"] = newval
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **meta)

        # check that the update worked
        meta = self._storage_handler.get_metadata(id=self._item_id)
        self.assertTrue(meta.get("meta2"), newval)

        # cleanup metadata
        _cleanup_metadata(self._storage_handler, self._item_id)

    def test_item_delete(self):
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **_test_resource)
        self.assertTrue(update_response.get(params.RESOURCE).get(params.DATA_MODIFIED))

        delete_response = self._storage_handler.delete(id=self._item_id, caller_identity=self._caller_identity)

        self.assertIsNotNone(delete_response)
        self.assertIsNotNone(delete_response.get(params.RESOURCE))
        self.assertTrue(delete_response.get(params.RESOURCE))

    def test_meta_delete(self):
        union = {
            params.RESOURCE: _test_resource.get(params.RESOURCE),
            params.METADATA: _test_metadata.get(params.METADATA)
        }
        # create resource and metadata
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **union)

        # confirm I get fetch the resource and metadata
        item = self._storage_handler.get(id=self._item_id)
        self.assertIsNotNone(item)
        self.assertIsNotNone(item.get(params.RESOURCE))
        self.assertIsNotNone(item.get(params.METADATA))

        # delete the metadata
        response = self._storage_handler.delete(id=self._item_id, caller_identity=self._caller_identity,
                                                **{params.METADATA: []})
        self.assertIsNotNone(response)
        self.assertTrue(response.get(params.METADATA).get(params.DATA_MODIFIED))

        # confirm I can't get the metadata
        with self.assertRaises(exceptions.ResourceNotFoundException):
            self._storage_handler.get_metadata(id=self._item_id)

        # confirm I can get the resource
        item = self._storage_handler.get(id=self._item_id, suppress_meta_fetch=True)
        self.assertIsNotNone(item)
        self.assertIsNotNone(item.get(params.RESOURCE))

    def test_item_remove_attr(self):
        # create an item with a value to be removed
        c = "attr4"
        item = copy.deepcopy(_test_resource)
        item[params.RESOURCE][c] = 1000
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **item)
        self.assertTrue(update_response.get(params.RESOURCE).get(params.DATA_MODIFIED))

        # make sure the created object is correct
        i = self._storage_handler.get(id=self._item_id, suppress_meta_fetch=True)
        self.assertIsNotNone(i)
        self.assertIsNotNone(i.get(params.RESOURCE))
        self.assertIsNotNone(i.get(params.RESOURCE).get(c))

        # delete the attr4 from the object
        delete_response = self._storage_handler.delete(id=self._item_id, caller_identity=self._caller_identity,
                                                       **{params.RESOURCE: [c]})
        i = self._storage_handler.get(id=self._item_id, suppress_meta_fetch=True)
        self.assertIsNotNone(i)
        self.assertIsNotNone(i.get(params.RESOURCE))
        self.assertIsNone(i.get(params.RESOURCE).get(c))

    def test_meta_remove_attr(self):
        # create an item with a value to be removed
        c = "meta3"
        item = copy.deepcopy(_test_metadata)
        item[params.METADATA][c] = False
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **item)
        self.assertTrue(update_response.get(params.METADATA).get(params.DATA_MODIFIED))

        # make sure the created object is correct
        i = self._storage_handler.get_metadata(id=self._item_id)
        self.assertIsNotNone(i)
        self.assertIsNotNone(i.get(c))

        # delete the meta3 from the object
        delete_response = self._storage_handler.delete(id=self._item_id, caller_identity=self._caller_identity,
                                                       **{params.METADATA: [c]})
        i = self._storage_handler.get_metadata(id=self._item_id)
        self.assertIsNotNone(i)
        self.assertIsNone(i.get(c))

    def _create_ten_random(self):
        for x in range(10):
            proto = {
                params.RESOURCE: copy.deepcopy(_test_resource).get(params.RESOURCE),
                params.METADATA: copy.deepcopy(_test_metadata).get(params.METADATA)
            }
            proto[params.RESOURCE]["attr2"] = f'{proto.get(params.RESOURCE).get("attr2")}-{x}'
            proto[params.METADATA]["meta1"] = f'{proto.get(params.METADATA).get("meta1")}-{x}'

            create_response = self._storage_handler.update_item(id=str(x), caller_identity=self._caller_identity,
                                                                **proto)

            self.assertTrue(create_response.get(params.RESOURCE).get(params.DATA_MODIFIED))
            self.assertTrue(create_response.get(params.METADATA).get(params.DATA_MODIFIED))

    def test_item_master_update(self):
        self._create_ten_random()

        # update items 3 and 8 to have item master 2
        update = {
            "id": "3,8",
            params.ITEM_MASTER_ID: "2"
        }
        update_response = self._storage_handler.item_master_update(caller_identity=self._caller_identity, **update)

        self.assertIsNotNone(update_response)
        self.assertEqual(update_response.get("RecordCount"), 2)

        # update a single item master, 7-9
        update = {
            "id": "7",
            params.ITEM_MASTER_ID: "9"
        }
        update_response = self._storage_handler.item_master_update(caller_identity=self._caller_identity, **update)
        self.assertIsNotNone(update_response)
        self.assertEqual(update_response.get("RecordCount"), 1)

        # check that I can unlink an item master
        update = {
            "id": "7",
            params.ITEM_MASTER_ID: None
        }
        update_response = self._storage_handler.item_master_update(caller_identity=self._caller_identity, **update)
        self.assertIsNotNone(update_response)
        self.assertEqual(update_response.get("RecordCount"), 1)
        item = self._storage_handler.get(id="7", suppress_meta_fetch=True)
        self.assertIsNotNone(item)
        self.assertIsNone(item.get(params.RESOURCE).get(params.ITEM_MASTER_ID))

        # ensure invalid item master links fail
        with self.assertRaises(exceptions.ResourceNotFoundException):
            update = {
                "id": "5",
                params.ITEM_MASTER_ID: "impossible_id"
            }
            update_response = self._storage_handler.item_master_update(caller_identity=self._caller_identity, **update)

        self._storage_handler.run_commands(commands=[f"delete from {self._storage_handler._resource_table_name}"])

    def test_find(self):
        # seed the resource and metadata tables with 10 records
        self._create_ten_random()

        # issue a find for a single resource
        find_request = {
            params.RESOURCE: {
                "attr2": "abc-7"
            }
        }
        found = self._storage_handler.find(**find_request)
        self.assertEqual("dict", type(found).__name__)
        self.assertEqual(found.get("id"), "7")

        # query for multiple resources
        find_request = {
            params.RESOURCE: {
                "attr1": _resource_attr1
            }
        }
        found = self._storage_handler.find(**find_request)
        self.assertEqual("list", type(found).__name__)
        self.assertEqual(10, len(found))

        # issue a find for metadata
        find_request = {
            params.METADATA: {
                "meta1": "12345-4"
            }
        }
        found = self._storage_handler.find(**find_request)
        self.assertEqual("dict", type(found).__name__)
        self.assertEqual(found.get("id"), "4")

        # query for multiple resources
        find_request = {
            params.METADATA: {
                "meta2": _meta_attr2
            }
        }
        found = self._storage_handler.find(**find_request)
        self.assertEqual("list", type(found).__name__)
        self.assertEqual(10, len(found))

        self._storage_handler.run_commands(commands=[f"delete from {self._storage_handler._resource_table_name}"])

    def test_restore_statement(self):
        restore = self._storage_handler._create_restore_statement(id=self._item_id,
                                                                  caller_identity=self._caller_identity)

        self.assertEqual(restore,
                         f"update {_tablename} set deleted = 0,last_update_action = 'update',last_update_date = CURRENT_TIMESTAMP,last_updated_by = '{self._caller_identity}' where id = '{self._item_id}'")


if __name__ == '__main__':
    unittest.main()
