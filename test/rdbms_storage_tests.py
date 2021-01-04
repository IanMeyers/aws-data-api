import unittest
import os
import sys
import tracemalloc

tracemalloc.start()

sys.path.append("../chalicelib")

import parameters as params
from pg_storage_handler import DataAPIStorageHandler
import warnings
import json
import uuid
import boto3

_resource_attr1 = '12345'
_resource_attr2 = 'abc'
_meta_attr1 = _resource_attr1
_meta_attr2 = 9876
_meta_attr3 = True
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


def teardown(storage_handler) -> None:
    cursor = storage_handler._run_commands(
        [f"drop table if exists {storage_handler._resource_table_name}",
         f"drop table if exists {storage_handler._metadata_table_name}"])
    storage_handler.disconnect()


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
    _extended_config = {
        "subnet_ids": ["subnet-dbcc31be",
                       "subnet-0dfe3d65",
                       "subnet-32af6f45"],
        "security_group_ids": ["sg-26ec6a5e"]
    }

    def create_storage_handler(self, with_name: str, resource_schema: dict, metadata_schema: dict,
                               extended_config: dict,
                               override_metaname: str = None) -> DataAPIStorageHandler:
        other_args = {
            params.CLUSTER_ADDRESS: self._cluster_address,
            params.CLUSTER_PORT: self._cluster_port,
            params.DB_USERNAME: self._cluster_user,
            params.DB_NAME: self._cluster_db,
            params.DB_USERNAME_PSTORE_ARN: "DataApiAuroraPassword",
            params.DB_USE_SSL: False,
            params.CONTROL_TYPE_RESOURCE_SCHEMA: resource_schema,
            params.CONTROL_TYPE_METADATA_SCHEMA: metadata_schema
        }

        if override_metaname is not None:
            other_args[params.OVERRIDE_METADATA_TABLENAME] = override_metaname

        sts_client = boto3.client('sts')
        account = sts_client.get_caller_identity().get('Account')
        handler = DataAPIStorageHandler(table_name=with_name, primary_key_attribute="id",
                                        region="eu-west-1",
                                        delete_mode='HARD', allow_runtime_delete_mode_change=True,
                                        table_indexes=["attr2"], metadata_indexes=None,
                                        crawler_rolename="DataAPICrawlerRole",
                                        catalog_database='data-api', allow_non_itemmaster_writes=False,
                                        strict_occv=True,
                                        gremlin_address=None, deployed_account=account,
                                        pitr_enabled=None, kms_key_arn=None,
                                        schema_validation_refresh_hitcount=None,
                                        extended_config=extended_config,
                                        **other_args)
        return handler

    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

        cls._storage_handler = cls.create_storage_handler(cls, with_name="MyItem_dev",
                                                          resource_schema=cls._resource_schema,
                                                          metadata_schema=cls._metadata_schema,
                                                          extended_config=cls._extended_config)

    @classmethod
    def tearDownClass(cls) -> None:
        teardown(cls._storage_handler)

    def test_update_clause(self):
        input = {
            "a": "12345",
            "b": 999,
            "c": False,
            "d": True
        }

        updates = self._storage_handler._synthesize_update(input, self._caller_identity)

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
        found = self._storage_handler.check("xyz")
        self.assertFalse(found)

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
        meta = meta[0]
        self.assertEqual(meta.get("meta1"), _meta_attr1)
        self.assertEqual(meta.get("meta2"), _meta_attr2)
        self.assertEqual(meta.get("meta3"), _meta_attr3)

    def test_get_item(self):
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity=self._caller_identity,
                                                            **_test_resource)
        self.assertTrue(update_response.get(params.RESOURCE).get(params.DATA_MODIFIED))

        # get the item back
        item = self._storage_handler.get(id=self._item_id).get(params.RESOURCE)

        self.assertEqual(item[0].get("attr1"), _resource_attr1)

    def test_bound_to_existing(self):
        resource_ovrr = "test_override_table"
        metadata_ovrr = "test_override_meta"
        handler = self.create_storage_handler(with_name=resource_ovrr,
                                              resource_schema=self._resource_schema,
                                              metadata_schema=self._metadata_schema,
                                              extended_config=self._extended_config,
                                              override_metaname=metadata_ovrr)

        self.assertEqual(resource_ovrr, handler._resource_table_name)
        self.assertEqual(metadata_ovrr, handler._metadata_table_name)
        teardown(handler)

    def test_item_update(self):
        pass

    def test_meta_update(self):
        pass

    def test_item_delete(self):
        pass

    def test_meta_delete(self):
        pass

    def test_item_remove_attr(self):
        pass

    def test_meta_remove_attr(self):
        pass

    def test_find(self):
        pass

    def test_restore_statement(self):
        restore = self._storage_handler._create_restore_statement(id=self._item_id,
                                                                  caller_identity=self._caller_identity)

        self.assertEqual(restore,
                         f"update myitem_dev set deleted = 0,last_update_action = 'update',last_update_date = CURRENT_TIMESTAMP,last_updated_by = '{self._caller_identity}' where id = '{self._item_id}'")


if __name__ == '__main__':
    unittest.main()
