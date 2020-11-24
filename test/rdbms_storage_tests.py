import unittest
import os
import sys
import tracemalloc

tracemalloc.start()

sys.path.append("../chalicelib")

import parameters as params
from aurora_pg_storage_handler import AuroraPostgresStorageHandler
import warnings
import json
import uuid


class RdbmsStorageTests(unittest.TestCase):
    _cluster_address = os.environ[params.CLUSTER_ADDRESS]
    _cluster_port = 5432
    _cluster_db = 'postgres'
    _cluster_user = 'unittest'
    _storage_handler = None
    _cluster_pstore = "DataApiAuroraPassword"
    _item_id = str(uuid.uuid4())

    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

        # load the test schemas
        with open(f"test_resource_schema.json", 'r') as f:
            resource_schema = json.load(f)
        with open(f"test_metadata_schema.json", 'r') as f:
            metadata_schema = json.load(f)

        other_args = {
            params.CLUSTER_ADDRESS: cls._cluster_address,
            params.CLUSTER_PORT: cls._cluster_port,
            params.DB_USERNAME: cls._cluster_user,
            params.DB_NAME: cls._cluster_db,
            params.DB_USERNAME_PSTORE_ARN: "DataApiAuroraPassword",
            params.DB_USE_SSL: False,
            params.CONTROL_TYPE_RESOURCE_SCHEMA: resource_schema,
            params.CONTROL_TYPE_METADATA_SCHEMA: metadata_schema
        }
        cls._storage_handler = AuroraPostgresStorageHandler(table_name="MyItem_dev", primary_key_attribute="id",
                                                            region="eu-west-1",
                                                            delete_mode='HARD', allow_runtime_delete_mode_change=True,
                                                            table_indexes=["attr2"], metadata_indexes=None,
                                                            crawler_rolename=None,
                                                            catalog_database=None, allow_non_itemmaster_writes=False,
                                                            strict_occv=True,
                                                            gremlin_address=None, deployed_account=None,
                                                            pitr_enabled=None, kms_key_arn=None,
                                                            schema_validation_refresh_hitcount=None, **other_args)

    @classmethod
    def tearDownClass(cls) -> None:
        cursor = cls._storage_handler._run_commands(
            [f"drop table if exists {cls._storage_handler._resource_table_name}",
             f"drop table if exists {cls._storage_handler._metadata_table_name}"])
        cls._storage_handler.disconnect()

    def test_update_clause(self):
        input = {
            "a": "12345",
            "b": 999,
            "c": False,
            "d": True
        }

        updates = self._storage_handler._synthesize_update(input)

        self.assertEqual(4, len(updates))
        self.assertEqual(updates[0], "a = '12345'")
        self.assertEqual(updates[1], 'b = 999')
        self.assertEqual(updates[2], 'c = 0')
        self.assertEqual(updates[3], 'd = 1')

        update_statement = self._storage_handler._create_update_statement(table_ref='my_table', pk_name="id",
                                                                          input=input, item_id="123")

        self.assertEqual(update_statement, "update my_table set a = '12345',b = 999,c = 0,d = 1 where id = '123'")

    def test_insert_clause(self):
        input = {
            "a": "12345",
            "b": 999,
            "c": False,
            "d": True
        }

        inserts = self._storage_handler._synthesize_insert(pk_name="id", pk_value=self._item_id, input=input)

        columns = inserts[0]
        values = inserts[1]

        # check column output
        self.assertEqual(5, len(columns))
        for i, k in enumerate(input):
            self.assertEqual(k, columns[i + 1])

        # check value output
        self.assertEqual(5, len(values))
        self.assertEqual(values[0], f"'{self._item_id}'")
        self.assertEqual(values[1], "'12345'")
        self.assertEqual(values[2], 999)
        self.assertEqual(values[3], 0)
        self.assertEqual(values[4], 1)

        insert = self._storage_handler._create_insert_statement(table_ref='mytable', pk_name="id",
                                                                pk_value=self._item_id, input=input)
        self.assertEqual(insert, f"insert into mytable (id,a,b,c,d) values ('{self._item_id}','12345',999,0,1)")

    def test_check(self):
        found = self._storage_handler.check("xyz")
        self.assertFalse(found)

    def test_update_item(self):
        v1 = '12345'
        v2 = 'abc'
        item = {
            "attr1": v1,
            "attr2": v2
        }
        update_response = self._storage_handler.update_item(id=self._item_id, caller_identity='bob', **item)
        self.assertTrue(update_response)

        # check that the item exists
        item = self._storage_handler.check(id=self._item_id)
        self.assertTrue(item)


if __name__ == '__main__':
    unittest.main()
