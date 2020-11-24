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

class RdbmsStorageTests(unittest.TestCase):
    _cluster_address = os.environ[params.CLUSTER_ADDRESS]
    _cluster_port = 5432
    _cluster_db = 'postgres'
    _cluster_user = 'unittest'
    _storage_handler = None
    _cluster_pstore = "DataApiAuroraPassword"

    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*<ssl.SSLSocket.*>")

        # load the test schema
        with open(f"test_resource_schema.json", 'r') as f:
            json_schema = json.load(f)

        other_args = {
            params.CLUSTER_ADDRESS: cls._cluster_address,
            params.CLUSTER_PORT: cls._cluster_port,
            params.DB_USERNAME: cls._cluster_user,
            params.DB_NAME: cls._cluster_db,
            params.DB_USERNAME_PSTORE_ARN: "DataApiAuroraPassword",
            params.DB_USE_SSL: False,
            params.CONTROL_TYPE_RESOURCE_SCHEMA: json_schema
        }
        cls._storage_handler = AuroraPostgresStorageHandler(table_name="MyItem_dev", primary_key_attribute="id",
                                                            region="eu-west-1",
                                                            delete_mode='HARD', allow_runtime_delete_mode_change=True,
                                                            table_indexes=None, metadata_indexes=None,
                                                            crawler_rolename=None,
                                                            catalog_database=None, allow_non_itemmaster_writes=False,
                                                            strict_occv=True,
                                                            gremlin_address=None, deployed_account=None,
                                                            pitr_enabled=None, kms_key_arn=None,
                                                            schema_validation_refresh_hitcount=None, **other_args)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._storage_handler.disconnect()

    def test_setup(self):
        self.assertIsNotNone(self._storage_handler)


if __name__ == '__main__':
    unittest.main()
