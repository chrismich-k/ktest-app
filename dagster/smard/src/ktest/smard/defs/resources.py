import os

import dagster as dg
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.arrow import PyArrowIcebergIOManager


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "io_manager": PyArrowIcebergIOManager(
                name="rest_catalog",
                config=IcebergCatalogConfig(
                    properties={
                        "type": "rest",
                        "uri": os.environ["SMARD_ICEBERG_REST_URI"],
                        "warehouse": os.environ["SMARD_ICEBERG_WAREHOUSE"],
                        "s3.endpoint": os.environ["SMARD_S3_ENDPOINT"],
                        "s3.access-key-id": os.environ[
                            "SMARD_S3_ACCESS_KEY_ID"
                        ],
                        "s3.secret-access-key": os.environ[
                            "SMARD_S3_SECRET_ACCESS_KEY"
                        ],
                    }
                ),
                namespace=os.environ["SMARD_ICEBERG_NAMESPACE"],
            )
        }
    )
