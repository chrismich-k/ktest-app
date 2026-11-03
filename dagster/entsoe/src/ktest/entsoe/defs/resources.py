import os

import dagster as dg

from ktest.entsoe.client import EntsoeApiResource
from ktest.entsoe.iceberg import IcebergTablesResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "entsoe_api": EntsoeApiResource(
                token=os.environ["ENTSOE_API_TOKEN"]
            ),
            "entsoe_iceberg": IcebergTablesResource(
                catalog_properties={
                    "type": "rest",
                    "uri": os.environ["ENTSOE_ICEBERG_REST_URI"],
                    "warehouse": os.environ["ENTSOE_ICEBERG_WAREHOUSE"],
                    "s3.endpoint": os.environ["ENTSOE_S3_ENDPOINT"],
                    "s3.access-key-id": os.environ["ENTSOE_S3_ACCESS_KEY_ID"],
                    "s3.secret-access-key": os.environ[
                        "ENTSOE_S3_SECRET_ACCESS_KEY"
                    ],
                },
                namespace=os.environ["ENTSOE_ICEBERG_NAMESPACE"],
            ),
        }
    )
