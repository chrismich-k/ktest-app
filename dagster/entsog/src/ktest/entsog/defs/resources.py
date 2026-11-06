import os

import dagster as dg

from ktest.entsog.client import EntsogApiResource
from ktest.entsog.iceberg import IcebergTablesResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "entsog_api": EntsogApiResource(),
            "entsog_iceberg": IcebergTablesResource(
                catalog_properties={
                    "type": "rest",
                    "uri": os.environ["ENTSOG_ICEBERG_REST_URI"],
                    "warehouse": os.environ["ENTSOG_ICEBERG_WAREHOUSE"],
                    "s3.endpoint": os.environ["ENTSOG_S3_ENDPOINT"],
                    "s3.access-key-id": os.environ["ENTSOG_S3_ACCESS_KEY_ID"],
                    "s3.secret-access-key": os.environ[
                        "ENTSOG_S3_SECRET_ACCESS_KEY"
                    ],
                },
                namespace=os.environ["ENTSOG_ICEBERG_NAMESPACE"],
            ),
        }
    )
