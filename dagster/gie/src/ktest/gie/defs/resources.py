import os

import dagster as dg

from ktest.gie.client import GieApiResource
from ktest.gie.iceberg import IcebergTablesResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "gie_api": GieApiResource(api_key=os.environ["GIE_API_KEY"]),
            "gie_iceberg": IcebergTablesResource(
                catalog_properties={
                    "type": "rest",
                    "uri": os.environ["GIE_ICEBERG_REST_URI"],
                    "warehouse": os.environ["GIE_ICEBERG_WAREHOUSE"],
                    "s3.endpoint": os.environ["GIE_S3_ENDPOINT"],
                    "s3.access-key-id": os.environ["GIE_S3_ACCESS_KEY_ID"],
                    "s3.secret-access-key": os.environ[
                        "GIE_S3_SECRET_ACCESS_KEY"
                    ],
                },
                namespace=os.environ["GIE_ICEBERG_NAMESPACE"],
            ),
        }
    )
