import json

import pandas as pd
import pandas_gbq
from dagster import (
    AssetKey,
    AssetMaterialization,
    Field,
    InitResourceContext,
    InputContext,
    IOManager,
    OutputContext,
    StringSource,
    io_manager,
)
from google.oauth2 import service_account


class BigQueryDataFrameIOManager(IOManager):
    def __init__(self, credentials: dict, project_id: str, dataset_id: str) -> None:
        self._credentials = service_account.Credentials.from_service_account_info(
            credentials
        )
        self._project_id = project_id
        self._dataset_id = dataset_id

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        if obj is None:
            return

        table_name = context.resources.bq_table_name

        pandas_gbq.to_gbq(
            obj,
            f"{self._dataset_id}.{table_name}",
            project_id=self._project_id,
            credentials=self._credentials,
            if_exists="append",
        )

        context.add_output_metadata(
            {"dataset_id": self._dataset_id, "table_name": table_name}
        )

        context.log_event(
            AssetMaterialization(
                asset_key=AssetKey(f"dataset_{self._dataset_id}"),
                description="Updated a table within dataset",
                metadata={
                    "dataset": self._dataset_id,
                },
            )
        )

    def load_input(self, context: InputContext):
        table_name = context.resources.bq_table_name

        df = pandas_gbq.read_gbq(
            f"SELECT * FROM `{self._dataset_id}.{table_name}`",
            credentials=self._credentials,
        )
        return df


@io_manager(
    config_schema={
        "credentials": StringSource,
        "project_id": StringSource,
        "dataset_id": Field(
            str,
            default_value="listings",
            description="Dataset ID",
        ),
    },
    required_resource_keys={"bq_table_name"},
)
def bigquery_pandas_io_manager(
    init_context: InitResourceContext,
) -> BigQueryDataFrameIOManager:
    return BigQueryDataFrameIOManager(
        credentials=json.loads(init_context.resource_config["credentials"]),
        project_id=init_context.resource_config["project_id"],
        dataset_id=init_context.resource_config["dataset_id"],
    )
