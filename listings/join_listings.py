from dagster import (
    AssetKey,
    EventLogEntry,
    RunRequest,
    SensorEvaluationContext,
    asset_sensor,
    job,
)


@job
def join_listings():
    pass


@asset_sensor(asset_key=AssetKey("dataset_listings"), job=join_listings)
def listings_updated_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
):
    yield RunRequest(
        run_key=context.cursor,
        run_config={
            "ops": {
                "read_materialization": {
                    "config": {
                        "asset_key": asset_event.dagster_event.asset_key.path,
                    }
                }
            }
        },
    )
