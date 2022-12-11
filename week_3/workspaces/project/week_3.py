from datetime import datetime, timedelta
from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config, Backoff, Jitter, String, ScheduleEvaluationContext,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock

retry_policy = RetryPolicy(
    max_retries=10,
    delay=1,  # 200ms
)


@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    out={'stocks': Out(List[Stock])}
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    s3_client = context.resources.s3
    stocks = [Stock.from_list(stock) for stock in s3_client.get_data(s3_key)]
    return stocks

@op(
    config_schema={},
    ins={'stocks': In(List[Stock])},
    out={'stock_aggregation': Out(Aggregation)}
)
def process_data(context, stocks: List[Stock]):
    max_stock = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@op(
    config_schema={},
    required_resource_keys={"redis"},
    ins={'stock_aggregation': In(Aggregation)}
)
def put_redis_data(context, stock_aggregation: Aggregation):
    redis_client = context.resources.redis
    redis_client.put_data(stock_aggregation.date.isoformat(), str(stock_aggregation.high))


@op(
    config_schema={},
    required_resource_keys={"s3"},
    ins={'stock_aggregation': In(Aggregation)}
)
def put_s3_data(context, stock_aggregation: Aggregation):
    s3_client = context.resources.s3
    s3_client.put_data(stock_aggregation.date.isoformat(), stock_aggregation)


@graph
def week_3_pipeline():
    max_stock = process_data(get_s3_data())
    put_redis_data(max_stock)
    put_s3_data(max_stock)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
}

PARTITIONS = list(map(str, range(1, 11)))
@static_partitioned_config(partition_keys=PARTITIONS)
def docker_config(partition_key: str):
    return dict({"ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}}}, **docker)


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": ResourceDefinition.mock_resource(), "redis": ResourceDefinition.mock_resource()},
    op_retry_policy=retry_policy,
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=retry_policy,
)


week_3_schedule_local = ScheduleDefinition(
    job=week_3_pipeline_local,
    cron_schedule="*/15 * * * *",
)


@schedule(
    job=week_3_pipeline_docker,
    cron_schedule="0 * * * *",
)
def week_3_schedule_docker(ctx: ScheduleEvaluationContext):
    for p in PARTITIONS:
        request = week_3_pipeline_docker.run_request_for_partition(partition_key=p, run_key=p)
        yield request


@sensor(
    job=week_3_pipeline_docker
)
def week_3_sensor_docker(ctx):
    s3_resources = docker['resources']['s3']['config']
    five_minutes_ago = datetime.now() - timedelta(seconds=300)
    s3_keys = get_s3_keys(s3_resources['bucket'], 'prefix', s3_resources['endpoint_url'], five_minutes_ago)
    # print(s3_keys)
    if s3_keys:
        for key in s3_keys:
            yield RunRequest(
                run_key=key,
                run_config=dict({"ops": {"get_s3_data": {"config": {"s3_key": key}}}}, **docker)
            )
    else:
        yield SkipReason("No new s3 files found in bucket.")
