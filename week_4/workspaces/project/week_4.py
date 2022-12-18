from typing import List

from dagster import Nothing, String, asset, with_resources, StaticPartitionsDefinition
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    group_name='corise',
    # partitions_def=StaticPartitionsDefinition([f"stock_{partition_key}" for partition_key in range(1, 11)])
)
def get_s3_data(context):
    # You can reuse the logic from the previous week
    # s3_key = context.asset_partition_key_for_output()
    s3_key = context.op_config["s3_key"]
    s3_client = context.resources.s3

    # stocks = [Stock.from_list(stock) for stock in s3_client.get_data(f"prefix/{s3_key}.csv")]
    stocks = [Stock.from_list(stock) for stock in s3_client.get_data(s3_key)]
    return stocks


@asset(
    config_schema={},
    group_name='corise'
)
def process_data(get_s3_data):
    # You can reuse the logic from the previous week
    max_stock = max(get_s3_data, key=lambda stock: stock.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@asset(
    config_schema={},
    required_resource_keys={"redis"},
    group_name='corise'
)
def put_redis_data(context, process_data):
    # You can reuse the logic from the previous week
    redis_client = context.resources.redis
    redis_client.put_data(process_data.date.isoformat(), str(process_data.high))


@asset(
    config_schema={},
    required_resource_keys={"s3"},
    group_name='corise'
)
def put_s3_data(context, process_data):
    # You can reuse the logic from the previous week
    s3_client = context.resources.s3
    s3_client.put_data(process_data.date.isoformat(), process_data)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={'redis': redis_resource, 's3': s3_resource},
    resource_config_by_key={
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
)
