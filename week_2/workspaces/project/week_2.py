from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op, get_dagster_logger
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock



@op(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},
    out={'stocks': Out(List[Stock])}
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    s3_client = context.resources.s3
    get_dagster_logger().info(s3_key)
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
    s3_client.put_data(stock_aggregation.date.isoformat(), str(stock_aggregation.high))


@graph
def week_2_pipeline():
    max_stock = process_data(get_s3_data())
    put_redis_data(max_stock)
    put_s3_data(max_stock)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={"s3": ResourceDefinition.mock_resource(), "redis": ResourceDefinition.mock_resource()},
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
