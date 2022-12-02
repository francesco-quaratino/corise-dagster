from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock

from workspaces.resources import postgres_resource


@op(
    config_schema={"s3_key": String}, 
    required_resource_keys={"s3"},
    out={
        "raw_data": Out(is_required=True, dagster_type=List[Stock])
    },
    tags={"kind": "s3"},
)
def get_s3_data(context) -> List[Stock]:
    """
    This op reads a file from S3 (provided as a config schema) and converts the contents into a list of our custom data type Stock. 
    Last week we used the csv module to read the contents of a local file and return an iterator. 
    We will replace that functionality with our S3 resource and use the S3 client method get_data to read the contents 
    a file from remote storage (in this case our localstack version of S3 within Docker).
    """
    return [Stock.from_list(st) for st in context.resources.s3.get_data(context.op_config["s3_key"])]


@op(
    ins={"stocks": In(dagster_type=List[Stock], description="List of stocks.")},
    out={"aggr_stocks": Out(is_required=True, dagster_type=Aggregation, description="Highest aggregated stock by day.")},
)
def process_data(context, stocks: List[Stock]) -> Aggregation:
    highest_stock = max(stocks, key= lambda stock: stock.high)
    return Aggregation(date=highest_stock.date,high=highest_stock.high)


@op(
    ins={"highest_value": In(dagster_type=Aggregation, description="Highest aggregated stock by day.")},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
)
def put_redis_data(context, highest_value: Aggregation) -> Nothing:
    """
    This op relies on the redis_resource. In week one, our op did not do anything besides accept the output from the processing app. 
    Now we want to take that output (our Aggregation custom type) and upload it to Redis. 
    Luckily, our wrapped Redis client has a method to do just that. If you look at the put_data method, it takes in a name and a value 
    and uploads them to our cache. Our Aggregation types has two properties to it, a date and a high. 
    The date should be the name and the high should be our value, but be careful because the put_data method expects those values as strings.    
    """   
    context.resources.redis.put_data(
        str(highest_value.date),
        str(highest_value.high)
    )



@op(
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def put_s3_data(context, highest_value: Aggregation) -> Nothing:
    """
    This op also relies on the same S3 resource as get_s3_data. For the sake of this project we will use the same bucket 
    so we can leverage the same configuration. As with the redis op we will take in the aggregation from and write to it into a file in S3.
    The key name for this file should not be set in a config (as it is with the get_s3_data op) but should be generated within the op itself.
    """
    s3_key = str(highest_value.date)
    context.resources.s3.put_data( 
        s3_key,
        highest_value
    )


@graph
def week_2_pipeline():
    data = process_data(get_s3_data())
    put_redis_data(data)
    put_s3_data(data)


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
    resource_defs={"s3": mock_s3_resource, "redis":ResourceDefinition.mock_resource()}
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource}
)