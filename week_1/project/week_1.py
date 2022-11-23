import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String}, 
    out={
        "raw_data": Out(is_required=True, dagster_type=List)
    }
)
def get_s3_data(context) -> List:
    return list(csv_helper(context.op_config["s3_key"]))


@op(
    ins={"stocks": In(dagster_type=List, description="List of stocks.")},
    out={"aggr_stocks": Out(is_required=True, dagster_type=Aggregation, description="Highest aggregated stock by day.")}
)
def process_data(context, stocks: List) -> Aggregation:
    # aggregate the stocks' high by date in a dictionary
    aggr_stock_dict=dict()
    for stock in stocks:
        if str(stock.date) in aggr_stock_dict.keys():
            sum_high = aggr_stock_dict[str(stock.date)] + stock.high
            aggr_stock_dict.update({str(stock.date): sum_high})
        else:
            aggr_stock_dict[str(stock.date)] = stock.high

    # find the highest daily stock
    date, high = None, float()
    for key, value in aggr_stock_dict.items():
        if value > high:
            date, high = key, value

    high_stock = {
        'date': date,
        'high': high 
    }

    return Aggregation(**high_stock)


@op(
    ins={"highest_value": In(dagster_type=Aggregation, description="Highest aggregated stock by day.")}
)
def put_redis_data(context, highest_value: Aggregation):
    pass


@job
def week_1_pipeline():
    put_redis_data(process_data(get_s3_data()))
