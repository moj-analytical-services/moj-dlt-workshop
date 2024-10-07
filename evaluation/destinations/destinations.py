from datetime import date
import os
import dlt
from dlt.destinations.adapters import athena_partition, athena_adapter

# create sample data
data_items = [
    (1, "A", date(2021, 1, 1)),
    (2, "A", date(2021, 1, 2)),
    (3, "A", date(2021, 1, 3)),
    (4, "A", date(2021, 2, 1)),
    (5, "A", date(2021, 2, 2)),
    (6, "B", date(2021, 1, 1)),
    (7, "B", date(2021, 1, 2)),
    (8, "B", date(2021, 1, 3)),
    (9, "B", date(2021, 2, 1)),
    (10, "B", date(2021, 3, 2)),
]


# define sample data as a dlt resource
@dlt.resource(write_disposition='replace')
def destination_data():
    yield [{"id": i, "category": c, "created_at": d} for i, c, d in data_items]


# pipeline to load directly to S3:
def s3_pipeline():
    pipeline_s3 = dlt.pipeline(
        pipeline_name="s3_pipeline",
        destination=dlt.destinations.filesystem(
            bucket_url="s3://dlt-workshop/guy/s3_destination"
        ),
    )
    info = pipeline_s3.run(destination_data())
    print(info)
    print("s3 pipeline complete")


# pipeline to load data to athena (not iceberg)
def athena_pipeline():
    os.environ["BUCKET_URL"] = "s3://dlt-workshop/guy/athena_destination"
    os.environ["QUERY_RESULT_BUCKET"] = "s3://dlt-workshop/guy/athena_destination"

    # load data to athena
    pipeline_athena = dlt.pipeline(
        pipeline_name="athena_pipeline", destination="athena"
    )
    info = pipeline_athena.run(destination_data())
    print(info)
    print("athena pipeline complete")


# pipeline to load data to athena (iceberg and partitioned)
def athena_pipeline_iceberg():
    os.environ["BUCKET_URL"] = "s3://dlt-workshop/guy/athena_iceberg_destination"
    os.environ["QUERY_RESULT_BUCKET"] = (
        "s3://dlt-workshop/guy/athena_iceberg_destination"
    )

    athena_adapter(
        destination_data,
        partition=[
            "category",
            athena_partition.month("created_at"),
        ],
    )
    pipeline_athena_iceberg = dlt.pipeline(
        pipeline_name="athena_pipeline_iceberg", destination="athena"
    )
    info = pipeline_athena_iceberg.run(destination_data(), table_format="iceberg")
    print(info)
    print("athena iceberg pipeline complete")


if __name__ == "__main__":
    s3_pipeline()
    athena_pipeline()
    athena_pipeline_iceberg()
