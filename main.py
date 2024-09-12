from filesystem import filesystem
from filesystem.readers import _read_jsonl, _read_parquet
from filesystem.readers import _read_jsonl
import dlt
import dlt.destinations as dlt_destinations
import logging

from filesystem import _read_jsonl, _read_parquet, filesystem

# Create a logger
logger = logging.getLogger("dlt")

# Set the log level
logger.setLevel(logging.INFO)

destination_fs = dlt_destinations.filesystem(bucket_url="s3://dlt-workshop/jab/raw_history/")

@dlt.source
def read_parquet_from_local_filesystem(
    table_name: str,
    folder_name: str,
    file_name: str
    ):
    yield filesystem(
            bucket_url=folder_name,
            file_glob=file_name
        ) | dlt.transformer(name=table_name)(_read_parquet)
    example_pipeline_2 = dlt.pipeline(
    pipeline_name="test_pipeline_2",
    dataset_name="synthetic_nonsense_duckdb_data",
)

@dlt.source
def read_json_from_local_filesystem(
    table_name: str,
    folder_name: str,
    file_name: str,
    incremental_load: str = None,
    ):
    fs = filesystem(
            bucket_url=folder_name,
            file_glob=file_name
        )
    fs.apply_hints(
        incremental=dlt.sources.incremental(incremental_load)
    )
    yield (
        fs | dlt.transformer(name=table_name)(_read_jsonl)
    )

destination_fs = dlt_destinations.filesystem(bucket_url="raw_history/")
example_pipeline = dlt.pipeline(
    pipeline_name="test_pipeline",
    dataset_name="synthetic_nonsense_data",
    destination=destination_fs
)

example_pipeline.run(
    read_json_from_local_filesystem(
        "synthetic_data",
        "raw_data",
        "*.jsonl",
        incremental_load="modification_date"
    ),
    loader_file_format="parquet"
)
example_pipeline_2 = dlt.pipeline(
    pipeline_name="test_pipeline_2",
    dataset_name="synthetic_nonsense_jab_data",
    destination=dlt_destinations.athena(
        query_result_bucket="s3://mojap-athena-query-dump-sandbox/",
        force_iceberg=True
    ),
    staging=dlt_destinations.filesystem("s3://dlt-workshop/jab/athena_stg")
)

example_pipeline_2.run(
    read_parquet_from_local_filesystem(
        "synthetic",
        "s3://dlt-workshop/jab/raw_history/synthetic_nonsense_data/synthetic",
        "*.parquet"
    ),
)
load_info_2 = example_pipeline_2.run(
    read_parquet_from_local_filesystem(
        "synthetic", "raw_history/synthetic_nonsense_data/synthetic", "*.parquet"
    ),
)

logger.info(load_info_2)

