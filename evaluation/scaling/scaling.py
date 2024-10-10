from itertools import islice

import dlt
from dlt.destinations import filesystem as destination_filesystem
from dlt.sources.filesystem import filesystem, read_parquet

scale = 1

fs_resource = dlt.resource(
    filesystem(
        bucket_url=f"s3://tpcds-ireland/tpcds/scale={scale}/table=store_sales/",
        file_glob="*.parquet",
    ) | read_parquet()
)

dlt_destination = destination_filesystem(bucket_url=f"./tpcds-local/dlt/scale={scale}/")

dlt_pipeline = dlt.pipeline(
    pipeline_name=f"tpcds-{scale}-pipeline",
    dataset_name=f"tpcds-{scale}",
    destination=dlt_destination,
    dev_mode=True,
    progress='tqdm'
)

load_info = dlt_pipeline.run(fs_resource)

print(load_info)
