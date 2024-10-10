from itertools import islice

import dlt
from dlt.destinations import filesystem as destination_filesystem
from dlt.sources.filesystem import filesystem, read_parquet

scale = 1

@dlt.resource(parallelized=True)
def dlt_source():
    source_data = filesystem(
        bucket_url=f"s3://tpcds-ireland/tpcds/scale={scale}/table=store_sales/",
        file_glob="*.parquet",
    ) | read_parquet(chunksize=100000)
    while item_slice := list(islice(source_data, 100000)):
        print(f"got chunk of length {len(item_slice)}")
        yield item_slice


dlt_destination = destination_filesystem(bucket_url=f"./tpcds-local/dlt/scale={scale}/")

dlt_pipeline = dlt.pipeline(
    pipeline_name=f"tpcds-{scale}-pipeline",
    dataset_name=f"tpcds-{scale}",
    destination=dlt_destination,
    dev_mode=True,
    progress="tqdm",
)

load_info = dlt_pipeline.run(dlt_source(), write_disposition="replace")

print(load_info)
