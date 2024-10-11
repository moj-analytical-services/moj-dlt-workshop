from itertools import islice

import dlt
from dlt.destinations import filesystem as destination_filesystem
from dlt.sources.filesystem import filesystem, read_parquet
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
scale = 1
 

def our_chunk(chunk):
    return chunk  

# Function to calculate optimal chunk size based on memory
def get_optimal_chunk_size():
    available_memory = psutil.virtual_memory().available
    # This is where we adjust
    return min(max(int(available_memory / (1 * 1024 * 1024)), 10_000), 100_000)
 
optimal_chunk_size = get_optimal_chunk_size()
print(f"{optimal_chunk_size=}")

dlt_source = dlt.resource(filesystem(
        bucket_url=f"s3://tpcds-ireland/tpcds/scale={scale}/table=store_sales/",
        file_glob="*.parquet",
    ) | read_parquet(chunksize=optimal_chunk_size), parallelized=True)

dlt_destination = destination_filesystem(bucket_url=f"./tpcds-local/dlt/scale={scale}/")

dlt_pipeline = dlt.pipeline(
    pipeline_name=f"tpcds-{scale}-pipeline",
    dataset_name=f"tpcds-{scale}",
    destination=dlt_destination,
    dev_mode=True,
    progress='tqdm'
)

load_info = dlt_pipeline.run(dlt_source, loader_file_format="parquet")

print(load_info)
