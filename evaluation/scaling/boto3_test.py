import awswrangler as wr
import datetime
import os
from pprint import pprint
# scale = 1

# current_time = datetime.datetime.now()

# df = wr.s3.read_parquet(path=f"s3://tpcds-ireland/tpcds/scale={scale}/table=store_sales/")

# print(df.head())

# df.to_parquet(path=f"./tpcds-local/boto/scale={scale}/data.parquet")

# print(datetime.datetime.now() - current_time)

times = {}

scales = [1,2,500,1000]

for i in scales:
    current_time = datetime.datetime.now()
    df = wr.s3.read_parquet(path=f"s3://tpcds-ireland/tpcds/scale={i}/table=store_sales/")
    print(df.head())
    os.mkdir(f'./tpcds-local/boto/scale={i}/')
    os.remove(f'./tpcds-local/boto/scale={i}/data.parquet')
    df.head().to_parquet(path=f"./tpcds-local/boto/scale={i}/data.parquet")
    times[i] = (datetime.datetime.now() - current_time)

pprint(times)