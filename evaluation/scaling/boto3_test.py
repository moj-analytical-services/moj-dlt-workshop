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
    dfs = wr.s3.read_parquet(path=f"s3://tpcds-ireland/tpcds/scale={i}/table=store_sales/",chunked=True)
    dir = f'./tpcds-local/boto/scale={i}/'
    if not os.path.exists(dir):
        os.mkdir(dir)
    data_path = f'./tpcds-local/boto/scale={i}/data.parquet'
    if os.path.exists(data_path):
        os.remove(data_path)
    for df in dfs[:2]:
        df.to_parquet(path=data_path)
    times[i] = (datetime.datetime.now() - current_time)
    print(times[i])

pprint(times)