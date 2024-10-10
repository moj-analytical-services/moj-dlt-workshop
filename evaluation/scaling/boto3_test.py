import awswrangler as wr
import datetime
import os
from pprint import pprint
import itertools
import shutil
import pandas as pd

times = {}

scales = [1]

for i in scales:
    current_time = datetime.datetime.now()
    dfs = wr.s3.read_parquet(path=f"s3://tpcds-ireland/tpcds/scale={i}/table=store_sales/",
                             chunked=True,
                             validate_schema=True
                             )
    dir = f'./tpcds-local/boto/scale={i}/'
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.mkdir(dir)
    # for n, df in enumerate(itertools.islice(dfs,2)):
    for n, df in enumerate(dfs):
        data_path = f'./tpcds-local/boto/scale={i}/data_{n}.parquet'
        df.to_parquet(path=data_path)
        print(f"chunk {n}")
    times[i] = (datetime.datetime.now() - current_time)
    print(times[i])

output = pd.read_parquet(path=dir, columns=['ss_sold_date_sk'])
print(output.shape)