import awswrangler as wr

scale = 1

df = wr.s3.read_parquet(path=f"s3://tpcds-ireland/tpcds/scale={scale}/table=store_sales/")

print(df.head())

df.head().to_parquet(path=f"./tpcds-local/boto/scale={scale}/")
