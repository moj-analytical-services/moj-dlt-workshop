# test that loaded tables are equal
import pydbtools as pydb
import awswrangler as wr

# import os

# update this in pydbtools.utils because it's not overriding it for some reason
# os.environ["ATHENA_QUERY_DUMP_BUCKET"] = "mojap-athena-query-dump-sandbox"

### update if loading new data directly to S3
s3_json_file = "s3://dlt-workshop/guy/s3_destination/s3_pipeline_dataset/destination_data/1727949349.8698199.20ce893c02.jsonl"

# load in raw file loaded to s3
df_s3 = wr.s3.read_json(
    s3_json_file,
    compression="gzip",
    lines=True,
)
print("S3 table:")
print(df_s3.head())

# load athena table
df_athena = pydb.read_sql_query(
    "SELECT * from athena_pipeline_dataset.destination_data"
)
print("Athena table:")
print(df_athena.head())

# load iceberg table
df_iceberg = pydb.read_sql_query(
    "SELECT * from athena_pipeline_iceberg_dataset.destination_data"
)
print("Iceberg table:")
print(df_iceberg.head())

# inspect ddl statements - note difference in partitioning btwn tables
athena_table_ddl = wr.athena.show_create_table(
    table="destination_data", database="athena_pipeline_dataset"
)
print("Athena table DDL:")
print(athena_table_ddl)

iceberg_table_ddl = wr.athena.show_create_table(
    table="destination_data", database="athena_pipeline_iceberg_dataset"
)
print("Iceberg table DDL:")
print(iceberg_table_ddl)


# standardise dfs to compare against each other; convert all cols to strings
def standardize_df(df):
    df = df.loc[:, ~df.columns.str.contains("dlt", case=False)]
    df = df.reindex(sorted(df.columns), axis=1)
    df = df.sort_values(list(df.columns)).reset_index(drop=True)
    df = df.astype(str)
    return df


df_s3_std = standardize_df(df_s3)
df_athena_std = standardize_df(df_athena)
df_iceberg_std = standardize_df(df_iceberg)

# compare
s3_equal_to_athena = df_s3_std.equals(df_athena_std)
print("S3 equal to Athena", s3_equal_to_athena)

s3_equal_to_iceberg = df_s3_std.equals(df_iceberg_std)
print("S3 equal to Iceberg", s3_equal_to_iceberg)

athena_equal_to_iceberg = df_iceberg_std.equals(df_athena_std)
print("Athena equal to Iceberg", athena_equal_to_iceberg)
