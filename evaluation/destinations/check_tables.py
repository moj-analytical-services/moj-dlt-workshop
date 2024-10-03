# test that loaded tables are equal
import pydbtools as pydb
import awswrangler as wr
import boto3

# import os
# update this in pydbtools.utils because it's not overriding it for some reason
# os.environ["ATHENA_QUERY_DUMP_BUCKET"] = "mojap-athena-query-dump-sandbox"

print("Checking tables are equal")

# load latest uploaded S3 file in bucket
s3 = boto3.client('s3')

# Specify the bucket name and optional prefix
bucket_name = 'dlt-workshop'
prefix = 'guy/s3_destination/s3_pipeline_dataset/destination_data'

# List objects in the bucket
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

# Get the list of all objects sorted by last modified date in descending order
if 'Contents' in response:
    all_files = response['Contents']
    latest_file = max(all_files, key=lambda x: x['LastModified'])['Key']

    # Load the latest file
    s3_path = f"s3://{bucket_name}/{latest_file}"
    df_s3 = wr.s3.read_json(
        s3_path,
        compression="gzip",
        lines=True,
    )
else:
    print("No files found in the bucket.")

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
