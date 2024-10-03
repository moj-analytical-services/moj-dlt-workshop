# Loading to different destinations
One of the many benefits of `dlt` is the ease with which data can be loaded to different destinations. We want to test that the loaded data across destinations is identical. In this instance we are comparing data which is loaded directly to S3, loaded directly to a standard Athena table and finally data loaded to an Athena Iceberg table with partitioning.

## Setup
Ensure you've SSO'd into the Sandbox account. Running the following creates dummy data and runs 3 pipelines to load the data to S3, Athena and Athena Iceberg with partitioning.

```bash
python3 evaluation/destinations/destinations.py
``` 

## Check loaded data
Once the data has been loaded to all 3 locations we can compare the tables. You may need to update the query dump bucket for this to work.
```bash
python3 evaluation/destinations/check_tables.py
```
This loads each of the loaded tables, prints a sample extract from each of them and looks at the Table DDL for the Athena tables to ensure that they're correctly defined and that the Iceberg table is partitioned correctly.

One difference to flag is that when tables are loaded using `pydbtools` and `awswrangler`, the datatypes for each column are slightly different. We cast all columns to strings to compare only the values of each table. Additionally, we remove the dlt metadata columns as the values here will be unique to each load.

We then check if the loaded tables are equal. The output tells us that the data loaded to each destination is identical.

## To test
```bash
aws-vault exec {sandbox account}
python3 evaluation/destinations/destinations.py
python3 evaluation/destinations/check_tables.py
```
