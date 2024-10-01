# Enforcing a data contract

Currently, we enforce data contracts by trying to cast data to mojap metadata. Then we move data to a pass/fail bucket appropriately. 

Using `dlt`, we can replicate this behaviour using multiple pipelines.

## Setup

First thing, we use some fake json data and run a standard pipeline first. This is effectively only to export a dlt schema (using the `export_schema_path` option) so we can enforce it. You could, of course write this schema yourself, and we can most likely quite easily write a schema adapter for `mojap_metadata`.

```python
# Export inferred schema contract on the resource data
data_pipeline = dlt.pipeline(
    destination=dlt.destinations.filesystem(
        bucket_url="./evaluation/enforce_contract/data"
    ),
    dataset_name="customer_data",  # Dataset name in the destination
    export_schema_path="./evaluation/enforce_contract/schemas/export",
)
```

This exports a schema to `/evaluation/enforce_contract/schemas/export/*.yaml` locally.

## Reading a contract

Of course, this is only explicitly reading the schema contract, if you are inferring the schema on your first ingest enforcing a contract would work in the exact same way. The only reason for doing this is to write out the schema so we can see it.

There are a few options in [schema contract](https://dlthub.com/docs/general-usage/schema-contracts). You can 'freeze' (raise exceptions on), 'evolve' (allow any and all changes), 'discard_row' or 'discard_value' on tables, columns, or data types. The explanations in the linked docs are useful. We are just going to look at freezing a table that goes wrong (using this data: `{"id": 2, "name": "Jane Smith", "email": 1234, "age": "25"},`):

```python
pipeline.run(data, schema_contract={"tables": "freeze"})
```

This breaks the pipeline and it will not load into the destination.

## Replicating pass/fail

If we want to replicate the functionality commonly used at MoJ, we can, if we wrap a contract enforced pipeline in a try and except block and add another pipeline in the except block:
```python
try:
    load_info = pipeline.run(data, schema_contract={"tables": "freeze"})
except (DataValidationError, PipelineStepFailed) as e:
    print(f"{e}: {pipeline.pipeline_name} failed, writing to fail destination.")
    load_info = pipeline.run(data, destination=fail_destination)
```

This will try to load the pipeline into the correct destination, but if the table is different, it will load it to a fail destination.