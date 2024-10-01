import dlt
from dlt.common.typing import TDataItem


@dlt.resource(write_disposition="append")
def load_data_from_source() -> TDataItem:
    # Sample JSON data from an API or file
    data = [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25},
    ]

    # Here the contract schema ensures the fields have the right types
    for record in data:
        yield record


# Export inferred schema contract on the resource data
data_pipeline = dlt.pipeline(
    destination=dlt.destinations.filesystem(
        bucket_url="./evaluation/enforce_contract/data"
    ),
    dataset_name="customer_data",  # Dataset name in the destination
    export_schema_path="./evaluation/enforce_contract/schemas/export",
)


# Sample wrong JSON data from an API or file
@dlt.resource(write_disposition="append")
def load_incorrect_data_from_source() -> TDataItem:
    data = [
        {"id": 2, "name": "Jane Smith", "email": 1234, "age": "25"},
    ]
    for record in data:
        yield record


# Use the validation-aware resource in the pipeline
data_pipeline_with_schema = dlt.pipeline(
    destination=dlt.destinations.filesystem(
        bucket_url="./evaluation/enforce_contract/data"
    ),
    dataset_name="customer_data",
    import_schema_path="./evaluation/enforce_contract/schemas/import",
)


if __name__ == "__main__":
    first_load_info = data_pipeline.run(load_data_from_source())
    print(first_load_info)
    second_load_info = data_pipeline_with_schema.run(
        load_incorrect_data_from_source(), schema_contract={"tables": "freeze"}
    )
