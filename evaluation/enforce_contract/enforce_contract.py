import dlt

from dlt.common.schema import Schema
from dlt.common.validation import validate_dict
from dlt.common.schema.typing import TDataItem


# Define a schema with contract enforcement
@dlt.resource(write_disposition="append")
def load_data_from_source() -> TDataItem:
    # Sample JSON data from an API or file
    data = [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25},
        {"id": 3, "name": "Mike Brown", "email": None, "age": 40},  # Invalid data
    ]

    # Here the contract schema ensures the fields have the right types
    for record in data:
        yield record


# Define a schema contract with data types and nullable enforcement
schema = Schema(name="customer_data")

# Add contract for schema fields
schema.add_table(
    "customers", {
        "id": {"type": "bigint", "nullable": False},  # Must have id
        "name": {"type": "string", "nullable": False},  # Must have a name
        "email": {"type": "string", "nullable": True},  # Email is optional
        "age": {"type": "integer", "nullable": False}  # Age must be present
    }
)


# Apply schema contract on the resource data
@dlt.pipeline(
    destination="duckdb",  # Load data to PostgreSQL
    dataset_name="customer_data",  # Dataset name in the destination
    schema=schema,  # Enforce the schema we defined
)
def data_pipeline():
    # Load data into the pipeline
    return load_data_from_source()


# Function to fix or handle invalid data based on schema contract
def validate_record(record):
    try:
        # Check if required fields are present
        validate_dict(record, schema)
        return record
    except Exception as e:
        print(f"Error in record {record}: {str(e)}")
        raise e


# Modify the data pipeline to validate records
@dlt.resource(write_disposition="append")
def load_data_with_validation() -> TDataItem:
    data = load_data_from_source()  # Fetch data

    for record in data:
        yield validate_record(record)


# Use the validation-aware resource in the pipeline
@dlt.pipeline(
    destination="duckdb",
    dataset_name="customer_data",
    schema=schema,
)
def validated_data_pipeline():
    return load_data_with_validation()
