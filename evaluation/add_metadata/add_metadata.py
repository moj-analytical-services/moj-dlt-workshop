import dlt
import pandas as pd


# Define the dlt resource
@dlt.resource(
    table_name="names",
)
def get_data():
    df = pd.read_csv('test_data.csv')
    for rec in df.to_dict(orient='records'):
        yield rec


@dlt.transformer()
def add_tag(record):
    record['tag'] = 'v0.0.1'
    yield record


pipeline = dlt.pipeline(
    pipeline_name="another_pipeline",
    destination="duckdb",
    dataset_name="test_data",
)

load_info = pipeline.run(
    (get_data | add_tag),
    write_disposition="replace",
)