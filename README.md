# dlt Workshop

# What is our aim?

Generally at MoJ in data engineering, we run our pipelines from a landing destination to a raw history location and then from there into Athena.

Today we will try to replicate that by first pulling our data out into parquet format from "landing" and then into duckdb format locally. 

If we have time we can pull this into AWS in the data engineering sandbox account to fully replicate (if you don't have permissions pair up with a data engineer).

## Step Zero: Boring Python Environment Setup

Boring boring poetry boring boring venv etc...

TLDR: Run the following...

```bash
poetry install
poetry run python3 python_apps/__init__.py
```

or to create an explicit venv:
```bash
poetry shell
python3 python_apps/__init__.py
```

and you should see a message saying you've set it up correctly.

## Step One: Generate Raw Data

This is the easiest step. If you have brought raw data with you - great. If it is a number of files add those into this repo in a folder. Remember your source is called `filesystem`.

If it is an API - you have nothing to do other than that your source is called `api`.

If it is a SQL Database, ensure that it is up and running and that your source is called `sql_database`.

If you have no data fear not, I have made a quick data generator for you - run the following (either prefixed by `poetry run` or not depending on your explicit venv choice above):

```bash
python3 python_apps/data_generator.py --help
```

you'll see a number of options and descriptions, but for now just run:

```bash
python3 python_apps/data_generator.py
```

This will generate a folder `raw_data` with a file `synthetic_data_0.jsonl` inside it with some meaningless data.

Note that your source is also called `filesystem`.

## Step Two: Set up your dlt source

You should now have some data set up that you can access. The next step is a bit dependent on what source you have, but effectively running:

```bash
dlt init {source_name} {destination_name}
```
should work. This will pull from dlt's verified sources repo and clone the code relevant to your source in a folder called `source_name` and then give you some example code to define and run a dlt pipeline. 

In this case, our destination is going to be `filesystem` (for now locally) as we are writing to parquet format.

I will from here assume your source is `filesystem`.

Look at the file `filesystem/readers.py`. This nicely defines some neat functions to read csv and json files for us to utilise.

```python
def _read_jsonl(
    items: Iterator[FileItemDict], chunksize: int = 1000
) -> Iterator[TDataItems]:
    """Reads jsonl file content and extract the data.

    Args:
        chunksize (int, optional): The number of JSON lines to load and yield at once, defaults to 1000

    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        with file_obj.open() as f:
            lines_chunk = []
            for line in f:
                lines_chunk.append(json.loadb(line))
                if len(lines_chunk) >= chunksize:
                    yield lines_chunk
                    lines_chunk = []
        if lines_chunk:
            yield lines_chunk
```


In `filesystem/init.py` we define a `dlt.resource` called `filesystem`, this is useful as well - this lists the files in our file system using glob.

```python
@dlt.resource(
    primary_key="file_url", spec=FilesystemConfigurationResource, standalone=True
)
def filesystem(
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: Optional[str] = "*",
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
) -> Iterator[List[FileItem]]:
    """This resource lists files in `bucket_url` using `file_glob` pattern. The files are yielded as FileItem which also
    provide methods to open and read file data. It should be combined with transformers that further process (ie. load files)

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials | AbstractFilesystem): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str, optional): The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
        files_per_page (int, optional): The number of files to process at once, defaults to 100.
        extract_content (bool, optional): If true, the content of the file will be extracted if
            false it will return a fsspec file, defaults to False.

    Returns:
        Iterator[List[FileItem]]: The list of files.
    """
    if isinstance(credentials, AbstractFileSystem):
        fs_client = credentials
    else:
        fs_client = fsspec_filesystem(bucket_url, credentials)[0]

    files_chunk: List[FileItem] = []
    for file_model in glob_files(fs_client, bucket_url, file_glob):
        file_dict = FileItemDict(file_model, credentials)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)  # type: ignore

        # wait for the chunk to be full
        if len(files_chunk) >= files_per_page:
            yield files_chunk
            files_chunk = []
    if files_chunk:
        yield files_chunk
```

The `readers` function in here is a `dlt.source` (i.e. a group of resources) that use `dlt.transformer` to **actually** read the data into memory.
```python        
    filesystem(bucket_url, credentials, file_glob=file_glob)
    | dlt.transformer(name="read_jsonl")(_read_jsonl),
```

Let us stick that all together to define a resource that just read our local json files into memory using the `filesystem` and `_read_json` functions from above:

```python
from filesystem import filesystem
from filesystem.readers import _read_jsonl
import dlt
import dlt.destinations as dlt_destinations
import logging

# Create a logger
logger = logging.getLogger("dlt")

# Set the log level
logger.setLevel(logging.INFO)
@dlt.source
def read_json_from_local_filesystem(
    table_name: str,
    folder_name: str,
    file_name: str
    ):
    yield filesystem(
            bucket_url=folder_name,
            file_glob=file_name
        ) | dlt.transformer(name=table_name)(_read_jsonl)
```

All we've done here is create a dlt source where you can define the name for your table, where the folder is locally, and what the name of the file in that folder you want to read is. Of course, we could set up everything more generally (a bit like the readers function is set up), but this is an example to hopefully show how the functionality of dlt works.

Using this source we can easily set up a dlt pipeline.

## Step Three: Set up your dlt pipeline

Setting up a dlt pipeline is relatively straightforward, using the `dlt.pipeline` functionality.

You need to define a few things including the:
- name of the pipeline
- destination
- dataset name (optionally is the same as the pipeline name)
- optional arguments

For example to set up writing to a local raw history folder you would need to set up a destination `filesystem`:

```python
destination_fs = filesystem(bucket_url="raw_history/")
```
then we can create our pipeline (notice we just define the destination, not the source here):

```python
example_pipeline = dlt.pipeline(
    pipeline_name="test_pipeline",
    dataset_name="synthetic_nonsense_data",
    destination=destination_fs
)
```

## Step Four: Run your dlt pipeline

To run a dlt pipeline you now need to define your data you are running the pipeline with and then use the run function to run it. For example for the pipeline above:

```python
example_pipeline.run(
    read_json_from_local_filesystem(
        "synthetic_data",
        "raw_data",
        "*.jsonl"
    )
)
```

This will just move files from a to b (a bit pointless!), remember we wanted to change the files to our preferred file format (parquet), so let's define that in our run:

```python
example_pipeline.run(
    read_json_from_local_filesystem(
        your_table_name,
        your_folder_name,
        your_file_name
    ),
    loader_file_format="parquet"
)
```

Now if you run in bash,

```bash
python3 main.py
```

You should see the running of a dlt pipeline, and then the output of parquet in another folder.

## Step Five: Create a second pipeline with a  DuckDB Output

Now we want to output in a SQL format, so let us create a second pipeline to a duckdb output.

This is going to mostly be the same as before, except we're going to edit our filesystem to include reading of our (newly written) parquet files.

```python
from .filesystem import _read_jsonl, _read_parquet, filesystem

@dlt.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def read_json_parquet_from_local_filesystem(
    table_name: str,
    folder_name: str,
    file_name: str
    ):
    yield (
        filesystem(
            bucket_url=folder_name,
            file_glob=file_name
        ) | dlt.transformer(name=table_name)(_read_jsonl),
        filesystem(
            bucket_url=folder_name,
            file_glob=file_name
        ) | dlt.transformer(name=table_name)(_read_parquet)
)
```

and now a new example pipeline:

```python
example_pipeline_2 = dlt.pipeline(
    pipeline_name="test_pipeline_2",
    dataset_name="synthetic_nonsense_duckdb_data",
    destination=duckdb(path="test_data.duckdb")
)

example_pipeline_2.run(
    read_json_parquet_from_local_filesystem(
        your_table_name,
        your_parquet_folder_name,
        your_parquet_file_name
    ),
)
```
again running:
```bash
python3 main.py
```
Should output a duckdb file locally.

## Step Six: Incremental loading

Now, what happens if you get more data in the same pipeline?

If you run the generate data app again, with a new flag to generate another file:

```bash
python3 python_apps/data_generator.py --new-data
```

This will generate a new file of data for you, if you then run the dlt pipeline again, and check how many rows it loads:

```bash
python3 main.py
```
you should see that it loads both files to both the filesystem and the duckdb location again.

Obviously, this is not ideal behaviour. We want to utilise dlt's interpretation of incremental loading.

We're gonna need this twice, once for the first pipeline and once for the second.

To do this, we will need to tweak our `dlt.source` to allow us to read the file's metadata:

```python
from .filesystem import _read_jsonl, _read_parquet, filesystem

@dlt.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def read_json_parquet_from_local_filesystem(
    table_name: str,
    folder_name: str,
    file_name: str,
    incremental_load: str = None,
    ):
    fs = filesystem(
            bucket_url=folder_name,
            file_glob=file_name
        )
    fs.apply_hints(
        incremental=dlt.sources.incremental(incremental_load)
    )
    yield (
        fs | dlt.transformer(name=table_name)(_read_jsonl),
        fs | dlt.transformer(name=table_name)(_read_parquet)
)
```
where we've added an argument for how we're going to incremental load.

So, for the first pipeline, let us add an incremental load based on modified date of the file (this is dlt's custom field regardless of what type of filesystem you are in):

```python
example_pipeline.run(
    read_json_from_local_filesystem(
        your_table_name,
        your_folder_name,
        your_file_name,
        incremental_load="modified_date"
    )
)
```

for the second pipeline, we need to run it based on a custom column that dlt adds, `_dlt_load_id`.

To do that:

```python
parquet_source = read_json_parquet_from_local_filesystem(
        your_table_name,
        your_parquet_folder_name,
        your_parquet_file_name
    )
for source in parquet_source.resources:
    run_source.resources[source].apply_hints(
        incremental=dlt.sources.incremental("_dlt_load_id")
        )

example_pipeline_2.run(run_source)
```

then re-run the data generation and the pipeline
```bash
python3 python_apps/data_generator.py --new-data
python3 main.py
```
and it should only pull through the final 1000 rows of data into the filesystem.

## Step Seven: Utilise this with AWS

Now, it would be great to utilise dlt to read and write not to duckdb, but to Athena, as that is what we use at MOJ.

If you have permissions to the AWS Data Engineering Sandbox account, store your creds in your local environment via ,for example, aws vault:
```bash
aws-vault exec your-profile-name
```
then, we just need to edit our destinations for our pipelines:

```python
destination_fs = dlt_destinations.filesystem(bucket_url="s3://gw-dlt/{your_name}/raw_history/")
s```

## Step Eight: Replicate all of this using moj-dlt and a yaml file
