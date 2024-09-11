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

## Step Two: Set up your dlt pipeline

You should now have some data set up that you can access. The next step is a bit dependent on what source you have, but effectively running:

```bash
dlt init {source_name} {destination_name}
```
should work. This will pull from dlt's verified sources repo and clone the code relevant to your source in a folder called `source_name` and then give you some example code to define and run a dlt pipeline. 

In this case, our destination is going to be `filesystem` (for now locally) as we are writing to parquet format.

I will from here assume if your source is `filesystem`:

Look at the file `filesystem/readers.py`, this nicely defines some neat functions to read csvs and json files: we can utilise these.

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
from .filesystem import readers, filesystem

@dlt.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def read_json_from_local_filesystem(
    table_name: str,
    folder_name: str,
    file_name: str
    ):
    yield filesystem(
            bucket_url=folder_name,
            file_glob=file_name
        ) | dlt.transformer(name=table_name)(_read_json)
```

All we've done here is create a dlt source where you can define the name for your table, where the folder is locally, and what the name of the file in that folder you want to read is.

Using this source we can easily set up a dlt pipeline.



## Step Three: Run your dlt pipeline (and again!)


## Step Four: Evaluate your dlt pipeline


## Step Five: Replicate all of this using moj-dlt and a yaml file