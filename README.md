# dlt workshop

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


## Step Two: Set up your pipeline