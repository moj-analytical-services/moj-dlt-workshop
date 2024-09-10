import os
import json
import csv
import random
import datetime
from pathlib import Path
import typer
import re

app = typer.Typer(help=("Generate csv/json data"))


# Mock StructureColNames class as per your schemas
class StructureColNames:
    TENTHOUSANDSGROUP = "ten_thousands_group"
    THOUSANDSGROUP = "thousands_group"
    HUNDREDSGROUP = "hundreds_group"
    PRIMARYFIELD = "primary_field"
    SECONDARYFIELD = "secondary_field"
    REPORTTYPE = "report_type"
    FILENAME = "file_name"
    FILETYPE = "file_type"
    FILESIZE = "file_size"
    MODIFIED = "modified"
    ISARCHIVED = "is_archived"
    DATECREATED = "date_created"
    INVALIDFIELD = "invalid_field"


# Function to generate a single row of data
def generate_row(is_bad_row: bool = False):
    # Simulate a bad row
    if is_bad_row:
        # Generate a row with bad or missing data
        return {
            StructureColNames.TENTHOUSANDSGROUP: (
                None if random.random() < 0.2 else f"group_{random.randint(1, 10)}"
            ),
            StructureColNames.THOUSANDSGROUP: random.choice(
                [f"group_{random.randint(1, 100)}", None, 123]
            ),
            StructureColNames.HUNDREDSGROUP: random.choice(
                [f"group_{random.randint(1, 1000)}", "INVALID"]
            ),
            StructureColNames.PRIMARYFIELD: (
                None if random.random() < 0.1 else f"field_{random.randint(1, 100)}"
            ),
            StructureColNames.SECONDARYFIELD: random.choice(
                [f"field_{random.randint(1, 100)}", 999]
            ),
            StructureColNames.REPORTTYPE: f"type_{random.choice(['A', 'B', 'C'])}",
            StructureColNames.FILENAME: (
                f"file_{random.randint(1, 10)}.txt"
                if random.random() < 0.3
                else f"file_{random.randint(1, 10000)}.txt"
            ),
            StructureColNames.FILETYPE: random.choice(
                [".txt", ".csv", ".json", "unknown"]
            ),
            StructureColNames.FILESIZE: random.choice(
                [random.randint(1000, 100000), "large", None]
            ),
            StructureColNames.MODIFIED: f"{datetime.datetime.now() - datetime.timedelta(days=random.randint(10, 1000))}",
            StructureColNames.ISARCHIVED: random.choice([True, False, "unknown"]),
            StructureColNames.DATECREATED: random.choice(
                [
                    (
                        datetime.datetime.now()
                        - datetime.timedelta(days=random.randint(1, 3650))
                    ).strftime("%Y-%m-%d"),
                    (
                        datetime.datetime.now()
                        - datetime.timedelta(days=random.randint(1, 3650))
                    ).strftime("%d-%m-%Y"),
                    None,
                ]
            ),
            StructureColNames.INVALIDFIELD: random.choice([None, "NaN", "InvalidData"]),
        }
    else:
        # Generate a row with valid data
        return {
            StructureColNames.TENTHOUSANDSGROUP: f"group_{random.randint(1, 10)}",
            StructureColNames.THOUSANDSGROUP: f"group_{random.randint(1, 100)}",
            StructureColNames.HUNDREDSGROUP: f"group_{random.randint(1, 1000)}",
            StructureColNames.PRIMARYFIELD: f"field_{random.randint(1, 100)}",
            StructureColNames.SECONDARYFIELD: f"field_{random.randint(1, 100)}",
            StructureColNames.REPORTTYPE: f"type_{random.choice(['A', 'B', 'C'])}",
            StructureColNames.FILENAME: f"file_{random.randint(1, 10000)}.txt",
            StructureColNames.FILETYPE: ".txt",
            StructureColNames.FILESIZE: random.randint(1000, 100000),
            StructureColNames.MODIFIED: f"{datetime.datetime.now() - datetime.timedelta(days=random.randint(10, 1000))}",
            StructureColNames.ISARCHIVED: random.choice([True, False]),
            StructureColNames.DATECREATED: (
                datetime.datetime.now()
                - datetime.timedelta(days=random.randint(1, 3650))
            ).strftime("%Y-%m-%d"),
            StructureColNames.INVALIDFIELD: None,
        }


def generate_good_dict_data(num_rows: int = 10000):
    return [generate_row() for _ in range(num_rows)]


def generate_bad_dict_data(num_rows: int = 10000):
    if num_rows < 11:
        raise "num_rows must be greater than 10"
    data = [generate_row() for _ in range(num_rows - 10)]
    for _ in range(10):
        data.append(generate_row(is_bad_row=True))
    return data


def trim_data(data, file_index, number_of_rows):
    start_data = file_index * number_of_rows
    end_data = start_data + number_of_rows
    data_for_file = data[start_data:end_data]
    return data_for_file


# Function to generate a JSONL file with synthetic data
def generate_jsonl_file(file_index, data, output_dir, number_of_rows):
    output_path = Path(output_dir) / f"synthetic_file_{file_index}.jsonl"
    data_for_file = trim_data(data, file_index, number_of_rows)
    # Write the JSON Lines data to a file
    with open(output_path, "w") as f:
        for row in data_for_file:
            f.write(json.dumps(row) + "\n")

    typer.echo(f"Generated: {output_path}")


def generate_csv_file(file_index, data, output_dir, number_of_rows):
    output_path = Path(output_dir) / f"synthetic_file_{file_index}.csv"
    data_for_file = trim_data(data, file_index, number_of_rows)
    keys = data[0].keys()
    with open(output_path, "w", newline="") as f:
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data_for_file)
    typer.echo(f"Generated: {output_path}")


def read_latest_file_index(output_dir):
    file_number = None
    pattern = re.compile(r"synthetic_file_(\d+)$")
    directory_path = Path(output_dir)
    for file in directory_path.iterdir():
        if file.is_file():
            file_name = file.stem
            reg_match = pattern.search(file_name)
            if reg_match:
                new_file_number = int(reg_match.group(1))
                if file_number is None or new_file_number > file_number:
                    file_number = new_file_number
    return file_number


@app.command()
def generate_data(
    file_type: str = typer.Option("jsonl", help="File type to generate"),
    num_files: int = typer.Option(1, help="number of files to generate"),
    rows_per_file: int = typer.Option(1000, help="number of rows to generate per file"),
    output_dir: str = typer.Option("raw_data", help="output directory to write to"),
    bad_data: bool = typer.Option(
        False, "--bad-data", help="whether to create bad data or not"
    ),
    new_data: bool = typer.Option(
        False, "--new-data", help="Whether to generate more data"
    ),
):
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    if new_data:
        largest_index = read_latest_file_index(output_dir) + 1
    else:
        largest_index = 0

    total_rows = rows_per_file * num_files

    if bad_data:
        data = generate_bad_dict_data(total_rows)
    else:
        data = generate_good_dict_data(total_rows)

    for i in range(num_files):
        j = i + largest_index
        if file_type == "csv":
            generate_csv_file(j, data, output_dir, rows_per_file)
        else:
            generate_jsonl_file(j, data, output_dir, rows_per_file)

    typer.echo("Synthetic data generation complete.")


if __name__ == "__main__":
    typer.run(generate_data)
