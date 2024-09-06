import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--project",
    type=str,
    required=True,
    default="storage-prod-olvin-com",
    help=(
        "Project to read BQ table from"
    ),
)
parser.add_argument(
    "--input_table",
    type=str,
    required=True,
    help=(
        "Name of BQ table"
        "Format: 'dataset.table"
    ),
)
parser.add_argument(
    "--output_folder",
    type=str,
    required=True,
    help=(
        "Google Cloud Storage folder to write output data to. "
        "Format: 'gs://{bucket}/{path}/'"
    ),
)
parser.add_argument(
    "--partition_column",
    type=str,
    required=True,
    help=(""),
)
parser.add_argument(
    "--num_files_per_partition",
    type=str,
    required=True,
    help=(""),
)
parser.add_argument(
    "--append_mode",
    type=str,
    required=True,
    help=(""),
)
parser.add_argument(
    "--date_start",
    type=str,
    required=True,
    help=(""),
)
parser.add_argument(
    "--date_end",
    type=str,
    required=True,
    help=(""),
)
args = parser.parse_args()

print("Running inference data prep job with args:")
for arg in vars(args):
    print(f"- {arg}={getattr(args, arg)!r}")