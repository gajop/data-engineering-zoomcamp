from pathlib import Path
import pandas as pd


"""
Run with:

python csv_to_parquet.py -d data/fhv_tripdata_2019-01.csv
python csv_to_parquet.py -d data/green_tripdata_2019-01.csv
"""


def convert(dataset: Path) -> None:
    print(f"Converting: {dataset} to parquet...")

    df = pd.read_csv(dataset)
    df = df.rename(
        columns={"PUlocationID": "PULocationID", "DOlocationID": "DOLocationID"}
    )
    df = df[df["PULocationID"].notna()]
    df = df[df["DOLocationID"].notna()]
    df["PULocationID"] = df["PULocationID"].astype(int)
    df["DOLocationID"] = df["DOLocationID"].astype(int)
    df.to_parquet(dataset.with_suffix(".parquet"))


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--dataset",
        dest="dataset",
        help="input dataset",
        type=Path,
        required=True,
    )

    args = parser.parse_args()

    convert(args.dataset)
