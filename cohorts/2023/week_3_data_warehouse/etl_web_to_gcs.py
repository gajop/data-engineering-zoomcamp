from pathlib import Path
import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@flow()
def etl_web_to_gcs(year=2020, month=1) -> None:
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz"
    print(dataset_url)

    # df = fetch(dataset_url)
    # path = write_local(df, dataset_file)
    # manually downloaded with wget because pandas's df.to_csv was slow
    path = Path(f"fhv_tripdata/{dataset_file}.csv.gz")
    write_gcs(path)


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    return pd.read_csv(dataset_url)


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    path = Path(f"fhv_tripdata/{dataset_file}.csv.gz")
    if not path.parent.exists():
        os.makedirs(path.parent)
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


if __name__ == "__main__":
    year = 2019
    for month in range(1, 13):
        etl_web_to_gcs(year=year, month=month)
