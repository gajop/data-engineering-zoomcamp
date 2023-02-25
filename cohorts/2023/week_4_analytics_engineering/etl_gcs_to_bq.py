from pathlib import Path
from typing import List
import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

from dotenv import load_dotenv

load_dotenv()


@flow(log_prints=True)
def etl_gcs_to_bq(months: List[int], year: int, color: str, dest_table: str):
    """Main ETL flow to load data into Big Query"""
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        write_bq(df, dest_table)


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    # Already saved it locally.. let's optimize
    # gcs_block = GcsBucket.load("zoom-gcs")
    # gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame, dest_table: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    # yellow only?
    # if "tpep_pickup_datetime" in df.columns:
    #     df = df.rename(
    #         columns={
    #             "tpep_pickup_datetime": "lpep_pickup_datetime",
    #             "tpep_dropoff_datetime": "lpep_dropoff_datetime",
    #         }
    #     )

    df.to_gbq(
        destination_table=dest_table,
        project_id=os.environ["GCP_PROJECT_ID"],
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


if __name__ == "__main__":
    months = list(range(1, 13))
    for (table, color) in zip(["dezoomcamp.green_tripdata", "dezoomcamp.yellow_tripdata"], ["green", "yellow"]):
        print(table, color)
        for year in [2019, 2020]:
            etl_gcs_to_bq(color=color, year=year, months=months,    dest_table=table)

    etl_gcs_to_bq(color="fhv", year=2019, months=months, dest_table="dezoomcamp.fhv_tripdata")
