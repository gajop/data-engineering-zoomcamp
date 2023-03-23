from json import dumps
from pathlib import Path
from kafka import KafkaProducer
from time import sleep
import pandas as pd


"""
Run with:

python producer_taxi_json.py -d data/fhv_tripdata_2019-01.parquet -t "fhv_taxi"
python producer_taxi_json.py -d data/green_tripdata_2019-01.parquet -t "green_taxi"
"""


def run(topic: str, dataset: Path) -> None:
    print(f"Starting kafka producer with topic:{topic} and dataset:{dataset}")
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        key_serializer=lambda x: dumps(x).encode("utf-8"),
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )

    # TODO: Should stream but I'm lazy
    df = pd.read_parquet(dataset)
    for row in df.to_dict(orient="records"):
        print(row)
        if topic == "fhv_taxi":
            key = {"dispatching_base_num": row["dispatching_base_num"]}
            value = {
                "dispatching_base_num": row["dispatching_base_num"],
                "pickup_datetime": row["pickup_datetime"],
                "dropOff_datetime": row["dropOff_datetime"],
                "PUlocationID": row["PULocationID"],
                "DOlocationID": row["DOLocationID"],
                "SR_Flag": row["SR_Flag"],
                "Affiliated_base_number": row["Affiliated_base_number"],
            }
        else:
            key = {"vendorId": row["VendorID"]}
            value = {
                "VendorID": row["VendorID"],
                "lpep_pickup_datetime": row["lpep_pickup_datetime"],
                "lpep_dropoff_datetime": row["lpep_dropoff_datetime"],
                "store_and_fwd_flag": row["store_and_fwd_flag"],
                "RatecodeID": row["RatecodeID"],
                "PULocationID": row["PULocationID"],
                "DOLocationID": row["DOLocationID"],
                "passenger_count": row["passenger_count"],
                "trip_distance": row["trip_distance"],
                "fare_amount": row["fare_amount"],
                "extra": row["extra"],
                "mta_tax": row["mta_tax"],
                "tip_amount": row["tip_amount"],
                "tolls_amount": row["tolls_amount"],
                "ehail_fee": row["ehail_fee"],
                "improvement_surcharge": row["improvement_surcharge"],
                "total_amount": row["total_amount"],
                "payment_type": row["payment_type"],
                "trip_type": row["trip_type"],
                "congestion_surcharge": row["congestion_surcharge"],
            }
        producer.send(topic, value=value, key=key)
        sleep(1)


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
    parser.add_argument(
        "-topic", "--topic", dest="topic", help="Kafka topic", required=True
    )

    args = parser.parse_args()

    run(args.topic, args.dataset)
