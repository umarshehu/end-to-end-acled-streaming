import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import List, Union
import kafka.errors
import requests
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
import random

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

# Set up logging configuration
logging.basicConfig(level=logging.INFO)


@dataclass
class IncidentEvent:
    """
    Represents an Incident Event with relevant data.
    """

    # Inciden event fields
    event_id_cnty: str
    event_date: str
    year: int
    time_precision: int
    disorder_type: str
    event_type: str
    sub_event_type: str
    actor1: str
    assoc_actor_1: str
    inter1: str
    actor2: str
    assoc_actor_2: str
    inter2: str
    interaction: str
    civilian_targeting: str
    iso: int
    region: str
    country: str
    admin1: str
    admin2: str
    location: str
    latitude: float
    longitude: float
    geo_precision: int
    source: str
    source_scale: str
    notes: str
    fatalities: int
    tags: str
    timestamp: str


def create_kafka_producer() -> KafkaProducer:
    """
    Creates the Kafka producer object
    """
    try:
        producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
    except kafka.errors.NoBrokersAvailable:
        logging.info(
            "Running locally, we use localhost instead of kafka and the external port 9094"
        )
        producer = KafkaProducer(bootstrap_servers=["localhost:9094"])

    return producer


def extract_key_values(
    data: Union[dict, list], flattened_data: dict, parent_key: str = ""
):
    """
    Recursively extract key-value pairs from nested JSON data
    and flatten them while keeping the order of dimensions intact.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                new_key = f"{parent_key}_{key}" if parent_key else key
                extract_key_values(value, flattened_data, new_key)
            else:
                new_key = f"{parent_key}_{key}" if parent_key else key
                flattened_data[new_key] = value
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_key = f"{parent_key}_{i}" if parent_key else str(i)
            extract_key_values(item, flattened_data, new_key)


def query_incidents_api() -> dict:
    """
    Queries the ACLED API and returns 1000 rows of data as a dictionary.
    """
    base_url = (
        "https://api.acleddata.com/acled/read?key={enter api key here}&email={enter email here}&year=2020|2025&year_where=BETWEEN&limit=1000"
    )
    url = base_url
    logging.info(f"Query URL: {url}")
    response = requests.get(url)
    return response.json()


def query_data() -> List[dict]:
    """
    Queries Incident data from the ACLED API and processes it into a list of dictionaries.
    """
    result: dict = query_incidents_api()
    features: List[dict] = result["data"]
    
    data: List[IncidentEvent] = []

    for feature in features:
        
        incident_data: IncidentEvent = IncidentEvent(
            
            event_id_cnty=feature["event_id_cnty"],
            event_date=feature["event_date"],
            year=feature["year"],
            time_precision=feature["time_precision"],
            disorder_type=feature["disorder_type"],
            event_type=feature["event_type"],
            sub_event_type=feature["sub_event_type"],
            actor1=feature["actor1"],
            assoc_actor_1=feature["assoc_actor_1"],
            inter1=feature["inter1"],
            actor2=feature["actor2"],
            assoc_actor_2=feature["assoc_actor_2"],
            inter2=feature["inter2"],
            interaction=feature["interaction"],
            civilian_targeting=feature["civilian_targeting"],
            iso=feature["iso"],
            region=feature["region"],
            country=feature["country"],
            admin1=feature["admin1"],
            admin2=feature["admin2"],
            location=feature["location"],
            latitude=feature["latitude"],
            longitude=feature["longitude"],
            geo_precision=feature["geo_precision"],
            source=feature["source"],
            source_scale=feature["source_scale"],
            notes=feature["notes"],
            fatalities=feature["fatalities"],
            tags=feature["tags"],
            timestamp=feature["timestamp"]
        )

        data.append(incident_data)

    unique_ids = Counter()

    for event in data:
        unique_ids[event.event_id_cnty] += 1

    json_data = [vars(event) for event in data]

    logging.info(f"Unique IDs Count: {len(unique_ids)}")

    return json_data


def stream_data():
    producer = KafkaProducer(bootstrap_servers='broker:9092', max_block_ms=5000)
    producer = create_kafka_producer()
    results = query_data()

    for kafka_data in results:
        producer.send("incidents", json.dumps(kafka_data).encode("utf-8"))


with DAG('acled_kafka',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

