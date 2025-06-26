# consumer.py
import os
import time
import json
import logging
import psycopg2
from kafka import KafkaConsumer, errors
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Consumer")

def init_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            dbname=os.getenv("POSTGRES_DB", "airflow")
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS incident (
            event_id_cnty TEXT PRIMARY KEY,
            event_date TEXT,
            year TEXT,
            time_precision TEXT,
            disorder_type TEXT,
            event_type TEXT,
            sub_event_type TEXT,
            actor1 TEXT,
            assoc_actor_1 TEXT,
            inter1 TEXT,
            actor2 TEXT,
            assoc_actor_2 TEXT,
            inter2 TEXT,
            interaction TEXT,
            civilian_targeting TEXT,
            iso TEXT,
            region TEXT,
            country TEXT,
            admin1 TEXT,
            admin2 TEXT,
            location TEXT,
            latitude TEXT,
            longitude TEXT,
            geo_precision TEXT,
            source TEXT,
            source_scale TEXT,
            notes TEXT,
            fatalities TEXT,
            tags TEXT,
            timestamp TEXT); 
        """)
        cur.close()
        return conn
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        raise

def insert_transaction(conn, tx):
    try:
        cur = conn.cursor()
        query = """
            INSERT INTO incident (
                event_id_cnty,event_date,year,time_precision,disorder_type,event_type,
                sub_event_type,actor1,assoc_actor_1,inter1,actor2,assoc_actor_2,inter2,
                interaction,civilian_targeting,iso,region,country,admin1,admin2,location,
                latitude,longitude,geo_precision,source,source_scale,notes,fatalities,
                tags,timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id_cnty) DO NOTHING;
        """
        cur.execute(query, (
            tx["event_id_cnty"],
            tx["event_date"],
            tx["year"],
            tx["time_precision"],
            tx["disorder_type"],
            tx["event_type"],
            tx["sub_event_type"],
            tx["actor1"],
            tx["assoc_actor_1"],
            tx["inter1"],
            tx["actor2"],
            tx["assoc_actor_2"],
            tx["inter2"],
            tx["interaction"],
            tx["civilian_targeting"],
            tx["iso"],
            tx["region"],
            tx["country"],
            tx["admin1"],
            tx["admin2"],
            tx["location"],
            tx["latitude"],
            tx["longitude"],
            tx["geo_precision"],
            tx["source"],
            tx["source_scale"],
            tx["notes"],
            tx["fatalities"],
            tx["tags"],
            tx["timestamp"]
        ))
        cur.close()
        conn.commit()
        logger.info(f"Inserted incident: {tx['event_id_cnty']}")
    except Exception as e:
        logger.error(f"Error inserting incident {tx['event_id_cnty']}: {e}")

def wait_for_kafka(bootstrap_servers, topic, retries=10, delay=5):
    """
    Wait until Kafka broker is available by attempting to create a temporary consumer.
    """
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap_servers],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="test_group",
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            consumer.close()
            logger.info("Kafka broker is available.")
            return
        except errors.NoBrokersAvailable:
            logger.warning(f"No Kafka brokers available, retrying in {delay} seconds... (Attempt {i+1}/{retries})")
            time.sleep(delay)
    raise Exception("Kafka broker not available after several retries.")

def main():
    # Initialize DB connection and table
    conn = None
    while not conn:
        try:
            conn = init_db()
            logger.info("Connected to PostgreSQL and table is ready.")
        except Exception as e:
            logger.error(f"DB connection failed, retrying in 5 seconds. Error: {e}")
            time.sleep(5)
    
    # Get Kafka configuration from environment
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    topic = os.getenv("TRANSACTION_TOPIC", "incidents")
    
    # Wait for Kafka broker to be available
    wait_for_kafka(kafka_bootstrap, topic)
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[kafka_bootstrap],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="incident_group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    logger.info("Kafka consumer is running. Listening on 'incidents_topic'...")
    
    for message in consumer:
        tx = message.value
        insert_transaction(conn, tx)

if __name__ == "__main__":
    main()
