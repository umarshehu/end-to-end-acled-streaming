
**This repository demonstrates a data engineering pipeline using Spark Structured Streaming. It retrieves Armed Conflict Location & Event Data (ACLED) from an API, sends the data to Kafka topics via Airflow, and processes it with Spark Structured Streaming before storing it in PostgreSQL.**

# System Architecture

![alt text](img/architecture.jpg)

## Components:

**Data Source:** Uses the ACLED API for crime data. \
**Apache Airflow:** Orchestrates the pipeline and schedules data ingestion. \
**Apache Kafka & Zookeeper:** Stream data from PostgreSQL to Spark. \
**Apache Spark:** Processes data in real time. \
**PostgreSQL:** Stores the processed data. \
**Scripts:**

**acled_to_kafka.py:** Airflow DAG script that pushes API data to Kafka during 2 minutes every 1 seconds. \
**spark_streaming.py:** Consumes and processes data from Kafka using Spark Structured Streaming. 

## What You'll Learn:

Setting up and orchestrating pipelines with Apache Airflow. \
Real-time data streaming with Apache Kafka. \
Synchronization with Apache Zookeeper. \
Data processing with Apache Spark. \
Storage solutions with PostgreSQL. \
Containerization of the entire setup using Docker. \
**Technologies:** \
Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, PostgreSQL, Docker 

## Getting Started

### WebUI links

`Airflow`  : <http://localhost:8080/> \
`Kafka UI` : <http://localhost:8085/> \

### Clone the repository:

`$ git clone https://github.com/umarshehu/end-to-end-acled-streaming.git`

### Navigate to the project directory:

`$ cd end-to-end-acled-streaming`

### Rename the "env file" into .env in project folder and set an AIRFLOW_UID

`$ echo -e "AIRFLOW_UID=$(id -u)" > .env`

`$ echo AIRFLOW_UID=50000 >> .env`


### Run Docker Compose to perform database migrations and create the first user account

`$ docker-compose up airflow-init`

![alt text](img/airflow-init.png)


### Run Docker Compose again to spin up the services:

`$ docker compose up -d`

![alt text](img/compose-up-d.png)


### Extract dependencies.7z and create a dependencies.zip file, then copy the dependencies.zip and spark_streaming.py files into spark-master container

`$ docker cp dependencies.zip spark-master:/dependencies.zip`

`$ docker cp spark_stream.py spark-master:/spark_streaming.py`

![alt text](img/docker-cp.png)

### Unpause the dag user_automation using Airflow UI

**Go to Airflow UI using :** <http://localhost:8080/>

**Login using** Username: `admin` Password: `admin`

![unpause the acled_kafka task]

**You can track the topic creation and message queue using the open source tool named UI for Apache Kafka that is running as a container, WebUI link:**  <http://localhost:8085/>

![alt text](img/kafkaui.png)

### In a new terminal run the docker exec command to run spark job to read the streaming from kafka topic:

`$ docker exec -it spark-master spark-submit     --packages org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1     --py-files /dependencies.zip     /spark_streaming.py`


### Now go to the PostgreSQL docker shell terminal back and run the command to see data is inserted to the incident table

`psql -U airflow -d airflow;`

#### and run count query several times to approve data is being inserted while running acled_kafka dag

`airflow=> SELECT COUNT(*) FROM incident;`
