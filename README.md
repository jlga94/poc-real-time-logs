# Real-time Log Ingestion
This project consists in ingesting Apache logs from a Python script into Kafka, then transform the data using Spark Structured Streaming and finally ingesting the data into Elasticsearch using Streamsets to visualize the data using Kibana
Based on : [LINK](https://itnext.io/creating-a-real-time-flight-info-data-pipeline-with-streamsets-kafka-elasticsearch-and-kibana-dc40868c1021)

### Install requirements
* Install python libraries
```
pip install -r log_generator/requirements.txt
```
* Create .env file
```
Create a '.env' file with the same content as 'dummy.env', put the ip found in en0. You need to open a terminal a type 'ifconfig', look after en0 and copy&paste the value inet.
```
* Run the docker containers
```
docker-compose up
```
* Run the python kafka producer
* In one terminal run: 
```
python log_generator/fake-logs.py -n 500 -o myfile.log -s 5 -f apache
```
* In another terminal run: 
```
python log_generator/kafka_producer.py --topic http_log --broker <ip_en0_karka_broker>:9092
```
* Open Streamsets
* Install Apache Kafka 0.10 connector
* Install Elasticsearch 5.2.0 connector
* Restart the container
```
docker restart <id_container>
```
* Import the json file streamsets/kafka_to_kibana.json
* Change the ip for Kafka


