import asyncio
import json
import requests

from fake_logs.fake_logs    import FakeLogs
from fake_logs.line_pattern import LinePattern

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
TOPIC_NAME = "logs.http.apache"
CONNECTOR_NAME = "logs.http.apache"
FILENAME = "apache"

def configure_connector():
    """Calls Kafka Connect to create the Connector"""
    print("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    #
    # TODO: Complete the Kafka Connect Config below.
    #       See: https://docs.confluent.io/current/connect/references/restapi.html
    #       See: https://docs.confluent.io/current/connect/filestream_connector.html#filesource-connector
    #
    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "FileStreamSource",
                    "topic": TOPIC_NAME,  # TODO
                    "tasks.max": 1,  # TODO
                    "file": f"/logs/{FILENAME}.log",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter.schemas.enable": "false",
                },
            }
        ),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    print("connector created successfully")


async def log():
    filename = f"/tmp/{FILENAME}.log"
    num_lines = 500
    sleep = 5
    file_format = "apache"

    line_pattern = LinePattern(None, date_pattern=None, file_format=file_format, fake_tokens=None)
    FakeLogs(
        filename=filename,
        num_lines=num_lines,
        sleep=sleep,
        line_pattern=line_pattern,
        file_format=file_format
    ).run()


async def log_task():
    """Runs the log task"""
    task = asyncio.create_task(log())
    configure_connector()
    await task


def run():
    """Runs the simulation"""
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    run()
