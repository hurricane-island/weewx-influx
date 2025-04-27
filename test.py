"""Test script for InfluxDB integration with Weewx"""
from time import time
from os import getenv
from bin.user.influx import Queue, weewx, InfluxThread

weewx.debug = 2

queue = Queue()
t = InfluxThread(
    queue,
    database="test",
    api_token=getenv("API_TOKEN"),
    measurement="observations",
    server_url=getenv("SERVER_URL"),
)

record = {
    "dateTime": int(time()),
    "usUnits": weewx.US,
    "outTemp": 33.5,
    "inTemp": 75.8,
    "outHumidity": 24,
}

queue.put(record)
queue.put(None)
t.run()
