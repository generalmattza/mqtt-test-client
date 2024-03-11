#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2023-01-23
# version ='1.0'
# ---------------------------------------------------------------------------
"""a_short_project_description"""
# ---------------------------------------------------------------------------


import json
import threading
import time
import random
from logging.config import dictConfig

from mqtt_node_network.node import MQTTNode
from mqtt_node_network.metrics_gatherer import MQTTMetricsGatherer
from mqtt_node_network.configuration import broker_config, logger_config, config
from fast_database_clients import FastInfluxDBClient

SUBSCRIBE_TOPIC = config["mqtt"]["node_client"].get("subscribe_topic", "#")
NODE_ID = config["mqtt"]["node_client"].get("node_id", "mqtt-node-client")
INFLUX_CONFIG = config["mqtt"]["node_client"].get("influx_config")
QOS = config["mqtt"]["node_client"].get("qos", 0)

def setup_logging(logger_config):
    from pathlib import Path

    Path.mkdir(Path("logs"), exist_ok=True)
    return dictConfig(logger_config)


def start_prometheus_server(port=8000):
    from prometheus_client import start_http_server

    start_http_server(port)


def process_forever():
    config_file = INFLUX_CONFIG
    database_client = FastInfluxDBClient.from_config_file(config_file=config_file)
    database_client.start()

    client = (
        MQTTMetricsGatherer(
            broker_config=broker_config,
            node_id=NODE_ID,
            buffer=database_client.buffer,
        )
        .connect()
    )
    client.subscribe(topic=SUBSCRIBE_TOPIC, qos=QOS)


    while True:
        time.sleep(1)

if __name__ == "__main__":
    setup_logging(logger_config)

    if config["mqtt"]["node_network"]["enable_prometheus_server"]:
        start_prometheus_server(config["mqtt"]["node_network"]["prometheus_port"])

    process_forever()

