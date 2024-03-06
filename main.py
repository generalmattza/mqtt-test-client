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


def setup_logging(logger_config):
    from pathlib import Path

    Path.mkdir(Path("logs"), exist_ok=True)
    return dictConfig(logger_config)


def start_prometheus_server(port=8000):
    from prometheus_client import start_http_server

    start_http_server(port)


def publish_forever():
    client = MQTTNode(broker_config=broker_config, node_id="node_0").connect()

    while True:
        data = random.random()
        payload = json.dumps(data)
        client.publish(topic="pzero/sensorbox_lower/temperature/IR/0", payload=payload)
        time.sleep(1)


def subscribe_forever():
    bucket = "testing"
    config_file = "config/influx_test.toml"
    database_client = FastInfluxDBClient.from_config_file(config_file=config_file)
    database_client.start()

    client = (
        MQTTMetricsGatherer(
            broker_config=broker_config,
            node_id="client_0",
            buffer=database_client.buffer,
        )
        .connect()
        .loop_start()
    )
    # client.subscribe(topic="node_0/metrics", qos=0)
    client.subscribe(topic="pzero/#", qos=0)


if __name__ == "__main__":
    setup_logging(logger_config)

    if config["mqtt"]["node_network"]["enable_prometheus_server"]:
        start_prometheus_server(config["mqtt"]["node_network"]["prometheus_port"])

    threading.Thread(target=publish_forever).start()

    subscribe_forever()
