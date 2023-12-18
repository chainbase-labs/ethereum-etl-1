from datetime import datetime
import json
import logging
import os
import sys
import time
from collections import defaultdict

from kafka import KafkaProducer

from ethereumetl.streaming.eth_streamer_adapter import OP_STATUS

logger = logging.getLogger(__name__)


class CustomKafkaProducer(KafkaProducer):

    def __int__(self, **kwargs):
        super().__init__(**kwargs)

    def _wait_on_metadata(self, topic, max_wait):
        start_time = datetime.now()
        result = super()._wait_on_metadata(topic, max_wait)
        duration = datetime.now() - start_time
        print(f"wait on metadata: {duration}ms")
        return result


class KafkaItemExporter:
    debezium_json: bool = False

    def __init__(self, output, item_type_to_topic_mapping, debezium_json=False):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.connection_url = self.get_connection_url(output)
        kafka_options = self.get_kafka_option_from_env()

        options = {
            'bootstrap_servers': self.connection_url,
            'retries': sys.maxsize,
            'max_in_flight_requests_per_connection': 1,
            'linger_ms': 1000,
            'batch_size': 16384 * 32,
            **kafka_options
        }
        print('kafka options', options)
        self.producer = CustomKafkaProducer(**options)
        self.debezium_json = debezium_json

    def get_kafka_option_from_env(self):
        try:
            env_option = os.getenv('KafkaOptions')
            return json.loads(env_option)
        except Exception as e:
            return {}

    def get_connection_url(self, output):
        try:
            output_url = output.split('/')[1]
            return output_url.split(';')
        except KeyError:
            raise Exception(
                'Invalid kafka output param, It should be in format of "kafka/127.0.0.1:9092"')

    def open(self):
        pass

    def _convert_to_debezium_json(self, converted_item):
        if converted_item["op"] == OP_STATUS.INSERT:
            return {
                "before": None,
                "after": converted_item,
                "op": OP_STATUS.INSERT,
                "ts_ms": int(time.time() * 1000),
                "type": converted_item["type"],
            }
        else:
            return {
                "before": converted_item,
                "after": None,
                "op": OP_STATUS.DELETE,
                "ts_ms": int(time.time() * 1000),
                "type": converted_item["type"],
            }

    def export_items(self, items):
        group = defaultdict(list)
        logger.info(f"Start coding, records count {len(items)}")
        for item in items:
            item_type = item.get('type')
            arr = group.get(item_type)
            if arr is None:
                group[item_type] = [item]
            else:
                arr.append(item)

        logger.info("Start sending")
        for key, value in group.items():
            if key not in self.item_type_to_topic_mapping:
                # ignore topic name is None
                continue

            topic_name = self.item_type_to_topic_mapping[key]
            for item in value:
                self.send_message(topic_name, item)
        logger.info("Wait flush")
        self.producer.flush(timeout=30)
        logger.info("End of sending")

    def send_message(self, topic_name, message):
        if self.debezium_json:
            message = self._convert_to_debezium_json(message)
        message_byte = json.dumps(message).encode('utf-8')
        self.producer.send(topic_name, value=message_byte)

    def fail(self, error):
        logger.exception(f"Send message to kafka failed: {error}.",
                         exc_info=error)

    def success(self, status):
        logger.info(f"Send message to kafka successfully {status}.")

    def close(self):
        pass
