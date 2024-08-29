from datetime import datetime
import json
import logging
import os
import time
from collections import defaultdict

from confluent_kafka import Producer
from ethereumetl.streaming.eth_streamer_adapter import OP_STATUS

logger = logging.getLogger(__name__)


def delivery_report(err: Exception, msg: object) -> None:
    if err is not None:
        raise Exception(f"failed to deliver message: {err}")


class KafkaItemExporter:
    debezium_json: bool = False

    def __init__(self, output, item_type_to_topic_mapping, debezium_json=False):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.connection_url = self.get_connection_url(output)
        kafka_options = self.get_kafka_option_from_env()

        options = {
            'bootstrap.servers': self.connection_url,
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True,
            'linger.ms': 1000,
            'queue.buffering.max.messages': 2147483647,
            'queue.buffering.max.kbytes': 2147483647,
            **kafka_options
        }
        print('kafka options', options)
        self.producer = Producer(**options)
        self.debezium_json = debezium_json

    def get_kafka_option_from_env(self):
        try:
            env_option = os.getenv('KafkaOptions')
            return json.loads(env_option)
        except Exception as e:
            return {}

    def get_connection_url(self, output):
        try:
            return output.split('/')[1]
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

        start_time = datetime.now()
        logger.info("Start sending")
        send_bytes_count = 0
        for key, value in group.items():
            if key not in self.item_type_to_topic_mapping:
                # ignore topic name is None
                continue

            topic_name = self.item_type_to_topic_mapping[key]
            for item in value:
                send_bytes_count += self.send_message(topic_name, item)
        self.producer.flush(timeout=30)
        logger.info(f"End of sending {datetime.now() - start_time}, {send_bytes_count} Bytes")

    def send_message(self, topic_name, message):
        if self.debezium_json:
            message = self._convert_to_debezium_json(message)
        message_byte = json.dumps(message).encode('utf-8')
        self.producer.produce(topic_name, value=message_byte, callback=delivery_report)
        self.producer.poll(0)
        return len(message_byte)

    def fail(self, error):
        logger.exception(f"Send message to kafka failed: {error}.",
                         exc_info=error)

    def success(self, status):
        logger.info(f"Send message to kafka successfully {status}.")

    def close(self):
        pass
