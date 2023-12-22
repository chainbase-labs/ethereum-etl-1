import collections
import json
import logging
import os
from collections import defaultdict
from datetime import datetime

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class KafkaItemExporter:

    def __init__(self, output, item_type_to_topic_mapping):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.connection_url = self.get_connection_url(output)
        kafka_options = self.get_kafka_option_from_env()

        options = {
            'bootstrap.servers': self.connection_url,
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True,
            'linger.ms': 1000,
            'queue.buffering.max.messages': 2147483648,
            'queue.buffering.max.kbytes': 2147483648,
            **kafka_options
        }
        print('kafka options', options)
        self.producer = Producer(**options)

    def get_connection_url(self, output):
        try:
            return output.split('/')[1]
        except KeyError:
            raise Exception(
                'Invalid kafka output param, It should be in format of "kafka/127.0.0.1:9092"')

    def get_kafka_option_from_env(self):
        try:
            env_option = os.getenv('KafkaOptions')
            return json.loads(env_option)
        except Exception as e:
            return {}

    def open(self):
        pass

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
        for key, value in group.items():
            if key not in self.item_type_to_topic_mapping:
                # ignore topic name is None
                continue

            topic_name = self.item_type_to_topic_mapping[key]
            """
            if(check has reorg block):
                write_reorg_message();
            """
            if group.get('reorg') is not None:
                if len(group.get('reorg')) != 1:
                    raise RuntimeError(
                        f"reorg occurs at multiple block heights {group.get('reorg')}")
                reorg_message = group.get('reorg')[0]
                logger.info(f'Writes a reorg message {reorg_message}')
                self.send_message(topic_name, reorg_message)

            for item in value:
                self.send_message(topic_name, item)
        self.producer.flush(timeout=30)
        logger.info(f"End of sending {datetime.now() - start_time}")

    def send_message(self, topic_name, message):
        message_byte = json.dumps(message).encode('utf-8')
        self.producer.produce(topic_name, value=message_byte)

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode('utf-8')
            return self.producer.produce(self.item_type_to_topic_mapping[item_type], value=data)
        else:
            logger.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def close(self):
        pass


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
