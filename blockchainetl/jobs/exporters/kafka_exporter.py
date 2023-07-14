import collections
import json
import logging
import sys

from kafka import KafkaProducer
from collections import defaultdict

logger = logging.getLogger(__name__)

class KafkaItemExporter:

    def __init__(self, output, item_type_to_topic_mapping):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.connection_url = self.get_connection_url(output)
        print(self.connection_url)
        self.producer = KafkaProducer(
            bootstrap_servers=self.connection_url,
            retries=sys.maxsize,
            max_in_flight_requests_per_connection=1,
            linger_ms=20,
            batch_size=16384 * 32
        )

    def get_connection_url(self, output):
        try:
            return output.split('/')[1]
        except KeyError:
            raise Exception('Invalid kafka output param, It should be in format of "kafka/127.0.0.1:9092"')

    def open(self):
        pass

    def export_items(self, items):
        group = defaultdict(list)
        logger.info(f"Start coding, records count {len(items)}")
        for item in items:
            item_type = item.get('type')
            arr = group.get(item_type)
            data = json.dumps(item).encode('utf-8')
            if arr is None:
                group[item_type] = [data]
            else:
                arr.append(data)

        logger.info("Start sending")
        for key, value in group.items():
            topic_name = self.item_type_to_topic_mapping[key]
            for item in value:
                self.producer.send(topic_name, value=item).add_errback(self.fail)
        # for item in items:
        #     self.export_item(item)
        self.producer.flush(timeout=30)
        logger.info("End of sending")

    def fail(self, error):
        logger.exception(f"Send message to kafka failed: {error}.",
                         exc_info=error)

    def success(self, status):
        logger.info(f"Send message to kafka successfully {status}.")

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode('utf-8')
            logger.debug(data)
            return self.producer.send(
                self.item_type_to_topic_mapping[item_type],
                value=data).add_errback(self.fail)
        else:
            logger.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def close(self):
        pass


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
