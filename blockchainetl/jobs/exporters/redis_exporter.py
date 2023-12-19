import logging
from collections import defaultdict
from datetime import datetime

from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.service.cache_service import CacheService
from ethereumetl.streaming.eth_streamer_adapter import OP_STATUS

logger = logging.getLogger(__name__)


class RedisItemExporter:
    cache_service: CacheService

    def __init__(self, output: str, chain: str):
        if not output.startswith("redis://"):
            raise RuntimeError(
                "The current output address is not a valid redis link address.")

        self.cache_service = CacheService(chain=chain, output=output)

    def open(self):
        pass

    def export_items(self, items):
        # ignore op is delete
        insert_items = [item for item in items if item.get('op') == OP_STATUS.INSERT]

        start_time = datetime.now()
        for item_type, item_arr in group_by_key(insert_items).items():
            if item_type == EntityType.BLOCK:
                for block_item in item_arr:
                    self.cache_service.write_cache(block_item.get('type'), block_item.get('number'), block_item)
            else:
                for block_number, value in group_by_key(item_arr, 'block_number').items():
                    self.cache_service.write_cache(item_type, block_number, value)
        logger.info(f"Write to cached: {datetime.now() - start_time}")

    def close(self):
        pass


def group_by_key(items, key='type'):
    dict_group = defaultdict(list)
    for item in items:
        dict_group[item.get(key)].append(item)
    return dict_group
