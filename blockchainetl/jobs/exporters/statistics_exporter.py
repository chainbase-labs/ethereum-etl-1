import logging
from collections import defaultdict

from ethereumetl.enumeration.entity_type import EntityType

logger = logging.getLogger(__name__)


class KafkaItemExporter:

  def __init__(self):
    pass

  def open(self):
    pass

  @staticmethod
  def statistics_by_block(dict_list, item_type):
    if item_type == 'block':
      return

    count_dict = defaultdict(int)

    # 遍历字典列表
    for d in dict_list:
      if item_type == d.get('type'):
        count_dict[d['block_number']] += 1

    for group, count in count_dict.items():
      logging.info(f"Exporting {count} {item_type} from block {group}.")

  def export_items(self, items):
    logger.info(f"-------- Exports Statistics --------")
    blocks = set([
      item.get('number') for item in items if item.get('type') == 'block'
    ])
    logger.info(f"Exporting {len(blocks)} block, {blocks}.")

    for entity_type in tuple(EntityType.ALL_FOR_STREAMING):
      self.statistics_by_block(items, entity_type)

  def close(self):
    pass
