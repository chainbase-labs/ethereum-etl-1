# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import logging
from collections import defaultdict

from ethereumetl.enumeration.entity_type import EntityType


class MultiItemExporter:
    def __init__(self, item_exporters):
        self.item_exporters = item_exporters

    def open(self):
        for exporter in self.item_exporters:
            exporter.open()

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
        logging.info(f"-------- Exports Statistics --------")
        blocks = set([
          item.get('number') for item in items if item.get('type') == 'block'
        ])
        logging.info(f"Exporting {len(blocks)} block, {blocks}.")

        for entity_type in tuple(EntityType.ALL_FOR_STREAMING):
            self.statistics_by_block(items, entity_type)

        for exporter in self.item_exporters:
            exporter.export_items(items)

    def export_item(self, item):
        for exporter in self.item_exporters:
            exporter.export_item(item)

    def close(self):
        for exporter in self.item_exporters:
            exporter.close()
