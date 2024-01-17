import collections
import csv
import logging
import subprocess
from datetime import datetime

from blockchainetl.jobs.exporters.converters.composite_item_converter import \
  CompositeItemConverter


class IndexerItemExporter:
  logger = logging.getLogger('IndexerItemExporter')

  def __init__(
      self, item_type_to_file_mapping, item_type_to_table_mapping, converters=()
  ):
    self.converter = CompositeItemConverter(converters)
    self.item_type_to_file_mapping = item_type_to_file_mapping
    self.item_type_to_table_mapping = item_type_to_table_mapping
    self.files = {}

  def open(self):
    pass

  def export_items(self, items):
    start_time = datetime.now()
    items_grouped_by_type = group_by_item_type(items)

    for item_type, file_name in self.item_type_to_file_mapping.items():
      items = items_grouped_by_type.get(item_type)
      if items:
        # file_name = f'{item_type}.csv'
        if item_type not in self.files:
          self.files[item_type] = open(file_name, 'a', newline='',
                                       encoding='UTF-8')

        table = self.item_type_to_table_mapping[item_type]
        csv_writer = csv.writer(self.files[item_type])
        converted_items = list(self.convert_items(items, table))
        csv_writer.writerows(converted_items)

    duration = datetime.now() - start_time
    self.logger.info(
        f"Finished write. Total items processed: {len(items)}. "
        f"Took {str(duration)}."
    )
    # self.call_go()

  def convert_items(self, items, table):
    for item in items:
      converted_item = self.converter.convert_item(item)
      columns = [column.name for column in table.columns]
      # converted_item.pop('type', None)
      yield [converted_item.get(column) for column in columns]

  def close(self):
    for file in self.files.values():
      file.close()

  def call_go(self):
    # 指定编译好的 Go 程序的路径
    go_program = './indexer'

    # 指定文件路径
    transactions_file = './data/transactions.txt'
    logs_file = './data/logs.txt'

    # 构建命令
    command = [go_program, '--transactions', transactions_file, '--logs',
               logs_file]

    try:
      # 调用 Go 程序
      result = subprocess.run(command, check=True, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
      print("Go program output:", result.stdout.decode())
    except subprocess.CalledProcessError as e:
      print("Error calling Go program:", e.stderr.decode())


def group_by_item_type(items):
  result = collections.defaultdict(list)
  for item in items:
    result[item.get('type')].append(item)

  return result
