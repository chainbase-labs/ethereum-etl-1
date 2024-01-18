import collections
import csv
import logging
import subprocess
import sys
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
          self.files[item_type] = open(file_name, 'w', newline='',
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

    self.call_go()

  def convert_items(self, items, table):
    columns = [column.name for column in table.columns]
    for item in items:
      converted_item = self.converter.convert_item(item)
      yield [converted_item.get(column) for column in columns]

  def close(self):
    for file in self.files.values():
      file.close()

  def call_go(self):
    start_time = datetime.now()
    go_program = '../open-indexer/indexer'

    command = [go_program, '--transactions', self.files["transaction"].name,
               '--logs',
               self.files["log"].name]

    try:
      result = subprocess.run(command, check=True, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
      self.logger.info("Go program output:", result.stdout.decode())
    except subprocess.CalledProcessError as e:
      self.logger.error("Error calling Go program:",
                        str(e) + "," + str(e.output))
      sys.exit(1)
    except Exception as e:
      self.logger.error(
          "Error calling Go program: " + str(e) + ", " + str(e.output))
      sys.exit(1)
    duration = datetime.now() - start_time
    self.logger.info(
        f"Finished Go program."
        f"Took {str(duration)}."
    )


def group_by_item_type(items):
  result = collections.defaultdict(list)
  for item in items:
    result[item.get('type')].append(item)

  return result
