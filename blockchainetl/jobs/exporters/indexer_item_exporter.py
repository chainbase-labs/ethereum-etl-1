import collections
import csv
import logging
import os
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

    write_rows = 0
    for item_type, file_name in self.item_type_to_file_mapping.items():
      items = items_grouped_by_type.get(item_type)
      if items:
        if item_type not in self.files:
          table = self.item_type_to_table_mapping[item_type]
          converted_items = list(self.convert_items(items, table))
          with open(file_name, 'w', newline='', encoding='UTF-8') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(converted_items)
          self.files[item_type] = file_name

        write_rows += len(converted_items)

    duration = datetime.now() - start_time
    self.logger.info(
        f"Finished write. Total items processed: {write_rows}. "
        f"Took {str(duration)}."
    )

    if write_rows > 0:
      self.call_go()
    self.files.clear()

  def convert_items(self, items, table):
    columns = [column.name for column in table.columns]
    for item in items:
      converted_item = self.converter.convert_item(item)
      if converted_item.get("type") == "transaction":
        if not (converted_item.get("input").startswith(
            "0x646174613a2c7b") and converted_item.get("receipt_status") == 1):
          continue  # 如果不满足条件，跳过此项
      elif converted_item.get("type") == "log":
        if converted_item.get("topic0") not in [
          '0xf1d95ed4d1680e6f665104f19c296ae52c1f64cd8114e84d55dc6349dbdafea3',
          '0xe2750d6418e3719830794d3db788aa72febcd657bcd18ed8f1facdbf61a69a9a']:
          continue  # 如果不满足条件，跳过此项
      yield [converted_item.get(column) for column in columns]

  def close(self):
    pass

  def call_go(self):
    start_time = datetime.now()
    go_program = os.environ.get('GO_PROGRAM_PATH', './indexer')

    command = [go_program, '--transactions', self.files["transaction"],
               '--logs',
               self.files["log"]]

    try:
      result = subprocess.run(command, check=True, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
      self.logger.info("Go program output: %s", result.stdout.decode())
    except subprocess.CalledProcessError as e:
      self.logger.error("Error calling Go program: %s",
                        str(e) + ", " + str(e.output))
      sys.exit(1)
    except Exception as e:
      self.logger.error(
          "Error calling Go program: %s",
          str(e) + ", " + str(e.output))
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
