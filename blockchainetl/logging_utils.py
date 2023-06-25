import logging


def logging_basic_config(filename=None, level_name='INFO'):
  format = '%(asctime)s - %(name)s [%(levelname)s] - %(message)s'
  log_level = logging.INFO if level_name is None else logging.getLevelName(
    level_name)
  if filename is not None:
    logging.basicConfig(level=log_level, format=format, filename=filename)
  else:
    logging.basicConfig(level=log_level, format=format)

  logging.getLogger('ethereum_dasm.evmdasm').setLevel(logging.ERROR)
  logging.getLogger('kafka.producer.kafka').setLevel(logging.DEBUG)
  logging.getLogger('kafka.producer.sender').setLevel(logging.DEBUG)
  logging.getLogger('kafka.producer.record_accumulator').setLevel(logging.DEBUG)
