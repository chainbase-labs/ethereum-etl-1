import logging

import requests


class MonitorService:
    blockchain: str
    endpoint: str

    def __init__(self, endpoint: str, blockchain: str):
        self.endpoint = endpoint
        self.blockchain = blockchain
        self.logger = logging.getLogger('MonitorService')

    def send_error(self, block_number, error):
        try:
            if self.endpoint is None or len(self.endpoint) == 0:
                return

            requests.post(self.endpoint, json={
                "block_number": block_number,
                "blockchain": self.blockchain,
                "error": str(error)[0: 1024]
            })
        except Exception as e:
            self.logger.error(f"An error was encountered sending an alert, {e}")
