import json
import logging
import os
from collections import OrderedDict

from blockchainetl.file_utils import smart_open
from ethereumetl.json_rpc_requests import generate_get_block_by_number_json_rpc
from ethereumetl.utils import hex_to_dec


class FixedCapacityDict(OrderedDict):
    def __init__(self, capacity):
        super().__init__()
        self.capacity = capacity

    def __setitem__(self, key, value):
        if key in self:
            del self[key]
        elif len(self) == self.capacity:
            self.popitem(last=False)
        super().__setitem__(key, value)


class ReorgException(RuntimeError):
    block_number: int

    def __init__(self, block_number, *args, **kwargs):  # real signature unknown
        super().__init__(args, kwargs)
        self.block_number = block_number


class BatchReorgException(RuntimeError):
    block_number: int

    def __init__(self, block_number, *args, **kwargs):  # real signature unknown
        super().__init__(args, kwargs)
        self.block_number = block_number


class ReorgService:
    _capacity: int
    _blockhash_capacity_dict: FixedCapacityDict
    _last_sync_block_hash: str
    _batch_web3_provider = None
    _batch_count = 100
    reorg_block = None

    def __init__(
        self, capacity, batch_web3_provider,
        last_sync_block_hash='last_sync_block_hash.json',
        batch_count=100,
        readonly=False
    ):
        self._capacity = capacity
        self._blockhash_capacity_dict = FixedCapacityDict(capacity)
        self._batch_web3_provider = batch_web3_provider
        self._last_sync_block_hash = last_sync_block_hash
        self._batch_count = batch_count
        self.logger = logging.getLogger('ReorgService')
        self.readonly = readonly

    def save(self):
        if self.readonly:
            return
        with smart_open(self._last_sync_block_hash, 'w') as file_handle:
            file_handle.write(json.dumps(self._blockhash_capacity_dict))

    def read_from_file(self):
        with smart_open(self._last_sync_block_hash, 'r') as file_handle:
            try:
                content = json.loads("".join(file_handle.readlines()))
                self._blockhash_capacity_dict.update(
                    {int(key): value for key, value in content.items()})
                return True
            except Exception as e:
                self.logger.error('Error reading contents of block hash file',
                                  e)
        return False

    def init_block_hash_file(self, block_number: int):
        if len(self._blockhash_capacity_dict) != 0:
            return
        if os.path.exists(self._last_sync_block_hash):
            if self.read_from_file():
                return

        block_header_rpc = list(
            generate_get_block_by_number_json_rpc(
                range(block_number, block_number - self._batch_count,
                      -1),
                include_transactions=False
            )
        )
        responses = self._batch_web3_provider.make_batch_request(
            json.dumps(block_header_rpc))

        self._blockhash_capacity_dict.update({
            hex_to_dec(response.get('result').get('number')): response.get(
                'result').get('hash') for response in responses
        })
        self.save()

    def check_batch(self, export_block_items):
        """ Check to see if a reorg occurred on fetch """

        sorted_block_items = sorted(export_block_items, key=lambda item: item.get('number'))

        start_block = min(
            sorted_block_items,
            key=lambda item: item.get('number')
        )
        self.check_prev_block(start_block)

        for index in range(1, len(sorted_block_items)):
            prev_block = sorted_block_items[index - 1]
            current_block = sorted_block_items[index]
            if prev_block.get('hash') != current_block.get('parent_hash'):
                raise BatchReorgException(
                    prev_block.get('number'),
                    f"Reorganization is highly occurring in the current block {prev_block.get('number')} {prev_block.get('hash')}"
                )

        last_block = max(
            sorted_block_items,
            key=lambda item: item.get('number')
        )

        if not self.check_block_hash(
            last_block.get('number'),
            last_block.get('hash')
        ):
            raise BatchReorgException(
                last_block.get('number'),
                f"Reorganization is highly occurring in the current block {last_block.get('number')} {last_block.get('hash')}"
            )
        self.logger.info(
            f"Check batch that block height {last_block.get('number')} is correct")

        # write hash to block
        for item in sorted_block_items:
            self.add(item.get('number'), item.get('hash').lower())

    def check_block_hash(self, block_number: int, target_hash: str):
        if target_hash is None:
            return True
        response = self._batch_web3_provider.make_request(
            'eth_getBlockByNumber', [hex(block_number), False]
        )
        result = response.get('result')
        block_hash = result.get('hash')
        return target_hash.lower() == block_hash.lower()

    def check_prev_block(self, block: dict):
        """
        Check if the block hash is consistent with the local,
        if not then lookup to the block height where the reorg occurred
        """

        prev_block = block.get('number') - 1
        self.init_block_hash_file(prev_block)

        if block.get(
            'parent_hash').lower() == self._blockhash_capacity_dict.get(
            prev_block):
            self.logger.info(
                f"Check that block height {prev_block} is correct")
            return

        self.reorg_block = self.find_reorg_block(prev_block)
        reorg_start_block = self.reorg_block.get('number')
        self.logger.warning(f"Rollback occurs at the height of the discovery block, {self.reorg_block}.")
        self._clear_block_from_number(reorg_start_block - 1)
        raise ReorgException(
            reorg_start_block,
            f'A block reorg occurred at block height {reorg_start_block}')

    @staticmethod
    def create_reorg_message(reorg_block: dict):
        reorg_block.update({
            'type': 'reorg',
        })


    def _clear_block_from_number(self, end_block):
        valid_block_hash = {
            key: value for key, value in self._blockhash_capacity_dict.items()
            if key <= end_block
        }
        self._blockhash_capacity_dict.clear()
        self._blockhash_capacity_dict.update(valid_block_hash)
        self.save()

    def find_reorg_block(self, block_number: int):
        """ Find the height of the block where the reorg occurred """

        start_block = max(block_number - self._batch_count, 0)

        block_header_rpc = list(
            generate_get_block_by_number_json_rpc(
                range(block_number, start_block, -1),
                include_transactions=False
            )
        )
        responses = self._batch_web3_provider.make_batch_request(
            json.dumps(block_header_rpc))

        for index, response in enumerate(responses):
            result = response.get('result')
            number = hex_to_dec(result.get('number'))
            block_hash = result.get('hash')
            if number not in self._blockhash_capacity_dict:
                raise RuntimeError(f'Missing {number} height block hash!')

            if self._blockhash_capacity_dict.get(number) == block_hash.lower():
                reorg_block = responses[index - 1].get('result')
                return {
                    'number': hex_to_dec(reorg_block.get('number')),
                    'reorg_block_hash': block_hash,
                    'hash': reorg_block.get('hash'),
                    'miner': reorg_block.get('miner'),
                    'mixHash': reorg_block.get('mixHash'),
                    'nonce': reorg_block.get('nonce'),
                    'parentHash': reorg_block.get('parentHash'),
                    'receiptsRoot': reorg_block.get('receiptsRoot'),
                    'size': reorg_block.get('size'),
                    'stateRoot': reorg_block.get('stateRoot'),
                    'timestamp': reorg_block.get('timestamp'),
                    'totalDifficulty': reorg_block.get('totalDifficulty'),
                    'transactionsRoot': reorg_block.get('transactionsRoot'),
                    'uncle_hash': reorg_block.get('uncle_hash'),
                }

        return self.find_reorg_block(start_block - 1)

    def add(self, block_number: int, block_hash: str):
        self._blockhash_capacity_dict.update({
            block_number: block_hash
        })
        self.save()
