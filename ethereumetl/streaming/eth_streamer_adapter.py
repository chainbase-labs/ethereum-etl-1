import logging
import time

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.jobs.exporters.in_memory_item_exporter import InMemoryItemExporter
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.jobs.export_block_receipts_job import ExportBlockReceiptsJob
from ethereumetl.jobs.export_blocks_job import ExportBlocksJob
from ethereumetl.jobs.export_geth_traces_job import ExportGethTracesJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.export_traces_job import ExportTracesJob
from ethereumetl.jobs.extract_contracts_job import ExtractContractsJob
from ethereumetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from ethereumetl.jobs.extract_tokens_job import ExtractTokensJob
from ethereumetl.service.reorg_service import ReorgService
from ethereumetl.streaming.enrich import (
    enrich_transactions,
    enrich_logs,
    enrich_token_transfers,
    enrich_traces,
    enrich_contracts,
    enrich_tokens,
    enrich_traces_with_blocks_transactions,
    enrich_blocks_finally,
)
from ethereumetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from ethereumetl.streaming.eth_item_timestamp_calculator import EthItemTimestampCalculator
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.web3_utils import build_web3


class OP_STATUS:
    INSERT: str = "c"
    DELETE: str = "d"


class EthStreamerAdapter:
    def __init__(
        self,
        batch_web3_provider,
        node_client,
        item_exporter=ConsoleItemExporter(),
        batch_size=100,
        max_workers=5,
        chain="ethereum",
        entity_types=tuple(EntityType.ALL_FOR_STREAMING),
        reorg_service: ReorgService = None
    ):
        self.batch_web3_provider = batch_web3_provider
        self.node_client = node_client
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.chain = (chain,)
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.reorg_service = reorg_service

    def get_client_version(self):
        try:
            response = self.batch_web3_provider.make_request('web3_clientVersion', [])
            return response["result"]
        except Exception as e:
            return self.node_client

    def open(self):
        self.client_version = self.get_client_version()
        self.item_exporter.open()

    def get_current_block_number(self):
        w3 = build_web3(self.batch_web3_provider)
        return int(w3.eth.getBlock("latest").number)

    def verify_clients(self, *client: str):
        for item in client:
            if self.client_version.lower().startswith(item):
                return True
        return False

    def export_all(self, start_block, end_block):
        # Export blocks and transactions
        blocks, transactions = [], []

        # TODO Check to see if a rollback occurred on the previous block
        if self._should_export(EntityType.BLOCK) or self._should_export(EntityType.TRANSACTION):
            blocks, transactions = self._export_blocks_and_transactions(start_block, end_block)

        sorted_enriched_blocks = sort_by(blocks, "number")

        # Check if reorg occurs
        if self.reorg_service is not None and len(sorted_enriched_blocks) != 0:
            self.reorg_service.check_batch(sorted_enriched_blocks)

        # Export receipts and logs
        receipts, logs = [], []
        if self._should_export(EntityType.RECEIPT) or self._should_export(EntityType.LOG):
            if not self.verify_clients("geth", "erigon"):
                receipts, logs = self._export_receipts_and_logs(transactions)
            else:
                receipts, logs = self._export_receipts_and_logs_by_block(blocks)

        # Extract token transfers
        token_transfers = []
        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = self._extract_token_transfers(logs)

        # Export traces
        traces = []
        if self._should_export(EntityType.TRACE):
            # use node client to check which client to get trace
            # 直接在这里补trace status,index 等字段不行，这里raw_traces没有trx_hash只有enrich 之后才能继续提取
            traces = self._export_traces(start_block, end_block, self.node_client)

        # Export contracts
        contracts = []
        if self._should_export(EntityType.CONTRACT):
            contracts = self._export_contracts(traces)

        # Export tokens
        tokens = []
        if self._should_export(EntityType.TOKEN):
            tokens = self._extract_tokens(contracts)

        enriched_transactions = (
            enrich_transactions(blocks, transactions, receipts) if EntityType.TRANSACTION in self.entity_types else []
        )
        enriched_logs = enrich_logs(blocks, logs, receipts) if EntityType.LOG in self.entity_types else []
        enriched_token_transfers = (
            enrich_token_transfers(blocks, token_transfers) if EntityType.TOKEN_TRANSFER in self.entity_types else []
        )
        enriched_traces = (
            enrich_traces_with_blocks_transactions(blocks, traces, enriched_transactions)
            if EntityType.TRACE in self.entity_types and self.node_client == "geth"
            else enrich_traces(blocks, traces)
            if EntityType.TRACE in self.entity_types
            else []
        )

        enriched_blocks = enrich_blocks_finally(
            blocks,
            enriched_transactions,
            enriched_traces,
        ) if EntityType.BLOCK in self.entity_types else []

        # geth 直接拿到的trace 没有txs hash,status等信息，contract表的计算依赖status,因此需要在trace enrich之后，在计算一次
        if self.node_client == "geth" and self._should_export(EntityType.CONTRACT):
            contracts = self._export_contracts(enriched_traces)

        enriched_contracts = enrich_contracts(blocks, contracts) if EntityType.CONTRACT in self.entity_types else []
        enriched_tokens = enrich_tokens(blocks, tokens) if EntityType.TOKEN in self.entity_types else []

        logging.info("Exporting with " + type(self.item_exporter).__name__)

        reorg_stream_cdc_messages = self._get_reorg_cdc_streaming_message(start_block)

        all_items = (
            reorg_stream_cdc_messages
            + sort_by(enriched_blocks, "number")
            + sort_by(enriched_transactions, ("block_number", "transaction_index"))
            + sort_by(enriched_logs, ("block_number", "log_index"))
            + sort_by(enriched_token_transfers, ("block_number", "log_index"))
            + sort_by(enriched_traces, ("block_number", "trace_index"))
            + sort_by(enriched_contracts, ("block_number",))
            + sort_by(enriched_tokens, ("block_number",))
        )

        self.calculate_item_info(all_items)

        self.item_exporter.export_items(all_items)
        self.reorg_service.save()

    def _get_reorg_cdc_streaming_message(self, start_block: int) -> list:
        if self.reorg_service.reorg_block is not None:
            reorg_block_number = self.reorg_service.reorg_block.get("block_number")
            if start_block <= reorg_block_number:
                reorg_block_range = self.reorg_service.reorg_block.get("reorg_block_range")
                block_number_list = [int(item) for item in reorg_block_range.keys()]
                reorg_streaming_record = self.reorg_service.get_delete_record(block_number_list, self.entity_types)
                for item in reorg_streaming_record:
                    item.update({"op": OP_STATUS.DELETE})
                return reorg_streaming_record
        return []

    def _export_blocks_and_transactions(self, start_block, end_block):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(item_types=["block", "transaction"])
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            chain=self.chain,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION),
        )
        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items("block")
        transactions = blocks_and_transactions_item_exporter.get_items("transaction")
        return blocks, transactions

    def _export_receipts_and_logs_by_block(self, blocks):
        exporter = InMemoryItemExporter(item_types=["receipt", "log"])
        job = ExportBlockReceiptsJob(
            blocks_iterable=(block["number"] for block in blocks),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
        )
        job.run()
        receipts = exporter.get_items("receipt")
        logs = exporter.get_items("log")
        return receipts, logs

    def _export_receipts_and_logs(self, transactions):
        exporter = InMemoryItemExporter(item_types=["receipt", "log"])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(transaction["hash"] for transaction in transactions),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            chain=self.chain,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
        )
        job.run()
        receipts = exporter.get_items("receipt")
        logs = exporter.get_items("log")
        return receipts, logs

    def _extract_token_transfers(self, logs):
        exporter = InMemoryItemExporter(item_types=["token_transfer"])
        job = ExtractTokenTransfersJob(
            logs_iterable=logs, batch_size=self.batch_size, max_workers=self.max_workers, item_exporter=exporter
        )
        job.run()
        token_transfers = exporter.get_items("token_transfer")
        return token_transfers

    def _export_traces(self, start_block, end_block, node_client):
        exporter = InMemoryItemExporter(item_types=["trace"])
        if node_client == "geth":
            job = ExportGethTracesJob(
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                batch_web3_provider=self.batch_web3_provider,
                max_workers=self.max_workers,
                item_exporter=exporter,
            )
        else:
            job = ExportTracesJob(
                start_block=start_block,
                end_block=end_block,
                batch_size=self.batch_size,
                web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
                max_workers=self.max_workers,
                item_exporter=exporter,
            )

        job.run()
        traces = exporter.get_items("trace")
        return traces

    def _export_contracts(self, traces):
        exporter = InMemoryItemExporter(item_types=["contract"])
        job = ExtractContractsJob(
            traces_iterable=traces, batch_size=self.batch_size, max_workers=self.max_workers, item_exporter=exporter
        )
        job.run()
        contracts = exporter.get_items("contract")
        return contracts

    def _extract_tokens(self, contracts):
        exporter = InMemoryItemExporter(item_types=["token"])
        job = ExtractTokensJob(
            contracts_iterable=contracts,
            web3=ThreadLocalProxy(lambda: build_web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
        )
        job.run()
        tokens = exporter.get_items("token")
        return tokens

    def _should_export(self, entity_type):
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(
                EntityType.LOG) or self._should_export(EntityType.TRACE) or self._should_export(EntityType.CONTRACT)

        if entity_type == EntityType.RECEIPT:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(EntityType.TOKEN_TRANSFER)

        if entity_type == EntityType.LOG:
            return EntityType.LOG in self.entity_types or self._should_export(EntityType.TOKEN_TRANSFER)

        if entity_type == EntityType.TOKEN_TRANSFER:
            return EntityType.TOKEN_TRANSFER in self.entity_types

        if entity_type == EntityType.TRACE:
            return EntityType.TRACE in self.entity_types or self._should_export(EntityType.CONTRACT)

        if entity_type == EntityType.CONTRACT:
            return EntityType.CONTRACT in self.entity_types or self._should_export(EntityType.TOKEN)

        if entity_type == EntityType.TOKEN:
            return EntityType.TOKEN in self.entity_types

        raise ValueError("Unexpected entity type " + entity_type)

    def calculate_item_info(self, items):
        for item in items:
            item.update(
                {
                    "item_id": self.item_id_calculator.calculate(item),
                    "item_ns": time.time_ns(),
                    "item_timestamp": self.item_timestamp_calculator.calculate(item),
                    "op": OP_STATUS.INSERT if "op" not in item else item.get("op"),
                }
            )

    def close(self):
        self.item_exporter.close()


def sort_by(arr, fields):
    if isinstance(fields, tuple):
        fields = tuple(fields)
    return sorted(arr, key=lambda item: tuple(item.get(f) for f in fields))
