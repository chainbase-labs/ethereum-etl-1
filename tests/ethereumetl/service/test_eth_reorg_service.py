import json

import pytest

import tests.resources
from tests.ethereumetl.job.helpers import get_web3_provider

from contextlib import nullcontext as does_not_raise
from ethereumetl.thread_local_proxy import ThreadLocalProxy

from ethereumetl.service.reorg_service import ReorgService, ReorgException, \
    BatchReorgException
from tests.helpers import skip_if_slow_tests_disabled

RESOURCE_GROUP = 'test_reorg_service'


def read_resource(resource_group, file_name):
    return tests.resources.read_resource([RESOURCE_GROUP, resource_group],
                                         file_name)


@pytest.mark.parametrize(
    "resource_group,provider_type,batch_count,expectation",
    [
        ('normal_no_reorg', 'mock', 2, does_not_raise()),
        ('prev_block_reorg', 'mock', 2, pytest.raises(ReorgException,
                                                      match='A block reorg occurred at block height 17684065')
         ),
        skip_if_slow_tests_disabled((
            'prev_block_reorg_8_block', 'infura', 50,
            pytest.raises(
                ReorgException,
                match='A block reorg occurred at block height 17684058'))
        )

    ]
)
def test_reorg(resource_group, provider_type, batch_count, expectation):
    def read_resource_lambda(file):
        return read_resource(resource_group, file)

    with expectation:
        reorg_service = ReorgService(
            capacity=1000,
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_web3_provider(
                    provider_type,
                    read_resource_lambda=read_resource_lambda,
                    batch=True
                )
            ),
            last_sync_block_hash=tests.resources.get_resource(
                [RESOURCE_GROUP, resource_group],
                'region_last_sync_block_hash.json'
            ),
            batch_count=batch_count
        )
        reorg_service.check_prev_block(17684067)


@pytest.mark.parametrize(
    "resource_group,provider_type,batch_hash_file,expectation",
    [
        (
            'reorg_occurs_in_batch', 'mock', 'batch_hash.json',
            pytest.raises(
                BatchReorgException,
                match='Reorganization is highly occurring in the current block 17684067 0x80012009d06d00058ca43a175337bbcac17609851e6de41eca871472f67f3d41')
        )
    ]
)
def test_batch_reorg(resource_group, provider_type, batch_hash_file,
    expectation):
    def read_resource_lambda(file):
        return read_resource(resource_group, file)

    with expectation:
        reorg_service = ReorgService(
            capacity=1000,
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_web3_provider(
                    provider_type,
                    read_resource_lambda=read_resource_lambda,
                    batch=True
                )
            ),
            batch_count=1
        )
        reorg_service.check_batch(
            json.loads(read_resource_lambda(batch_hash_file))
        )
