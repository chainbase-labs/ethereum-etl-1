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
    "resource_group,provider_type,block_number,batch_count,expectation",
    [
        ('normal_no_reorg', 'mock', 17684067, 2, does_not_raise()),
        ('prev_block_reorg', 'mock', 17684067, 2, pytest.raises(ReorgException,
                                                                match='A block reorg occurred at block height 17684066')
         ),
        skip_if_slow_tests_disabled((
            'prev_block_reorg_8_block', 'infura', 17684059, 50,
            pytest.raises(
                ReorgException,
                match='A block reorg occurred at block height 17684058'))
        )
    ]
)
def test_reorg(resource_group, provider_type, block_number, batch_count,
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
            last_sync_block_hash=tests.resources.get_resource(
                [RESOURCE_GROUP, resource_group],
                'region_last_sync_block_hash.json'
            ),
            batch_count=batch_count,
            readonly=True
        )
        reorg_service.check_prev_block({
            "number": block_number,
            "parent_hash": "0x2ee865042ecb315717c7aca65c6eccc967b55dd0ce3c09689a34dada4d96d069",
            "hash": "0x80012009d06d00058ca43a175337bbcac17609851e6de41eca871472f67f3d45"
        })


@pytest.mark.parametrize(
    "resource_group,provider_type,batch_hash_file,expectation",
    [
        (
            'reorg_occurs_in_batch', 'mock', 'batch_hash.json',
            pytest.raises(
                BatchReorgException,
                match='Reorganization is highly occurring in the current block 17684064 0x1f3b091bfca49163a635b82e1977d8fb3468289dc437022d98f1c6b5ee83fc54')
        ),
        (
            'reorg_occurs_in_batch_head', 'mock', 'batch_hash.json',
            pytest.raises(
                ReorgException,
                match='A block reorg occurred at block height 17684057')
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
            last_sync_block_hash=tests.resources.get_resource(
                [RESOURCE_GROUP, resource_group],
                'last_sync_block_hash.json'
            ),
            batch_count=1,
            readonly=True
        )
        reorg_service.check_batch(
            json.loads(read_resource_lambda(batch_hash_file))
        )


@pytest.mark.parametrize(
    "resource_group,provider_type,block_number,batch_count,reorg_block_number,expectation",
    [
        (
            'find_reorg_block', 'mock', 17792258, 2, 17792258, does_not_raise()
        ),
        skip_if_slow_tests_disabled((
            'find_reorg_block', 'infura', 17792259, 5, 17792258, does_not_raise())
        )
    ]
)
def test_find_prev_reorg(resource_group, provider_type, block_number,
    batch_count, reorg_block_number, expectation):
    def read_resource_lambda(file):
        return read_resource(resource_group, file)

    with expectation:
        reorg_service = ReorgService(
            capacity=100,
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_web3_provider(
                    provider_type,
                    read_resource_lambda=read_resource_lambda,
                    batch=True
                )
            ),
            last_sync_block_hash=tests.resources.get_resource(
                [RESOURCE_GROUP, resource_group],
                'last_sync_block_hash.json'
            ),
            batch_count=batch_count,
            readonly=True
        )
        reorg_service.init_block_hash_file(block_number)
        reorg_block = reorg_service.find_reorg_block(block_number)
        assert reorg_block.get('number') == reorg_block_number
