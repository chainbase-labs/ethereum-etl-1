import json
from urllib.parse import urlparse, parse_qs

from redis.cluster import RedisCluster
from ethereumetl.enumeration.entity_type import EntityType


class CacheService:
    chain: str = None
    redis_client: RedisCluster = None
    cache_block_count = 250

    def __init__(self, chain: str, output: str):
        self.chain = chain
        connection_opt = parse_schema(output)
        self.redis_client = RedisCluster(**{
            'host': connection_opt.get('host'),
            'port': connection_opt.get('port')
        })
        self.cache_block_count = int(connection_opt.get('cachedBlockCount')) if 'cachedBlockCount' in connection_opt else 250

    def write_cache(self, message_type, block_number, data):
        sorted_set_key = f"{self.chain}_{message_type}"

        if message_type == EntityType.BLOCK:
            self.redis_client.zadd(sorted_set_key, mapping={
                self.serialize(data): block_number
            })
        else:
            self.redis_client.zremrangebyscore(sorted_set_key, min=block_number, max=block_number)
            self.redis_client.zadd(sorted_set_key, mapping={
                self.serialize(item): block_number for item in data
            })

        self.redis_client.zremrangebyscore(sorted_set_key, min=0, max=block_number - self.cache_block_count)

    def read_cache(self, message_type, block_number) -> list:
        def get_index(item: dict):
            sort_key = f"{message_type}_index"
            return item.get(sort_key) if sort_key in item else 0

        sorted_set_key = f"{self.chain}_{message_type}"
        result_list = self.redis_client.zrangebyscore(sorted_set_key, min=block_number, max=block_number)
        decoded_list = [self.deserialize(item) for item in result_list]
        return sorted(decoded_list, key=get_index)

    @staticmethod
    def serialize(message):
        return json.dumps(message)

    @staticmethod
    def deserialize(message):
        return json.loads(message)


def parse_schema(connection_url):
    connection_option = urlparse(connection_url)
    host, port = connection_option.netloc.split(":")
    config = {key: value[0] for key, value in parse_qs(connection_option.query).items()}
    return {
        "host": host,
        "port": port,
        **config
    }
