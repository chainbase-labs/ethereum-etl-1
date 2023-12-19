import json
from urllib.parse import urlparse, parse_qs

from rediscluster import RedisCluster
from ethereumetl.enumeration.entity_type import EntityType


class CacheService:
    chain: str = None
    redis_client: RedisCluster = None
    cache_block_count = 250

    def __init__(self, chain: str, output: str):
        self.chain = chain
        connection_opt = parse_schema(output)
        print('redis options: ', connection_opt)
        self.redis_client = RedisCluster(
            host=connection_opt.get('host'),
            port=connection_opt.get('port')
        )
        print('self.redis_client', self.redis_client)
        self.cache_block_count = int(connection_opt.get('cachedBlockCount')) if 'cachedBlockCount' in connection_opt else 250

    def write_cache(self, message_type, block_number, data):
        cache_key_prefix = f"{self.chain}_{message_type}"

        if message_type == EntityType.BLOCK:
            self.redis_client.zremrangebyscore(cache_key_prefix, min=block_number, max=block_number)
            self.redis_client.zadd(cache_key_prefix, mapping={
                self.serialize(data): block_number
            })
            self.redis_client.zremrangebyscore(cache_key_prefix, min=0, max=block_number - self.cache_block_count)
        else:
            block_type_key = f"{cache_key_prefix}:{block_number}"
            self.redis_client.delete(block_type_key)
            self.redis_client.rpush(block_type_key, *[self.serialize(item) for item in data])
            self.clear_block_range(cache_key_prefix, block_number - self.cache_block_count)

    def clear_block_range(self, _prefix, min_block):
        keys = self.redis_client.scan_iter(f"{_prefix}:*")
        for item in keys:
            key = item.decode('ascii')
            delete_block_number = key.split(':')[-1]
            try:
                self.redis_client.keys(_prefix + ':*')
                if int(delete_block_number, 10) < min_block:
                    self.redis_client.delete(key)
            except Exception as e:
                # ignore redis key
                pass

    def read_cache(self, message_type, block_number) -> list:
        def get_index(item: dict):
            sort_key = f"{message_type}_index"
            return item.get(sort_key) if sort_key in item else 0

        cache_key_prefix = f"{self.chain}_{message_type}"

        if message_type == EntityType.BLOCK:
            result_list = self.redis_client.zrangebyscore(cache_key_prefix, min=block_number, max=block_number)
            decoded_list = [self.deserialize(item) for item in result_list]
            return sorted(decoded_list, key=get_index)
        else:
            block_type_key = f"{cache_key_prefix}:{block_number}"
            result_list = self.redis_client.lrange(block_type_key, 0, -1)
            if len(result_list) == 0:
                return []
            decoded_list = [self.deserialize(item) for item in result_list]
            return sorted(decoded_list, key=get_index)

    @staticmethod
    def serialize(message) -> str:
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
