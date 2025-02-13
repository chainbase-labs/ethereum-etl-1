import functools
import binascii
from collections import defaultdict

import rlp
from eth.constants import BLANK_ROOT_HASH
from eth.db.trie import HexaryTrie
import eth.vm.opcode_values

EMPTY_TRACES_ROOT = "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"


def decode_hex(value: str) -> bytes:
    non_prefixed = value.removeprefix("0x")
    ascii_hex = non_prefixed.encode("ascii")
    return binascii.unhexlify(ascii_hex)


def _get_call_type(t: str) -> bytes:
    if t == "suicide":
        t = "selfdestruct"

    return getattr(eth.vm.opcode_values, t.upper())


@functools.lru_cache(128)
def make_trie_root(items: [bytes]) -> bytes:
    trie = HexaryTrie({}, BLANK_ROOT_HASH)
    with trie.squash_changes() as memory_trie:
        for index, item in enumerate(items):
            index_key = rlp.encode(index, sedes=rlp.sedes.big_endian_int)
            memory_trie[index_key] = item
    return trie.root_hash


class CustomTrace(rlp.Serializable):
    fields = [
        ("from_address", rlp.sedes.Binary.fixed_length(20, allow_empty=True)),
        ("to_address", rlp.sedes.Binary.fixed_length(20, allow_empty=True)),
        ("gas", rlp.sedes.big_endian_int),
        ("gas_used", rlp.sedes.big_endian_int),
        ("input", rlp.sedes.binary),
        ("output", rlp.sedes.binary),
        ("value", rlp.sedes.big_endian_int),
        ("type", rlp.sedes.big_endian_int),
    ]


def _trace_to_rlp(trace) -> bytes:
    print(trace)
    custom_trace = CustomTrace(
        from_address=decode_hex(trace["from_address"]),
        to_address=decode_hex(trace["to_address"]) if trace["to_address"] else b"",
        gas=int(trace["gas"]),
        gas_used=int(trace["gas_used"]),
        input=decode_hex(trace["input"]) if trace["input"] else b"",
        output=decode_hex(trace["output"]) if trace["output"] else b"",
        value=int(trace["value"]) if trace["value"] else 0,
        type=_get_call_type(trace["raw_type"]),
    )
    return rlp.encode(custom_trace)


def generate_traces_root(traces: []) -> str:
    trace_root = make_trie_root(tuple(_trace_to_rlp(trace) for trace in traces))

    return trace_root.hex()


def get_custom_traces_root(traces) -> str:
    if not traces:
        return EMPTY_TRACES_ROOT

    return "0x" + generate_traces_root(traces)


def calculator_traces_root(traces) -> {}:
    traces_grouped_by_block = defaultdict(list)
    for trace in traces:
        traces_grouped_by_block[trace["block_number"]].append(trace)

    result = {}
    for block_number, block_traces in traces_grouped_by_block.items():
        result[block_number] = get_custom_traces_root(block_traces)

    # {block_number: traces_root}
    return result
