# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


class EthReceipt(object):
    def __init__(self):
        self.transaction_hash = None
        self.transaction_index = None
        self.block_hash = None
        self.block_number = None
        self.cumulative_gas_used = None
        self.gas_used = None
        self.contract_address = None
        self.logs = []
        self.root = None
        self.status = None
        self.effective_gas_price = None

        self.blob_gas_used = None
        self.blob_gas_price = None

        self.transaction_type = None
        self.bloom = None
        self.block_timestamp = None
        self.from_address = None
        self.to_address = None
        self.log_count = None

        # OP Stack
        # l1_block_number -> input[57:72]
        # l1_timestamp -> input[41:56]
        self.deposit_nonce = None
        self.deposit_receipt_version = None
        self.l1_gas_used = None
        self.l1_gas_price = None
        self.l1_fee = None
        self.fee_scalar = None
        self.l1_blob_base_fee = None
        self.l1_base_fee_scalar = None
        self.l1_blob_base_fee_scalar = None
