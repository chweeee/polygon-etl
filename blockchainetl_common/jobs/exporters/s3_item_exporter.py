# MIT License
#
# Copyright (c) 2020 Evgeny Medvedev, evge.medvedev@gmail.com
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

import json
import logging
import os
from collections import defaultdict

import boto3
from botocore.exceptions import ClientError


def build_block_bundles(items):
    blocks = defaultdict(list)
    transactions = defaultdict(list)
    logs = defaultdict(list)
    token_transfers = defaultdict(list)
    traces = defaultdict(list)
    for item in items:
        item_type = item.get('type')
        if item_type == 'block':
            blocks[item.get('number')].append(item)
        elif item_type == 'transaction':
            transactions[item.get('block_number')].append(item)
        elif item_type == 'log':
            logs[item.get('block_number')].append(item)
        elif item_type == 'token_transfer':
            token_transfers[item.get('block_number')].append(item)
        elif item_type == 'trace':
            traces[item.get('block_number')].append(item)
        else:
            logging.info(f'Skipping item with type {item_type}')

    block_bundles = []
    for block_number in sorted(blocks.keys()):
        if len(blocks[block_number]) != 1:
            raise ValueError(f'There must be a single block for a given block number, was {len(blocks[block_number])} for block number {block_number}')
        block_bundles.append({
            'block': blocks[block_number][0],
            'transactions': transactions[block_number],
            'logs': logs[block_number],
            'token_transfers': token_transfers[block_number],
            'traces': traces[block_number],
        })

    return block_bundles


class S3ItemExporter:

    def __init__(
            self,
            bucket,
            path='blocks',
            build_block_bundles_func=build_block_bundles):
        self.bucket = bucket
        self.path = normalize_path(path)
        self.build_block_bundles_func = build_block_bundles_func
        self.storage_client = boto3.client('s3')

    def open(self):
        pass

    def export_items(self, items):
        block_bundles = self.build_block_bundles_func(items)

        for block_bundle in block_bundles:
            block = block_bundle.get('block')
            if block is None:
                raise ValueError('block_bundle must include the block field')

            block_number = block.get('number')
            if block_number is None:
                raise ValueError('block_bundle must include the block.number field')

            # convert transactions data to csv and upload to s3
            field_data_to_csv(block_bundle, block_number, "transactions")
            upload_file_to_s3(self.storage_client, self.bucket, f'transactions_{block_number}.csv', f'transactions/{block_number}.csv')
            os.remove(f'transactions_{block_number}.csv')

            # convert logs data to csv and upload to s3
            field_data_to_csv(block_bundle, block_number, "logs")
            upload_file_to_s3(self.storage_client, self.bucket, f'logs_{block_number}.csv', f'logs/{block_number}.csv')
            os.remove(f'logs_{block_number}.csv')

            # convert token_transfers data to csv and upload to s3
            # field_data_to_csv(block_bundle, block_number, "token_transfers")
            # upload_file_to_s3(self.storage_client, self.bucket, f'token_transfers_{block_number}.csv', f'token_transfers/{block_number}.csv')
            # os.remove(f'token_transfers_{block_number}.csv')

            # convert traces data to csv and upload to s3
            # field_data_to_csv(block_bundle, block_number, "traces")
            # upload_file_to_s3(self.storage_client, self.bucket, f'traces_{block_number}.csv', f'traces/{block_number}.csv')
            # os.remove(f'traces_{block_number}.csv')

    def close(self):
        pass


def field_data_to_csv(block_bundle, block_number, field_name):
    field_data = block_bundle.get(field_name)
    if field_data is None:
        raise ValueError(f'block_bundle must include the {field_name} field')
    else:
        import csv
        filename = f"{field_name}_{block_number}.csv"
        with open(filename, "w", newline="") as f:
            if len(field_data) == 0:
                pass
            else:
                headers = field_data[0].keys()
                cw = csv.DictWriter(f, headers, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
                cw.writeheader()
                cw.writerows(field_data)


def upload_file_to_s3(client, bucket_name, local_file_path, object_file_name):
    try:
        client.upload_file(local_file_path, bucket_name, object_file_name)
        logging.info(f'Uploaded file s3://{bucket_name}/{object_file_name}')
    except ClientError as e:
        logging.error(e)


def normalize_path(p):
    if p is None:
        p = ''
    if p.startswith('/'):
        p = p[1:]
    if p.endswith('/'):
        p = p[:len(p) - 1]

    return p
