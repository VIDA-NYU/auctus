#!/usr/bin/env python3
import elasticsearch
import os
import sys


SIZE = 10000


def delete(datasets):
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    for dataset in datasets:
        es.delete('datamart', '_doc', dataset)


if __name__ == '__main__':
    delete(sys.argv[1:])
