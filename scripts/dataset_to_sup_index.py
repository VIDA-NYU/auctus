#!/usr/bin/env python3
import elasticsearch
import logging
import os

from datamart_core.common import add_dataset_to_sup_index


def create_indices():
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )

    body = {
        'query': {
            'match_all': {}
        }
    }

    from_ = 0
    result = es.search(
        index='datamart',
        body=body,
        from_=from_,
        size=100,
        request_timeout=30
    )

    size_ = len(result['hits']['hits'])

    while size_ > 0:
        for hit in result['hits']['hits']:
            add_dataset_to_sup_index(es, hit['_id'], hit['_source'])

        from_ += size_
        result = es.search(
            index='datamart',
            body=body,
            from_=from_,
            size=100,
            request_timeout=30
        )
        size_ = len(result['hits']['hits'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    create_indices()
