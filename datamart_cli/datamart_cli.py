import argparse
from datetime import datetime
import elasticsearch
import json
import logging
import os
import requests
import shutil
import sys


coordinator = None
es = None


def get_json(path):
    r = requests.get(coordinator + path,
                     headers={'Accept': 'application/json'})
    r.raise_for_status()
    return r.json()


def post_json(path, body):
    r = requests.post(coordinator + path,
                      headers={'Accept': 'application/json'},
                      json=body)
    r.raise_for_status()
    return r.json()


def dataset_list(args):
    # Get list of datasets currently in storage
    stored = get_json('/status')['storage']

    # Get all datasets from database
    hits = es.search(
        index='datamart',
        body={
            'query': {
                'match': {'kind': 'dataset'}
            }
        }
    )['hits']['hits']
    hits = {h['_id']: h['_source'] for h in hits}

    def print_dataset(id, path, doc):
        print(id)
        if path is not None:
            print("in storage: %r" % path)
        if doc is None:
            print("\t(no dataset record)")
        else:
            for k, v in doc.items():
                print("\t%s: %s" % (k, json.dumps(v)))

    # First show the stored ones
    for path, pair in stored.items():
        if pair is None:
            print_dataset('(unknown dataset)', path, {})
            continue
        id, _ = pair
        try:
            doc = hits.pop(id)
        except KeyError:
            print_dataset('(unknown dataset)', path, {})
        else:
            print_dataset(id, path, doc)
    # Then the other ones
    for id, doc in hits.items():
        print_dataset(id, None, doc)


def dataset_add(args):
    dataset_meta = dict(json.loads(sys.stdin.read()),
                        discoverer='cli',
                        kind='dataset',
                        date=datetime.utcnow().isoformat() + 'Z')
    dataset_id = es.index(
        'datamart',
        '_doc',
        dataset_meta,
    )['_id']
    post_json('/dataset_discovered?id=cli',
              {'id': dataset_id, 'meta': dataset_meta})
    if args.path:
        storage_path = get_json('/allocate_dataset?id=cli')['path']
        for fname in os.listdir(args.path):
            if os.path.isdir(os.path.join(args.path, fname)):
                copy = shutil.copytree
            else:
                copy = shutil.copy2
            copy(os.path.join(args.path, fname),
                 os.path.join(storage_path, fname))
        post_json('/dataset_downloaded?id=cli',
                  {'dataset_id': dataset_id, 'storage_path': storage_path})
    print(dataset_id)


def dataset_remove(args):
    TODO


def index_list(args):
    TODO


def index_process(args):
    TODO


def index_remove(args):
    TODO


def status(args):
    obj = get_json('/status')
    print("Discoverers:")
    for name, count in obj['discoverers']:
        print("\t%s (%d)" % (name, count))
    print("Ingesters:")
    for name, count in obj['ingesters']:
        print("\t%s (%d)" % (name, count))
    print("Recently discovered datasets:")
    for name, discoverer in obj['recent_discoveries']:
        print("\t%s (%s)" % (name, discoverer))
    print("Datasets in local storage:")
    for path, what in obj['storage'].items():
        if what is None:
            print("\t%r (allocated)")
        else:
            dataset_id, ingesters = what
            if ingesters:
                ingesters = " | " + ", ".join(ingesters)
            else:
                ingesters = ''
            print("\t%r %s%s" % (path, dataset_id, ingesters))


def query(args):
    TODO


def _dataset_cli(parser):
    subparser = parser.add_subparsers(title="dataset commands", metavar='')

    parser_list = subparser.add_parser('list',
                                       help="List known datasets")
    parser_list.set_defaults(func=dataset_list)

    parser_add = subparser.add_parser('add',
                                      help="Add a dataset, have it go through "
                                           "ingestion")
    parser_add.set_defaults(func=dataset_add)
    parser_add.add_argument('path', nargs=argparse.OPTIONAL,
                            help="Data to copy to local storage")

    parser_remove = subparser.add_parser('remove',
                                         help="Remove a known dataset")
    parser_remove.set_defaults(func=dataset_remove)


def _index_cli(parser):
    subparser = parser.add_subparsers(title="index commands", metavar='')

    parser_list = subparser.add_parser('list',
                                       help="List metadata records for a "
                                            "dataset")
    parser_list.set_defaults(func=index_list)

    parser_process = subparser.add_parser('process',
                                          help="Run all or specifics ingester "
                                               "on a dataset again")
    parser_process.set_defaults(func=index_process)

    parser_remove = subparser.add_parser('remove',
                                         help="Remove metadata records for a "
                                              "dataset")
    parser_remove.set_defaults(func=index_remove)


def main():
    parser = argparse.ArgumentParser(
        description="command-line interface to the DataMart system"
    )
    parser.add_argument('-v', '--verbose', action='count', default=1,
                        dest='verbosity', help="Augments verbosity level")
    subparsers = parser.add_subparsers(title="commands", metavar='')

    parser_dataset = subparsers.add_parser('dataset',
                                           help="Manipulate known datasets")
    _dataset_cli(parser_dataset)

    parser_index = subparsers.add_parser('index',
                                         help="Manipulate extracted metadata "
                                              "records")
    _index_cli(parser_index)

    parser_status = subparsers.add_parser('status',
                                          help="Print current system status")
    parser_status.set_defaults(func=status)

    parser_query = subparsers.add_parser('query',
                                         help="Do a query against the system")
    parser_query.set_defaults(func=query)

    args = parser.parse_args()
    levels = [logging.CRITICAL, logging.WARNING, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[min(args.verbosity, 3)],
                        format="%(asctime)s %(levelname)s: %(message)s")

    if getattr(args, 'func', None) is None:
        parser.print_help(sys.stderr)
        sys.exit(2)
    global coordinator
    coordinator = os.environ['COORDINATOR_URL']
    global es
    es = elasticsearch.Elasticsearch(
        os.environ['ELASTICSEARCH_HOSTS'].split(',')
    )
    args.func(args)


'''
dataset
    list
    add
    remove
index
    list
    process
    remove
status
query
'''


if __name__ == '__main__':
    main()
