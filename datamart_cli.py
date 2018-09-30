import argparse
import logging
import sys


def dataset_list(args):
    TODO


def dataset_add(args):
    TODO


def dataset_remove(args):
    TODO


def index_list(args):
    TODO


def index_process(args):
    TODO


def index_remove(args):
    TODO


def status(args):
    TODO


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
