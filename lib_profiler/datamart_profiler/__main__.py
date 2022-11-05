import argparse
import contextlib
import json
import logging
import pandas
import string
import sys
import tempfile

from datamart_profiler import process_dataset


logger = logging.getLogger('datamart_profiler.__main__')


materialize_req = 'datamart-materialize>=0.11,<1.0'
try:
    from datamart_materialize import __version__ as materialize_version
except Exception:
    detect_format_convert_to_csv = materialize_version = None
else:
    materialize_version = [int(c) for c in materialize_version.split('.')]
    if [0, 11] <= materialize_version < [1, 0]:
        from datamart_materialize.detect import detect_format_convert_to_csv
    else:
        print(
            "Warning: unsupported version of datamart-materialize",
            file=sys.stderr,
        )
        detect_format_convert_to_csv = None


SUFFIXES = {
    '': 1,
    'B': 1,
    'k': 1000,
    'M': 1000000,
    'G': 1000000000,
    'T': 1000000000000,
}


def parse_size(number):
    # Check for a suffix
    suffix = number[-1]
    if suffix not in string.digits:
        number = number[:-1]
    else:
        suffix = ''

    # Parse the number
    size = int(number.rstrip())

    # Multiply by suffix
    size *= SUFFIXES[suffix]

    return size


def main():
    parser = argparse.ArgumentParser('datamart_profiler')
    parser.add_argument('-v', action='count',
                        default=0, dest='verbosity',
                        help="augments verbosity level")
    parser.add_argument('--include-sample',
                        action='store_true', default=False,
                        help="include a few random rows to the result")
    parser.add_argument('--no-coverage',
                        action='store_false', default=True, dest='coverage',
                        help="don't compute data ranges (using k-means)")
    parser.add_argument('--plots',
                        action='store_true', default=False, dest='plots',
                        help="compute plots (in vega format)")
    parser.add_argument('--load-max-size', action='store', nargs=1,
                        help="target size of the data to be analyzed. The "
                             "data will be randomly sampled if it is bigger")
    parser.add_argument('file', nargs=1, help="file to profile")
    if detect_format_convert_to_csv is None:
        parser.add_argument(
            '--detect-format',
            action='store_true', default=False, dest='detect_format',
            help=argparse.SUPPRESS,
        )
        parser.add_argument(
            '--dont-detect-format',
            action='store_false', default=False, dest='detect_format',
            help=argparse.SUPPRESS,
        )
    else:
        parser.add_argument(
            '--detect-format',
            action='store_true', default=True, dest='detect_format',
            help=(
                "use datamart-materialize to detect and convert "
                + "non-CSV files"
            ),
        )
        parser.add_argument(
            '--dont-detect-format',
            action='store_false', default=True, dest='detect_format',
            help=(
                "don't use datamart-materialize to detect and convert "
                + "non-CSV files"
            ),
        )
    args = parser.parse_args()

    # Set up logging
    level = {
        0: logging.WARNING,
        1: logging.INFO,
    }.get(args.verbosity, logging.DEBUG)
    logging.basicConfig(level=level)

    # Check for datamart-geo
    try:
        from datamart_geo import GeoData
        geo_data = GeoData.from_local_cache()
    except ImportError:
        logger.info("datamart-geo not installed")
        geo_data = None
    except FileNotFoundError:
        logger.warning("datamart-geo is installed but no data is available")
        geo_data = None

    # Parse max size
    load_max_size = None
    if args.load_max_size:
        if args.load_max_size[0] in ('0', '-1', ''):
            load_max_size = float('inf')
        else:
            load_max_size = parse_size(args.load_max_size[0])

    input_file = args.file[0]
    materialize = {}

    with contextlib.ExitStack() as stack:
        if args.detect_format:
            if detect_format_convert_to_csv is None:
                print(
                    (
                        "Can't detect and convert non-CSV files, "
                        + "%s is not installed"
                    ) % materialize_req,
                    file=sys.stderr,
                )
                sys.exit(1)

            def convert_dataset(func, path):
                tmpfile = stack.enter_context(
                    tempfile.NamedTemporaryFile(
                        'w+',
                        newline='',
                        prefix='datamart_profiler_',
                    )
                )
                func(path, tmpfile)
                tmpfile.flush()
                return tmpfile.name

            logger.info("Doing format detection")
            input_file = detect_format_convert_to_csv(
                input_file,
                convert_dataset,
                materialize,
            )

        # Profile
        try:
            metadata = process_dataset(
                input_file,
                geo_data=geo_data,
                include_sample=args.include_sample,
                coverage=args.coverage,
                plots=args.plots,
                load_max_size=load_max_size,
            )
        except (pandas.errors.ParserError, UnicodeError):
            if detect_format_convert_to_csv is None:
                print(
                    "\nError parsing CSV file\n"
                    + "Tip: if you install datamart-materialize, I will try to"
                    + "convert other supported formats to CSV for you before"
                    + "profiling\n",
                    file=sys.stderr,
                )
                sys.exit(1)
            else:
                raise

    if materialize:
        metadata['materialize'] = materialize
    json.dump(metadata, sys.stdout, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
