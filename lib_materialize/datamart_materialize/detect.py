import csv
from datetime import datetime
import logging
import zipfile

from datamart_profiler import parse_date
from datamart_profiler.core import count_rows_to_skip

from .common import skip_rows
from .excel import xlsx_to_csv
from .excel97 import xls_to_csv
from .parquet import parquet_to_csv
from .pivot import pivot_table
from .spss import spss_to_csv
from .stata import stata_to_csv
from .tsv import tsv_to_csv


logger = logging.getLogger(__name__)


DELIMITERS = ',\t;|'


def detect_format_convert_to_csv(dataset_path, convert_dataset, materialize):
    """Detect supported formats and convert to CSV.

    :param dataset_path: Input dataset to be processed.
    :param convert_dataset: Function wrapping the conversion, in charge of
        creating the new file and cleaning up the previous one for each
        conversion. Takes the conversion function (filename, unicode file
        object), runs it, and returns the new path.
    :param materialize: Materialization info to be updated with the applied
        conversions.
    """
    with open(dataset_path, 'rb') as fp:
        magic = fp.read(16)

    # Check for Excel XLSX file format (2007+)
    if magic[:4] == b'PK\x03\x04':
        try:
            zip = zipfile.ZipFile(dataset_path)
        except zipfile.BadZipFile:
            pass
        else:
            if any(info.filename.startswith('xl/') for info in zip.infolist()):
                # Update metadata
                logger.info("This is an Excel XLSX (2007+) file")
                materialize.setdefault('convert', []).append({'identifier': 'xlsx'})

                # Update file
                dataset_path = convert_dataset(xlsx_to_csv, dataset_path)

    # Check for Excel XLS file format (1997-2003)
    if magic[:8] == b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1':
        # Update metadata
        logger.info("This is an Excel XLS (1997-2003) file")
        materialize.setdefault('convert', []).append({'identifier': 'xls'})

        # Update file
        dataset_path = convert_dataset(xls_to_csv, dataset_path)

    # Check for Parquet file format
    if magic[:4] == b'PAR1':
        # Update metadata
        logger.info("This is a Parquet file")
        materialize.setdefault('convert', []).append({'identifier': 'parquet'})

        # Update file
        dataset_path = convert_dataset(parquet_to_csv, dataset_path)

    # Check for Stata file format
    if magic[:11] == b'<stata_dta>' or magic[:4] in (
        b'\x73\x01\x01\x00', b'\x73\x02\x01\x00',
        b'\x72\x01\x01\x00', b'\x72\x02\x01\x00',
        b'\x71\x01\x01\x01', b'\x71\x02\x01\x01',
    ):
        # Update metadata
        logger.info("This is a Stata file")
        materialize.setdefault('convert', []).append({'identifier': 'stata'})

        # Update file
        dataset_path = convert_dataset(stata_to_csv, dataset_path)

    # Check for SPSS file format
    if magic[:4] in (b'\xC1\xE2\xC3\xC9', b'$FL2', b'$FL3'):
        # Update metadata
        logger.info("This is an SPSS file")
        materialize.setdefault('convert', []).append({'identifier': 'spss'})

        # Update file
        dataset_path = convert_dataset(spss_to_csv, dataset_path)

    # Check for TSV file format
    with open(dataset_path, 'r') as fp:
        # Read at least 65kB and 3 lines, and at most 5MB
        sample = fp.read(65536)
        newlines = sample.count('\n')
        while newlines < 3 and len(sample) < 5242880:
            more = fp.read(65536)
            if not more:
                break
            sample += more
            newlines += more.count('\n')

        # Run the sniffer
        dialect = csv.get_dialect('excel')
        if newlines >= 3:
            try:
                dialect = csv.Sniffer().sniff(sample, DELIMITERS)
            except Exception as error:  # csv.Error, UnicodeDecodeError
                logger.warning("csv.Sniffer error: %s", error)
        else:
            logger.warning("Lines are too long to use csv.Sniffer")
    if getattr(dialect, 'delimiter', ',') != ',':
        # Update metadata
        logger.info("Detected separator is %r", dialect.delimiter)
        materialize.setdefault('convert', []).append({
            'identifier': 'tsv',
            'separator': dialect.delimiter,
        })

        # Update file
        dataset_path = convert_dataset(
            lambda s, d: tsv_to_csv(s, d, separator=dialect.delimiter),
            dataset_path,
        )

    # Check for non-data rows at the top of the file
    with open(dataset_path, 'r') as fp:
        non_data_rows = count_rows_to_skip(fp)
        if non_data_rows > 0:
            # Update metadata
            logger.info("Detected %d lines to skip", non_data_rows)
            materialize.setdefault('convert', []).append({
                'identifier': 'skip_rows',
                'nb_rows': non_data_rows,
            })

            # Update file
            dataset_path = convert_dataset(
                lambda s, d: skip_rows(s, d, nb_rows=non_data_rows),
                dataset_path,
            )

    # Check for pivoted temporal table
    with open(dataset_path, 'r') as fp:
        reader = csv.reader(fp)
        try:
            columns = next(iter(reader))
        except StopIteration:
            columns = []
    if len(columns) >= 3:
        # Look for dates
        non_dates = [
            i for i, name in enumerate(columns)
            if parse_date(name) is None
        ]

        # Look for years
        def is_year(name, max_year=datetime.utcnow().year + 2):
            if len(name) != 4:
                return False
            try:
                return 1900 <= int(name) <= max_year
            except ValueError:
                return False
        non_years = [
            i for i, name in enumerate(columns)
            if not is_year(name)
        ]

        # If there's enough matches, pivot
        non_matches = min([non_dates, non_years], key=len)
        if len(non_matches) <= max(2.0, 0.20 * len(columns)):
            date_label = 'year' if non_matches is non_years else 'date'

            # Update metadata
            logger.info("Detected pivoted table")
            materialize.setdefault('convert', []).append({
                'identifier': 'pivot',
                'except_columns': non_matches,
                'date_label': date_label,
            })

            # Update file
            dataset_path = convert_dataset(
                lambda path, dst: pivot_table(path, dst,
                                              non_matches, date_label),
                dataset_path,
            )

    return dataset_path
