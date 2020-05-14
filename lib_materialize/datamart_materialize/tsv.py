import csv

from datamart_materialize.utils import SimpleConverter


def tsv_to_csv(source_filename, dest_fileobj):
    with open(source_filename, 'r') as src_fp:
        src = csv.reader(src_fp, delimiter='\t')
        dst = csv.writer(dest_fileobj)
        for line in src:
            dst.writerow(line)


class TsvConverter(SimpleConverter):
    """Adapter converting a TSV file to CSV.
    """
    transform = staticmethod(tsv_to_csv)
