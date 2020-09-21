import csv

from datamart_materialize.utils import SimpleConverter


def tsv_to_csv(source_filename, dest_fileobj, separator='\t'):
    with open(source_filename, 'r') as src_fp:
        src = csv.reader(src_fp, delimiter=separator)
        dst = csv.writer(dest_fileobj)
        for line in src:
            dst.writerow(line)


class TsvConverter(SimpleConverter):
    """Adapter converting a TSV or other separated file to CSV.
    """
    def __init__(self, writer, separator='\t'):
        self.separator = separator
        super(TsvConverter, self).__init__(writer)

    def transform(self, source_filename, dest_fileobj):
        tsv_to_csv(source_filename, dest_fileobj, separator=self.separator)
