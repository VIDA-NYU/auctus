import pyreadstat

from datamart_materialize.utils import SimpleConverter


def spss_to_csv(source_filename, dest_fileobj):
    df, meta = pyreadstat.read_sav(source_filename)
    df.to_csv(dest_fileobj, index=False)


class SpssConverter(SimpleConverter):
    """Adapter converting a TSV file to CSV.
    """
    transform = staticmethod(spss_to_csv)
