import csv
import xlrd
import xlrd.sheet

from .common import UnsupportedConversion
from .utils import SimpleConverter


def xls_to_csv(source_filename, dest_fileobj):
    with xlrd.open_workbook(source_filename) as workbook:
        datemode = workbook.datemode
        sheets = workbook.sheets()
        if len(sheets) != 1:
            raise UnsupportedConversion(
                "Excel workbook has %d sheets" % len(sheets)
            )
        sheet, = sheets

        writer = csv.writer(dest_fileobj)
        for row_num in range(sheet.nrows):
            values = sheet.row_values(row_num)

            for col_num, cell_type in enumerate(sheet.row_types(row_num)):
                if cell_type == xlrd.sheet.XL_CELL_DATE:
                    # Decode dates into ISO-8601 strings
                    values[col_num] = xlrd.xldate_as_datetime(
                        values[col_num],
                        datemode,
                    ).isoformat()
                elif cell_type == xlrd.sheet.XL_CELL_NUMBER:
                    # Avoid forced decimal point on integers
                    values[col_num] = '{0:g}'.format(values[col_num])

            writer.writerow(values)


class Excel97Converter(SimpleConverter):
    """Adapter converting Excel files to CSV.
    """
    transform = staticmethod(xls_to_csv)
