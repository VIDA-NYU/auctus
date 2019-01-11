import csv
import xlrd


def xls_to_csv(source_filename, dest_fileobj):
    workbook = xlrd.open_workbook(source_filename)
    sheets = workbook.sheets()
    if len(sheets) != 1:
        raise ValueError("Excel workbook has %d sheets" % len(sheets))
    sheet, = sheets

    writer = csv.writer(dest_fileobj)
    for row_num in range(sheet.nrows):
        writer.writerow(sheet.row_values(row_num))
