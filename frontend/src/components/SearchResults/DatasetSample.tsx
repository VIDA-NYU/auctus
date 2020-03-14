import React from 'react';
import { useTable, Column } from 'react-table';

function Table(props: { columns: Column<string[]>[]; data: string[][] }) {
  const { columns, data } = props;
  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    rows,
    prepareRow,
  } = useTable({
    columns,
    data,
  });

  return (
    <table {...getTableProps()} className="table table-hover small">
      <thead>
        {headerGroups.map(headerGroup => (
          <tr {...headerGroup.getHeaderGroupProps()}>
            {headerGroup.headers.map(column => (
              <th scope="col" {...column.getHeaderProps()}>
                {column.render('Header')}
              </th>
            ))}
          </tr>
        ))}
      </thead>
      <tbody {...getTableBodyProps()}>
        {rows.map((row, i) => {
          prepareRow(row);
          return (
            <tr {...row.getRowProps()}>
              {row.cells.map(cell => {
                return <td {...cell.getCellProps()}>{cell.render('Cell')}</td>;
              })}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

interface TableSampleProps {
  data: string[][];
}

export function DatasetSample(props: TableSampleProps) {
  const { data } = props;

  const headers = data[0];
  const rows = data.slice(1, data.length - 1);

  const columns = headers.map((h, i) => ({
    Header: h,
    accessor: (row: string[]) => row[i],
  }));

  return (
    <div className="mt-2">
      <h6>Dataset Sample:</h6>
      <div className="mt-2" style={{ overflow: 'auto', maxHeight: '20rem' }}>
        <Table columns={columns} data={rows} />
      </div>
    </div>
  );
}
