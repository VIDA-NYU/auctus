import React from 'react';
import { useTable, Column } from 'react-table';
import { SearchResult } from '../../api/types';
import {
  ColumnMetadata,
  NumericalDataVegaFormat,
  TemporalDataVegaFormat,
  CategoricalDataVegaFormat,
} from '../../api/types';
import './DatasetSample.css';
// import { VegaLite, createClassFromSpec } from 'react-vega'
import { VegaLite } from 'react-vega';
// import * as VegaLite from
import { TopLevelSpec as VlSpec } from 'vega-lite';

const classMapping: { [key: string]: string } = {
  text: 'semtype-text',
  boolean: 'semtype-boolean',
  enumeration: 'semtype-enumeration',
  identifier: 'semtype-identifier',
  latitude: 'semtype-latitude',
  longitude: 'semtype-longitude',
  datetime: 'semtype-datetime',
};

function typeName(type: string) {
  return type
    .replace('http://schema.org/', '')
    .replace('https://metadata.datadrivendiscovery.org/types/', '');
}

function SemanticTypeBadge(props: { type: string }) {
  const label = typeName(props.type);
  const semtypeClass = classMapping[label.toLowerCase()];
  const spanClass = semtypeClass
    ? `badge badge-pill semtype ${semtypeClass}`
    : 'badge badge-pill semtype';
  return <span className={spanClass}>{label}</span>;
}

function TypeBadges(props: { column: ColumnMetadata }) {
  return (
    <>
      <span className="badge badge-pill badge-primary">
        {typeName(props.column.structural_type)}
      </span>
      {props.column.semantic_types.map(c => (
        <SemanticTypeBadge type={c} key={`sem-type-badge-${c}`} />
      ))}
    </>
  );
}

interface TableProps {
  columns: Array<Column<string[]>>;
  data: string[][];
  hit: SearchResult;
}

function getEncoding(typePlot: string | undefined) {
  const yContent = {
    field: 'count',
    type: 'quantitative',
    title: null,
  };
  if (typePlot === 'histogram_numerical') {
    return {
      y: yContent,
      x: {
        title: null,
        bin: { binned: true },
        field: 'bin_start',
        type: 'quantitative',
        // axis: null,
      },
      x2: {
        field: 'bin_end',
      },
      tooltip: [
        { field: 'bin_start', title: 'start', type: 'quantitative' },
        { field: 'bin_end', title: 'end', type: 'quantitative' },
      ],
    };
  } else if (typePlot === 'histogram_temporal') {
    return {
      y: yContent,
      x: {
        title: null,
        bin: { binned: true },
        field: 'date_start',
        type: 'temporal',
        utc: true,
        // axis: null,
      },
      x2: {
        field: 'date_end',
      },
      tooltip: [
        { field: 'date_start', title: 'start', type: 'temporal' },
        { field: 'date_end', title: 'end', type: 'temporal' },
      ],
    };
  } else if (typePlot === 'histogram_categorical') {
    return {
      y: yContent,
      x: {
        title: null,
        bin: { binned: true },
        field: 'bin',
        type: 'ordinal',
        // axis: null,
      },
      tooltip: { field: 'bin', type: 'ordinal' },
    };
  } else {
    console.log('Unknown plot type ', typePlot);
    return;
  }
}

function getSpecification(
  data:
    | NumericalDataVegaFormat[]
    | TemporalDataVegaFormat[]
    | CategoricalDataVegaFormat[]
    | undefined,
  typePlot: string | undefined
) {
  return {
    width: '120',
    height: '120',
    data: { values: data },
    description: 'A simple bar chart with embedded data.',
    encoding: getEncoding(typePlot),
    mark: 'bar',
  };
}

function Table(props: TableProps) {
  const { columns, data, hit } = props;
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
        {headerGroups.map((headerGroup, i) => (
          <tr {...headerGroup.getHeaderGroupProps()}>
            {headerGroup.headers.map(column => (
              <th scope="col" {...column.getHeaderProps()}>
                {column.render('Header')}
              </th>
            ))}
          </tr>
        ))}
        {headerGroups.map(headerGroup => (
          <tr {...headerGroup.getHeaderGroupProps()}>
            {headerGroup.headers.map((column, i) => {
              const dataVega = hit.metadata.columns[i].plot?.data;
              return (
                <th scope="col" {...column.getHeaderProps()}>
                  <VegaLite
                    spec={
                      getSpecification(
                        dataVega,
                        hit.metadata.columns[i].plot?.type
                      ) as VlSpec
                    }
                    data={{ values: dataVega }}
                  />
                </th>
              );
            })}
          </tr>
        ))}
        {headerGroups.map(headerGroup => (
          <tr {...headerGroup.getHeaderGroupProps()}>
            {headerGroup.headers.map((column, i) => (
              <th scope="col" {...column.getHeaderProps()}>
                <TypeBadges column={hit.metadata.columns[i]} />
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
  hit: SearchResult;
}

export function DatasetSample(props: TableSampleProps) {
  const { hit } = props;

  const sample = hit.sample;
  const headers = sample[0];
  const rows = sample.slice(1, sample.length - 1);

  const columns = headers.map((h, i) => ({
    Header: h,
    accessor: (row: string[]) => row[i],
  }));

  return (
    <div className="mt-2">
      <h6>Dataset Sample:</h6>
      <div className="mt-2" style={{ overflow: 'auto', maxHeight: '20rem' }}>
        <Table columns={columns} data={rows} hit={hit} />
      </div>
    </div>
  );
}
