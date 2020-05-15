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
  typeView: number;
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
        axis: null,
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
        axis: null,
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
        axis: null,
      },
      tooltip: { field: 'bin', type: 'ordinal' },
    };
  } else if (typePlot === 'histogram_text') {
    return {
      y: {
        field: 'bin',
        type: 'ordinal',
        title: null,
      },
      x: {
        title: null,
        field: 'count',
        type: 'quantitative',
        axis: null,
      },
      tooltip: [
        { field: 'bin', type: 'ordinal' },
        { field: 'count', type: 'quantitative' },
      ],
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
  const { columns, data, hit, typeView } = props;
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
              <th
                scope="col"
                {...column.getHeaderProps()}
                style={{ position: 'sticky', top: 0, background: '#eee' }}
              >
                {column.render('Header')}
              </th>
            ))}
          </tr>
        ))}
        {typeView === 2 &&
          headerGroups.map(headerGroup => (
            <tr {...headerGroup.getHeaderGroupProps()}>
              {headerGroup.headers.map((column, i) => {
                const dataVega = hit.metadata.columns[i].plot?.data;
                if (dataVega) {
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
                } else {
                  return (
                    <th
                      scope="col"
                      {...column.getHeaderProps()}
                      className="text-center"
                      style={{ verticalAlign: 'middle' }}
                    >
                      <p className="small">Nothing to show.</p>
                    </th>
                  );
                }
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
interface TableSampleState {
  typeView: number;
}

class DatasetSample extends React.PureComponent<
  TableSampleProps,
  TableSampleState
> {
  constructor(props: TableSampleProps) {
    super(props);
    this.state = { typeView: 1 };
  }
  updateTypeView(view: number) {
    this.setState({ typeView: view });
  }

  // export function DatasetSample(props: TableSampleProps) {
  render() {
    const { hit } = this.props;

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
        <div>
          <div
            className="btn-group btn-group-sm"
            role="group"
            aria-label="Basic example"
            style={{ float: 'initial', marginBottom: '-8px' }}
          >
            <button
              type="button"
              className={`btn btn-secondary ${
                this.state.typeView === 1 ? 'active' : ''
              }`}
              onClick={() => this.updateTypeView(1)}
            >
              Compact view
            </button>
            <button
              type="button"
              className={`btn btn-secondary ${
                this.state.typeView === 2 ? 'active' : ''
              }`}
              onClick={() => this.updateTypeView(2)}
            >
              Detail view
            </button>
            <button
              type="button"
              className={`btn btn-secondary ${
                this.state.typeView === 3 ? 'active' : ''
              }`}
              onClick={() => this.updateTypeView(3)}
            >
              Column view
            </button>
          </div>
          <div
            className="mt-2"
            style={{ overflowY: 'auto', maxHeight: '20rem' }}
          >
            <Table
              columns={columns}
              data={rows}
              hit={hit}
              typeView={this.state.typeView}
            />
          </div>
        </div>
      </div>
    );
  }
}

export { DatasetSample };
