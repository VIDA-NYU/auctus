import React from 'react';
import { useTable, Column } from 'react-table';
import { SearchResult } from '../../api/types';
import { ColumnMetadata } from '../../api/types';
import './DatasetSample.css';
import { VegaLite } from 'react-vega';
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
        field: 'bin',
        type: 'ordinal',
        axis: null,
        sort: { order: 'descending', field: 'count' },
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
        sort: { order: 'descending', field: 'count' },
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

function getSpecification(typePlot: string | undefined) {
  return {
    width: '120',
    height: '120',
    data: { name: 'values' },
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
      {typeView < 3 ? (
        <>
          {/* Compact and detail View */}
          <thead>
            {headerGroups.map((headerGroup, i) => (
              <tr {...headerGroup.getHeaderGroupProps()}>
                {headerGroup.headers.map((column, i) => (
                  <th
                    scope="col"
                    {...column.getHeaderProps()}
                    style={{
                      position: 'sticky',
                      top: '-1px',
                      background: '#eee',
                      zIndex: 1,
                    }}
                  >
                    {column.render('Header')}
                    <br />
                    <TypeBadges column={hit.metadata.columns[i]} />
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
          </thead>
          <tbody {...getTableBodyProps()}>
            {rows.map((row, i) => {
              prepareRow(row);
              return (
                <tr {...row.getRowProps()}>
                  {row.cells.map(cell => {
                    return (
                      <td {...cell.getCellProps()}>{cell.render('Cell')}</td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </>
      ) : (
        // Column View
        <tbody>
          {headerGroups[0].headers.map((column, i) => {
            const dataVega = hit.metadata.columns[i].plot?.data;
            const columName = (
              <td>
                <b>{column.render('Header')} </b>
              </td>
            );
            const columnTypeBadges = (
              <td>
                <TypeBadges column={hit.metadata.columns[i]} />{' '}
              </td>
            );
            const plotVega = dataVega ? (
              <td>
                <VegaLite
                  spec={
                    getSpecification(
                      hit.metadata.columns[i].plot?.type
                    ) as VlSpec
                  }
                  data={{ values: dataVega }}
                />
              </td>
            ) : (
              <td className="text-center" style={{ verticalAlign: 'middle' }}>
                <p className="small">Nothing to show.</p>
              </td>
            );
            const columnStatistics = (
              <td style={{ minWidth: 200 }}>
                <ul
                  style={{ listStyle: 'none', columnCount: 2, columnGap: 10 }}
                >
                  {hit.metadata.columns[i].num_distinct_values && (
                    <li>Unique Values</li>
                  )}
                  {hit.metadata.columns[i].stddev && <li>Std Deviation</li>}
                  {hit.metadata.columns[i].mean && <li>Mean</li>}
                  {hit.metadata.columns[i].num_distinct_values && (
                    <li>{hit.metadata.columns[i].num_distinct_values}</li>
                  )}
                  {hit.metadata.columns[i].stddev && (
                    <li>{hit.metadata.columns[i].stddev?.toFixed(2)}</li>
                  )}
                  {hit.metadata.columns[i].mean && (
                    <li>{hit.metadata.columns[i].mean?.toFixed(2)}</li>
                  )}
                </ul>
              </td>
            );
            return (
              <tr key={'column' + i} {...column.getHeaderProps()}>
                {columName}
                {columnTypeBadges}
                {plotVega}
                {columnStatistics}
              </tr>
            );
          })}
        </tbody>
      )}
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
