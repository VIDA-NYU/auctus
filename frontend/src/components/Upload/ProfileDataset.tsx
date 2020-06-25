import React from 'react';
import * as Icon from 'react-feather';
import { ProfileData, ColumnMetadata, ProfilingStatus } from '../../api/types';
import { useTable, Column } from 'react-table';
import { Loading } from '../visus/Loading/Loading';

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
  const typeData = [
    'String',
    'Integer',
    'Float',
    'Categorical',
    'DateTime',
    'Text',
    'Boolean',
    'Real',
  ];
  return (
    <>
      <select
        className="bootstrap-select badge badge-pill badge-primary"
        // id="settings-max-time-unit"
        // style={{ maxWidth: '200px' }}
        value={typeName(props.column.structural_type)}
        // onChange={e => {
        //   this.updateDataTypes(element.name, e.target.value);
        // }}
      >
        {typeData.map(unit => (
          <option key={unit} value={unit}>
            {unit}
          </option>
        ))}
      </select>
      {/* <span className="badge badge-pill badge-primary">
          {typeName(props.column.structural_type)}
        </span> */}
      {props.column.semantic_types.map(c => (
        <SemanticTypeBadge type={c} key={`sem-type-badge-${c}`} />
      ))}
      <div>
        <Icon.PlusCircle className="feather" />
      </div>
    </>
  );
}

// Compact view.
function TableCompactDetailView(props: { tableProps: TableProps }) {
  const { columns, data, hit } = props.tableProps;
  const { getTableBodyProps, headerGroups, rows, prepareRow } = useTable({
    columns,
    data,
  });
  return (
    <>
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
                {hit && <TypeBadges column={hit.columns[i]} />}
              </th>
            ))}
          </tr>
        ))}
      </thead>
      <tbody {...getTableBodyProps()} style={{ height: 100 }}>
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
    </>
  );
}

interface TableProps {
  columns: Array<Column<string[]>>;
  data: string[][];
  hit: ProfileData | undefined;
}

function Table(props: TableProps) {
  const { columns, data } = props;
  const { getTableProps } = useTable({
    columns,
    data,
  });
  return (
    <table
      {...getTableProps()}
      className="table table-hover small"
      style={{ height: 100 }}
    >
      <TableCompactDetailView tableProps={props} />
    </table>
  );
}

interface ProfileDatasetProps {
  profilingStatus: ProfilingStatus;
  profiledData?: ProfileData;
  successProfiler?: boolean;
  failedProfiler?: string;
}

class ProfileDataset extends React.PureComponent<ProfileDatasetProps, {}> {
  renderErrorMessage(error?: string) {
    return (
      <>
        <div className="text-danger mt-2 mb-4">
          Unexpected error while profiling data {error && `(${error})`}
        </div>
      </>
    );
  }

  getStringArrays(text: string) {
    const lines = text.split('\n');
    const result = [];
    const headers = lines[0].split(',');
    result.push(headers);
    for (let i = 1; i < lines.length; i++) {
      const row = lines[i].split(',');
      result.push(row);
    }
    return result;
  }

  render() {
    const {
      profiledData,
      profilingStatus,
      successProfiler,
      failedProfiler,
    } = this.props;
    const sample = profiledData
      ? this.getStringArrays(profiledData.sample)
      : [];
    const headers = sample[0];
    const rows = sample.slice(1, sample.length - 1);
    const columns =
      profiledData &&
      headers.map((h, i) => ({
        Header: h,
        accessor: (row: string[]) => row[i],
      }));
    return (
      <>
        {successProfiler &&
          profilingStatus === ProfilingStatus.COMPLETE &&
          columns && (
            <div style={{ maxHeight: 300, minHeight: 200, overflow: 'auto' }}>
              <Table columns={columns} data={rows} hit={profiledData} />
            </div>
          )}
        {profilingStatus === ProfilingStatus.RUNNING && (
          <span className="mr-2">
            <Loading message={`Profiling CSV file ...`} />
          </span>
        )}
        {!successProfiler && failedProfiler && (
          <span className="mr-2">
            <>{this.renderErrorMessage(failedProfiler)}</>
          </span>
        )}
      </>
    );
  }
}

export { ProfileDataset };
