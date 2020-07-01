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

function TypeBadges(props: {
  column: ColumnMetadata;
  onEdit: (value: string, type: string) => void;
}) {
  const structuralTypes = [
    'Text',
    'Integer',
    'Float',
    'GeoCoordinates',
    'GeoShape',
    'MissingData',
  ];
  const semanticTypes = [
    'Enumeration',
    'DateTime',
    'latitude',
    'longitude',
    'Boolean',
    'Text',
    'AdministrativeArea',
    'identifier',
  ];
  return (
    <>
      <select
        className="bootstrap-select badge badge-pill badge-primary"
        value={typeName(props.column.structural_type)}
        onChange={e => {
          props.onEdit(e.target.value, 'structural');
        }}
      >
        {structuralTypes.map(unit => (
          <option key={unit} value={unit}>
            {unit}
          </option>
        ))}
      </select>
      {props.column.semantic_types.map(c => (
        <SemanticTypeBadge type={c} key={`sem-type-badge-${c}`} />
      ))}

      <div>
        <div className="dropdown">
          <button type="button" className="btn btn-link">
            <Icon.PlusCircle className="feather small" />
            <span className="small">Add</span>
            <span className="caret"></span>
          </button>
          <div className="dropdown-content">
            {semanticTypes
              .filter(
                unit =>
                  !props.column.semantic_types
                    .map(unit => typeName(unit))
                    .includes(unit)
              )
              .map(unit => (
                <div
                  key={unit}
                  className="menu-link"
                  onClick={() => props.onEdit(unit, 'semantic')}
                >
                  {unit}
                </div>
              ))}
          </div>
        </div>
      </div>
    </>
  );
}

// Compact view.

interface TableProps {
  columns: Array<Column<string[]>>;
  data: string[][];
  profiledData: ProfileData;
  onEdit: (value: string, type: string, column: ColumnMetadata) => void;
}

function Table(props: TableProps) {
  const { columns, data, profiledData } = props;
  const {
    headerGroups,
    rows,
    getTableProps,
    getTableBodyProps,
    prepareRow,
  } = useTable({
    columns,
    data,
  });
  return (
    <table
      {...getTableProps()}
      className="table table-hover small"
      style={{ height: 100 }}
    >
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
                {
                  <TypeBadges
                    column={profiledData.columns[i]}
                    onEdit={(value, type) => {
                      props.onEdit(value, type, profiledData.columns[i]);
                    }}
                  />
                }
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
    </table>
  );
}

interface ProfileDatasetProps {
  profilingStatus: ProfilingStatus;
  profiledData?: ProfileData;
  failedProfiler?: string;
  onEdit: (value: string, type: string, column: ColumnMetadata) => void;
}
interface DataTable {
  columns: Array<Column<string[]>>;
  rows: string[][];
}

class ProfileDataset extends React.PureComponent<ProfileDatasetProps, {}> {
  getSample(text: string): string[][] {
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

  getDataTable(profiledData: ProfileData): DataTable {
    const sample = this.getSample(profiledData.sample);
    const headers = sample[0];
    const rows = sample.slice(1, sample.length - 1);
    const columns = headers.map((h, i) => ({
      Header: h,
      accessor: (row: string[]) => row[i],
    }));
    return { columns, rows };
  }

  renderErrorMessage(error?: string) {
    return (
      <>
        <div className="text-danger mt-2 mb-4">
          Unexpected error while profiling data {error && `(${error})`}
        </div>
      </>
    );
  }

  render() {
    const { profiledData, profilingStatus, failedProfiler } = this.props;

    const dataTable = profiledData && this.getDataTable(profiledData);

    return (
      <>
        {profilingStatus === ProfilingStatus.SUCCESSED &&
          profiledData &&
          dataTable && (
            <div style={{ maxHeight: 300, minHeight: 200, overflow: 'auto' }}>
              <Table
                columns={dataTable.columns}
                data={dataTable.rows}
                profiledData={profiledData}
                onEdit={(value, type, updatedColumn) => {
                  this.props.onEdit(value, type, updatedColumn);
                }}
              />
            </div>
          )}
        {profilingStatus === ProfilingStatus.RUNNING && (
          <span className="mr-2">
            <Loading message={`Profiling CSV file ...`} />
          </span>
        )}
        {profilingStatus === ProfilingStatus.ERROR && (
          <span className="mr-2">
            <>{this.renderErrorMessage(failedProfiler)}</>
          </span>
        )}
      </>
    );
  }
}

export { ProfileDataset };
