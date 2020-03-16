import React from 'react';
import * as Icon from 'react-feather';
import { BASE_PATH_URL } from '../../config';
import { SearchResult } from '../../api/types';
import { ColumnMetadata } from '../../api/types';
import { generateRandomId } from '../../utils';
import { GeoSpatialCoverageMap } from '../GeoSpatialCoverageMap/GeoSpatialCoverageMap';

export function SpatialCoverage(props: { hit: SearchResult }) {
  const { spatial_coverage } = props.hit.metadata;
  if (!spatial_coverage) {
    return <></>;
  }
  return (
    <>
      <h6>Spatial Coverage</h6>
      <span> This is the approximate spatial coverage of the data.</span>
      {spatial_coverage.map((s, i) => (
        <GeoSpatialCoverageMap key={`spatial-coverage-map-${i}`} coverage={s} />
      ))}
    </>
  );
}

export function DataTypes(props: { hit: SearchResult; label?: boolean }) {
  const { hit, label } = props;
  const isTemporal =
    hit.metadata.columns
      .map(c => c.semantic_types)
      .flat()
      .filter(t => t === 'http://schema.org/DateTime').length > 0;
  const isSpatial =
    hit.metadata.spatial_coverage && hit.metadata.spatial_coverage.length > 0;
  return (
    <>
      {(isSpatial || isTemporal) && (
        <div className="mt-2">
          {label && <b>Data Types: </b>}
          {isSpatial && (
            <span className="badge badge-primary badge-pill mr-2">
              <Icon.MapPin className="feather-xs" /> Spatial
            </span>
          )}
          {isTemporal && (
            <span className="badge badge-info badge-pill">
              <Icon.Calendar className="feather-xs" /> Temporal
            </span>
          )}
        </div>
      )}
    </>
  );
}

export function DownloadButtons(props: { hit: SearchResult }) {
  const { hit } = props;
  return (
    <>
      <div className="mt-2">
        <b>Download: </b>
        <a
          className="btn btn-sm btn-outline-primary ml-2"
          href={`${BASE_PATH_URL}/download/${hit.id}`}
        >
          <Icon.Download className="feather" /> CSV
        </a>
        <a
          className="btn btn-sm btn-outline-primary ml-2"
          href={`${BASE_PATH_URL}/download/${hit.id}?format=d3m`}
        >
          <Icon.Download className="feather" /> D3M
        </a>
      </div>
    </>
  );
}

export function Description(props: { hit: SearchResult; label?: boolean }) {
  const { description } = props.hit.metadata;
  const showLabel = props.label ? props.label : false;
  return (
    <div className="mt-2">
      {showLabel && <b>Description: </b>}
      {description ? (
        description
      ) : (
        <span className="text-muted">[No description]</span>
      )}
    </div>
  );
}

interface ColumnsViewerProps {
  columns: ColumnMetadata[];
  maxLength?: number;
  label?: boolean;
}

interface ColumnsViewerState {
  hidden: boolean;
}

export class DatasetColumns extends React.PureComponent<
  ColumnsViewerProps,
  ColumnsViewerState
> {
  id = generateRandomId();

  constructor(props: ColumnsViewerProps) {
    super(props);
    this.state = { hidden: true };
  }

  splitColumns(columns: Array<{ name: string }>) {
    const visibleColumns: string[] = [];
    const hiddenColumns: string[] = [];
    const maxLength = this.props.maxLength ? this.props.maxLength : 100;
    let characters = 0;
    columns.forEach(c => {
      if (characters + c.name.length > maxLength) {
        hiddenColumns.push(c.name);
      } else {
        visibleColumns.push(c.name);
        characters += c.name.length + 5; // add 5 extra to account for extra space of badges
      }
    });
    return { visibleColumns, hiddenColumns };
  }

  render() {
    const { visibleColumns, hiddenColumns } = this.splitColumns(
      this.props.columns
    );
    const showLabel = this.props.label ? this.props.label : false;
    return (
      <div className="mt-2">
        {showLabel && (
          <>
            <b>Columns:</b>&nbsp;
          </>
        )}
        {visibleColumns.map(cname => (
          <span
            key={`${this.id}-${cname}`}
            className="badge badge-pill badge-secondary mr-1"
          >
            {cname}
          </span>
        ))}
        {!this.state.hidden &&
          hiddenColumns.map(cname => (
            <span
              key={`${this.id}-${cname}`}
              className="badge badge-pill badge-secondary mr-1"
            >
              {cname}
            </span>
          ))}
        {hiddenColumns.length > 0 && (
          <button
            className="text-muted small"
            style={{
              cursor: 'pointer',
              textDecoration: 'underline',
              background: 'transparent',
              border: 0,
            }}
            onClick={() => this.setState({ hidden: !this.state.hidden })}
          >
            {this.state.hidden
              ? `Show ${hiddenColumns.length} more...`
              : 'Hide'}
          </button>
        )}
      </div>
    );
  }
}
