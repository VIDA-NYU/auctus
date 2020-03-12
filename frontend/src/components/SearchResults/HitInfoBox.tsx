import React from 'react';
import * as Icon from 'react-feather';
import { SearchResult } from '../../api/types';
import { BASE_PATH_URL } from '../../config';
import { ColumnsViewer } from './ColumnsViewer';
import { formatSize } from '../../utils';

function SpecialDataTypes(props: { hit: SearchResult }) {
  const { hit } = props;
  const isTemporal = hit.metadata.columns
    .map(c => c.semantic_types)
    .flat()
    .filter(t => t === 'http://schema.org/DateTime')
    .length > 0;
  const isSpatial = hit.metadata.spatial_coverage && hit.metadata.spatial_coverage.length > 0;
  return <>
    {(isSpatial || isTemporal) &&
      <>
        <b>Data Types:</b>
        {isSpatial && <span className="badge badge-primary badge-pill ml-2">Spatial</span>}
        {isTemporal && <span className="badge badge-info badge-pill ml-2">Temporal</span>}
      </>
    }
  </>;
}

function DownloadButtons(props: { hit: SearchResult }) {
  const { hit } = props;
  return <>
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
  </>;
}

function HitInfoBox(props: { hit: SearchResult }) {
  const { hit } = props;
  return <>
    <div className="ml-2" style={{ maxWidth: 800 }}>
      <div className="card shadow-sm ml-2">
        <div className="card-body d-flex flex-column">
          <h4>{hit.metadata.name}</h4>
          <div className="mt-2">
            <b>Source:</b> {hit.metadata.source}
          </div>
          <div className="mt-2">
            <b>Description:</b> {hit.metadata.description}
          </div>
          <div className="mt-2">
            <SpecialDataTypes hit={hit} />
          </div>
          <div className="mt-2">
            <b>Columns:</b>&nbsp;
            <ColumnsViewer columns={hit.metadata.columns} maxLength={200} />
          </div>
          <div className="mt-2">
            <b>Size:</b> {formatSize(hit.metadata.size)}
          </div>
          <div className="mt-2">
            <DownloadButtons hit={hit} />
          </div>
        </div>
      </div>
    </div>
  </>;
}

export { HitInfoBox };