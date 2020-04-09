import React from 'react';
import { SearchResult } from '../../api/types';
import { formatSize } from '../../utils';
import {
  Description,
  DataTypes,
  DownloadButtons,
  DatasetColumns,
  SpatialCoverage,
} from './Metadata';
import { DatasetSample } from './DatasetSample';

function HitInfoBox(props: { hit: SearchResult }) {
  const { hit } = props;
  return (
    <div className="ml-2" style={{ maxWidth: 800 }}>
      <div className="sticky-top" style={{ top: '1rem' }}>
        <div
          className="card shadow-sm ml-2"
          style={{
            maxHeight: '96vh',
            overflowY: 'scroll',
          }}
        >
          <div className="card-body d-flex flex-column">
            <h4>{hit.metadata.name}</h4>
            <div className="mt-2">
              <b>ID:</b> {hit.id}
            </div>
            <div className="mt-2">
              <b>Source:</b> {hit.metadata.source}
            </div>
            <Description hit={hit} label={true} />
            <DataTypes hit={hit} label={true} />
            <DatasetColumns
              columns={hit.metadata.columns}
              maxLength={200}
              label={true}
            />
            <div className="mt-2">
              <b>Size:</b> {formatSize(hit.metadata.size)}
            </div>
            <div className="mt-2">
              <DownloadButtons hit={hit} />
            </div>
            <div className="mt-2">
              <SpatialCoverage hit={hit} />
            </div>
            <div className="mt-2">
              <DatasetSample hit={hit} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export { HitInfoBox };
