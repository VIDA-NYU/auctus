import React from 'react';
import { SearchResult } from '../../api/types';
import { formatSize } from '../../utils';
import {
  Description,
  DataTypes,
  DownloadButtons,
  DatasetColumns,
} from './Metadata';
import { DatasetSample } from './DatasetSample';

function HitInfoBox(props: { hit: SearchResult }) {
  const { hit } = props;
  return (
    <>
      <div className="ml-2" style={{ maxWidth: 800 }}>
        <div className="card shadow-sm ml-2">
          <div className="card-body d-flex flex-column">
            <h4>{hit.metadata.name}</h4>
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
              <DatasetSample data={hit.sample} />
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

export { HitInfoBox };
