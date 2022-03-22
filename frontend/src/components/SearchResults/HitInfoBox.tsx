import React from 'react';
import {SearchResult, InfoBoxType, Session} from '../../api/types';
import {formatSize} from '../../utils';
import {
  Description,
  DatasetTypes,
  DownloadButtons,
  DatasetColumns,
  SpatialCoverage,
} from './Metadata';
import {DatasetSample} from './DatasetSample';
import {AugmentationOptions} from './AugmentationOptions';
import {SearchQuery} from '../../api/rest';

function HitInfoBox(props: {
  hit: SearchResult;
  searchQuery: SearchQuery;
  infoBoxType: InfoBoxType;
  session?: Session;
}) {
  const {hit, searchQuery, infoBoxType, session} = props;
  const lastUpdatedDate = hit.metadata.materialize.date
    ? new Date(hit.metadata.materialize.date)
    : null;
  return (
    <div className="card shadow-sm">
      <div className="card-body d-flex flex-column">
        <div className="row">
          <div className="col-sm-9">
            <h4>{hit.metadata.name} </h4>
          </div>
          <div className="col-sm-3 text-right">
            <DownloadButtons hit={hit} session={session} />
          </div>
        </div>
        {infoBoxType === InfoBoxType.AUGMENTATION ? (
          <AugmentationOptions
            hit={hit}
            session={session}
            searchQuery={searchQuery}
          />
        ) : (
          <>
            <div className="row">
              <div className="col-sm">
                <small>
                  <b>Source:</b>{' '}
                  {hit.metadata.source_url ? (
                    <a href={hit.metadata.source_url}>{hit.metadata.source}</a>
                  ) : (
                    hit.metadata.source
                  )}{' '}
                  <span>&#8212;</span>{' '}
                </small>
                {lastUpdatedDate !== null ? (
                  <small>
                    Last Updated Date:&nbsp;
                    {lastUpdatedDate.toLocaleString()}
                  </small>
                ) : null}
              </div>
            </div>
            <Description hit={hit} label={true} length={320} />
            <DatasetTypes hit={hit} label={true} />
            <DatasetColumns
              columns={hit.metadata.columns}
              maxLength={200}
              label={true}
            />
            <div className="mt-2">
              <b>Rows:</b> {hit.metadata.nb_rows}
              <span className="text-muted ml-4 mr-4">|</span>
              <b>Columns:</b> {hit.metadata.columns.length}
              <span className="text-muted ml-4 mr-4">|</span>
              <b>Size:</b> {formatSize(hit.metadata.size)}
            </div>
            <div className="mt-4">
              <SpatialCoverage hit={hit} />
            </div>
            <div className="mt-2">
              <DatasetSample hit={hit} />
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export {HitInfoBox};
