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
  const lastUpdatedDate = new Date(hit.metadata.date);
  return (
    <div className="card shadow-sm">
      <div className="card-body d-flex flex-column">
        <h4>{hit.metadata.name}</h4>
        {infoBoxType === InfoBoxType.AUGMENTATION ? (
          <AugmentationOptions
            hit={hit}
            session={session}
            searchQuery={searchQuery}
          />
        ) : (
          <>
            <div className="mt-2">
              <b>ID:</b> {hit.id}
            </div>
            <div className="mt-2">
              <b>Source:</b> {hit.metadata.source}
            </div>
            <div className="mt-2">
              <b>Last Updated Date:</b> {lastUpdatedDate.toLocaleString()}
            </div>
            <Description hit={hit} label={true} />
            <DatasetTypes hit={hit} label={true} />
            <DatasetColumns
              columns={hit.metadata.columns}
              maxLength={200}
              label={true}
            />
            <div className="mt-2">
              <b>Rows:</b> {hit.metadata.nb_rows}
            </div>
            <div className="mt-2">
              <b>Size:</b> {formatSize(hit.metadata.size)}
            </div>
            <div className="mt-2">
              <DownloadVersions hit={hit} session={session} />
            </div>
            <div className="mt-2">
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

function DownloadVersions({
  hit,
  session,
}: {
  hit: SearchResult;
  session?: Session;
}) {
  const versions = [];
  const converters = hit.metadata.materialize?.convert;
  if (converters && converters.length > 0) {
    for (let i = 0; i <= converters.length; ++i) {
      const hitVersion: SearchResult = {
        ...hit,
        metadata: {
          ...hit.metadata,
          materialize: {
            ...hit.metadata.materialize,
            convert: converters.slice(0, i),
          },
        },
      };
      let label;
      if (i === 0) {
        label = 'Original';
      } else {
        const identifier = converters[i - 1].identifier;
        if (identifier === 'xls') {
          label = 'Converted from Excel';
        } else {
          label = "After '" + identifier + "' conversion";
        }
      }
      // Only the last entry can get obtained via GET (no converters disabled)
      const canPostOnly = i !== converters.length;
      versions.push({
        label,
        hit: hitVersion,
        canPostOnly,
      });
    }
  } else {
    versions.push({hit, canPostOnly: false});
  }
  return (
    <>
      {versions.map(({label, hit, canPostOnly}, idx) => (
        <DownloadButtons
          key={idx}
          label={label}
          hit={hit}
          session={session}
          canPostOnly={canPostOnly}
        />
      ))}
    </>
  );
}

export {HitInfoBox};
