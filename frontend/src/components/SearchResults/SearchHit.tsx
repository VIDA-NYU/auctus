import * as React from 'react';
import * as Icon from 'react-feather';
import { BASE_URL, API_URL } from '../../config';
import { formatSize } from '../../utils';
import { SearchResult } from '../../api/types';
import { Description, DataTypes, DatasetColumns } from './Metadata';
import { AugmentationOptions } from './AugmentationOptions';
import { SearchQuery } from '../../api/rest';

interface SearchHitProps {
  searchQuery: SearchQuery;
  hit: SearchResult;
  onSearchHitExpand: (hit: SearchResult) => void;
}

interface SearchHitState {
  hidden: boolean;
}

function DownloadViewDetails(props: { id: string }) {
  return (
    <div className="mt-2">
      <a
        className="btn btn-sm btn-outline-primary"
        href={`${API_URL}/download/${props.id}`}
      >
        <Icon.Download className="feather" /> Download
      </a>
      <a
        href={`${BASE_URL}/dataset/${props.id}`}
        className="btn btn-sm btn-outline-primary ml-2"
        target="_blank"
        rel="noopener noreferrer"
      >
        <Icon.Info className="feather" /> View Details
      </a>
    </div>
  );
}

function HitTitle(props: { hit: SearchResult }) {
  return (
    <span
      className="text-primary"
      style={{ fontSize: '1.2rem', fontFamily: 'Source Sans Pro' }}
    >
      {props.hit.metadata.name}{' '}
      <span className="small text-muted">
        ({formatSize(props.hit.metadata.size)})
      </span>
    </span>
  );
}

class SearchHit extends React.PureComponent<SearchHitProps, SearchHitState> {
  constructor(props: SearchHitProps) {
    super(props);
    this.state = {
      hidden: true,
    };
  }

  render() {
    const { hit, searchQuery } = this.props;
    return (
      <div className="card mb-4 shadow-sm d-flex flex-row">
        <div className="card-body d-flex flex-column">
          <HitTitle hit={hit} />
          <span className="small">{hit.metadata.source}</span>
          <Description hit={hit} label={false} />
          <DatasetColumns columns={hit.metadata.columns} label={false} />
          <DataTypes hit={hit} label={false} />
          <DownloadViewDetails id={hit.id} />
          <AugmentationOptions hit={hit} searchQuery={searchQuery} />
        </div>
        <div
          style={{ margin: 'auto 0', cursor: 'pointer' }}
          onClick={() => this.props.onSearchHitExpand(this.props.hit)}
        >
          <Icon.ChevronRight className="feather feather-lg" />
        </div>
      </div>
    );
  }
}

export { SearchHit };
