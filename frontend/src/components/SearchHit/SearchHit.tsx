import * as React from 'react';
import * as Icon from 'react-feather';
import { BASE_PATH_URL } from '../../config';
import { formatSize } from '../../utils';
import { SearchResult } from '../../api/types';
import { ColumnsViewer } from './ColumnsViewer';

interface SearchHitProps {
  hit: SearchResult;
}

interface SearchHitState {
  hidden: boolean;
}

class SearchHit extends React.PureComponent<SearchHitProps, SearchHitState> {
  constructor(props: SearchHitProps) {
    super(props);
    this.state = {
      hidden: true,
    };
  }

  render() {
    const { hit } = this.props;
    return (
      <div className="card mb-4 shadow-sm">
        <div className="card-body d-flex flex-column">
          <span
            className="text-primary"
            style={{ fontSize: '1.1rem', fontFamily: 'Source Sans Pro' }}
          >
            {hit.metadata.name}
          </span>
          <span className="small text-muted">
            {formatSize(hit.metadata.size)}
          </span>
          <div className="row">
            <div className="col-md-12 mt-2">
              <span className="text-muted">{hit.metadata.description}</span>
              <div className="mt-2">
                <ColumnsViewer columns={hit.metadata.columns} />
              </div>
              <div className="mt-2">
                <a
                  className="btn btn-sm btn-outline-primary"
                  href={`${BASE_PATH_URL}/download/${hit.id}`}
                >
                  <Icon.Download className="feather" /> Download
                </a>
                <a
                  href={`${BASE_PATH_URL}/dataset/${hit.id}`}
                  className="btn btn-sm btn-outline-primary ml-2"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <Icon.Info className="feather" /> View Details
                </a>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export { SearchHit };
