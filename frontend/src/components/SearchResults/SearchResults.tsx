import React from 'react';
import * as Icon from 'react-feather';
import { SearchResponse, SearchResult } from '../../api/types';
import { SearchHit } from './SearchHit';
import { SearchState } from './SearchState';
import { Loading } from '../visus/Loading/Loading';
import { HitInfoBox } from './HitInfoBox';

interface SearchResultsProps {
  searchState: SearchState;
  searchResponse?: SearchResponse;
}

interface SearchResultsState {
  selectedHit?: SearchResult;
}

class SearchResults extends React.PureComponent<SearchResultsProps, SearchResultsState> {

  lastSearchResponse?: SearchResponse;

  constructor(props: SearchResultsProps) {
    super(props);
    this.state = {};
  }

  componentDidUpdate() {
    if(this.lastSearchResponse !== this.props.searchResponse) {
      this.setState({ selectedHit: undefined});
    }
    this.lastSearchResponse = this.props.searchResponse;
  }

  render() {
    const { searchResponse, searchState } = this.props;
    switch (searchState) {
      case SearchState.SEARCH_REQUESTING: {
        return (
          <Loading message="Searching..." />
        );
      }
      case SearchState.SEARCH_FAILED: {
        return (
          <>
            <Icon.XCircle className="feather" />
            &nbsp; An unexpected error occurred. Please try again later.
          </>
        );
      }
      case SearchState.SEARCH_SUCCESS: {
        if (!(searchResponse && searchResponse.results.length > 0)) {
          return (
            <>
              <Icon.AlertCircle className="feather text-primary" />
              &nbsp;No datasets found, please try another query.
            </>
          );
        }

        // TODO: Implement proper results pagination
        const page = 1;
        const k = 20;
        const currentHits = searchResponse.results.slice(
          (page - 1) * k,
          page * k
        );
        const { selectedHit } = this.state;
        return (
          <div className="d-flex flex-row">
            <div style={{ width: 750 }}>
              {currentHits.map((hit, idx) => (
                <SearchHit hit={hit} key={idx} onSearchHitExpand={(hit) => {
                  this.setState({ selectedHit: hit })
                  console.log(hit);
                }} />
              ))}
            </div>
            {selectedHit &&
              <HitInfoBox hit={selectedHit} />
            }
          </div>
        );
      }
      case SearchState.CLEAN:
      default: {
        return null;
      }
    }
  }
}

export { SearchResults };