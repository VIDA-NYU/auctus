import React from 'react';
import * as Icon from 'react-feather';
import { SearchResponse } from '../../api/types';
import { SearchHit } from './SearchHit';
import { SearchState } from './SearchState';
import { Loading } from '../visus/Loading/Loading';

interface SearchResultsProps {
  searchState: SearchState;
  searchResponse?: SearchResponse;
}

function SearchResults(props: SearchResultsProps): React.ReactElement | null {
  const { searchResponse, searchState } = props;
  switch (searchState) {
    case SearchState.SEARCH_REQUESTING: {
      return (
        <div className="col-md-12">
          <Loading message="Searching..." />
        </div>
      );
    }
    case SearchState.SEARCH_FAILED: {
      return (
        <div className="col-md-12">
          <Icon.XCircle className="feather" />
          &nbsp; Search failed. Please try again later.
        </div>
      );
    }
    case SearchState.SEARCH_SUCCESS: {
      if (!(searchResponse && searchResponse.results.length > 0)) {
        return (
          <div className="col-md-12">
            <Icon.AlertCircle className="feather" />
            &nbsp; Sorry, no datasets found for you query.
          </div>
        );
      }

      // TODO: Implement proper results pagination
      const page = 1;
      const k = 20;
      const currentHits = searchResponse.results.slice(
        (page - 1) * k,
        page * k
      );
      return <>
        {currentHits.map((hit, idx) => (
          <div className="col-md-12" key={idx}>
            <SearchHit hit={hit} />
          </div>
        ))}
      </>;
    }
    case SearchState.CLEAN:
    default: {
      return null;
    }
  }
}

export { SearchResults };