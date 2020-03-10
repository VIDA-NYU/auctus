import React from 'react';
import { generateRandomId } from './utils';
import * as api from './api/rest';
import * as Icon from 'react-feather';
import { VerticalLogo, HorizontalLogo } from './Logo';
import { SearchBar } from './components/SearchBar/SearchBar';
import {
  AdvancedSearchBar,
  FilterType,
} from './components/AdvancedSearchBar/AdvancedSearchBar';
import { DateFilter } from './components/DateFilter/DateFilter';
import { RelatedFileFilter } from './components/RelatedFileFilter/RelatedFileFilter';
import { GeoSpatialFilter } from './components/GeoSpatialFilter/GeoSpatialFilter';
import { SearchResponse } from './api/types';
import { SearchHit } from './components/SearchHit/SearchHit';
import { FilterContainer } from './components/FilterContainer/FilterContainer';
import { Loading } from './components/visus/Loading/Loading';

enum SearchState {
  CLEAN,
  SEARCH_REQUESTING,
  SEARCH_SUCCESS,
  SEARCH_FAILED,
}

interface Filter {
  id: string;
  component: JSX.Element;
}

interface AppState {
  query: string;
  filters: Filter[];
  searchState: SearchState;
  searchResponse?: SearchResponse;
}

class App extends React.Component<{}, AppState> {
  constructor(props: AppState) {
    super(props);
    this.state = {
      filters: [],
      query: '',
      searchResponse: undefined,
      searchState: SearchState.CLEAN,
    };
  }

  removeFilter(filterId: string) {
    this.setState({
      filters: this.state.filters.filter(f => f.id !== filterId),
    });
  }

  validQuery() {
    if (this.state.query && this.state.query.length > 0) return true;
    return this.state.filters.length > 0; // TODO: check for valid values in filters
  }

  handleAddFilter(filterType: FilterType) {
    let filterComponent: JSX.Element | undefined = undefined;
    const filterId = generateRandomId();
    switch (filterType) {
      case FilterType.TEMPORAL:
        filterComponent = (
          <FilterContainer
            key={filterId}
            title="Temporal Filter"
            onClose={() => this.removeFilter(filterId)}
          >
            <DateFilter />
          </FilterContainer>
        );
        break;
      case FilterType.RELATED_FILE:
        filterComponent = (
          <FilterContainer
            key={filterId}
            title="Related Dataset Filter"
            onClose={() => this.removeFilter(filterId)}
          >
            <RelatedFileFilter />
          </FilterContainer>
        );
        break;
      case FilterType.GEO_SPATIAL:
        filterComponent = (
          <FilterContainer
            key={filterId}
            title="Geo-Spatial Filter"
            onClose={() => this.removeFilter(filterId)}
          >
            <GeoSpatialFilter />
          </FilterContainer>
        );
        break;
      default:
        console.error(`Received not supported filter type=[${filterType}]`);
    }
    if (filterComponent) {
      const filter = {
        id: filterId,
        component: filterComponent,
      };
      this.setState({ filters: [...this.state.filters, filter] });
    }
  }

  async submitQuery() {
    if (this.validQuery()) {
      this.setState({ searchState: SearchState.SEARCH_REQUESTING });
      api
        .search(this.state.query)
        .then(response => {
          if (response.status === api.ResquestResult.SUCCESS && response.data) {
            this.setState({
              searchState: SearchState.SEARCH_SUCCESS,
              searchResponse: {
                results: response.data.results,
              },
            });
          }
        })
        .catch(() => {
          this.setState({ searchState: SearchState.SEARCH_FAILED });
        });
    }
  }

  renderSearchResults() {
    switch (this.state.searchState) {
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
        const { searchResponse } = this.state;
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
        return currentHits.map((hit, idx) => (
          <div className="col-md-12" key={idx}>
            <SearchHit hit={hit} />
          </div>
        ));
      }
      default: {
        // search is triggered automatically when the data augmentation
        // tag is opened, so this shouldn't show up for long time
        return <div />;
      }
    }
  }

  render() {
    return (
      <div className="container-fluid">
        {this.state.searchState !== SearchState.CLEAN ? (
          <>
            <div className="row">
              <div className="col-md">
                <div className="d-flex flex-row mt-4 mb-3">
                  <div>
                    <HorizontalLogo />
                  </div>
                  <div className="ml-4">
                    <SearchBar
                      value={this.state.query}
                      active={this.validQuery()}
                      onQueryChange={q => this.setState({ query: q })}
                      onSubmitQuery={() => this.submitQuery()}
                    />
                    <AdvancedSearchBar
                      onAddFilter={type => this.handleAddFilter(type)}
                    />
                  </div>
                </div>
              </div>
            </div>
            <div className="row">
              <div className="col-md-12">
                {this.state.filters.map(f => f.component)}
              </div>
            </div>
            <div className="row" style={{ width: 780 }}>
              {this.renderSearchResults()}
            </div>
          </>
        ) : (
          <div>
            <VerticalLogo />
            <SearchBar
              value={this.state.query}
              active={this.validQuery()}
              onQueryChange={q => this.setState({ query: q })}
              onSubmitQuery={() => this.submitQuery()}
            />
            <AdvancedSearchBar
              onAddFilter={type => this.handleAddFilter(type)}
            />
            <div className="" style={{ maxWidth: 1000, margin: '1.5rem auto' }}>
              {this.state.filters.map(f => f.component)}
            </div>
          </div>
        )}
      </div>
    );
  }
}

export { App };
