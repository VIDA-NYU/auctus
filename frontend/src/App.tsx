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
import {
  DateFilter
} from './components/DateFilter/DateFilter';
import { RelatedFileFilter } from './components/RelatedFileFilter/RelatedFileFilter';
import {
  GeoSpatialFilter
} from './components/GeoSpatialFilter/GeoSpatialFilter';
import { SearchResponse } from './api/types';
import { SearchHit } from './components/SearchHit/SearchHit';
import { FilterContainer } from './components/FilterContainer/FilterContainer';
import { Loading } from './components/visus/Loading/Loading';
import { SourceFilter } from './components/SourceFilter/SourceFilter';

enum SearchState {
  CLEAN,
  SEARCH_REQUESTING,
  SEARCH_SUCCESS,
  SEARCH_FAILED,
}

interface Filter {
  id: string;
  type: FilterType;
  component: JSX.Element;
  state?: api.FilterVariables;
}

interface AppState {
  query: string;
  filters: Filter[];
  searchState: SearchState;
  searchResponse?: SearchResponse;
  sources?: string[];
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
    if (this.state.filters.filter(f => f.state).length > 0) return true;
    if (this.state.sources && this.state.sources.length > 0) return true;
    return false;
  }

  updateFilterState(filterId: string, state: api.TemporalVariable | api.GeoSpatialVariable) {
    const filter = this.state.filters.find(f => f.id === filterId);
    if (filter) {
      filter.state = state;
      this.setState({ filters: [...this.state.filters] });
    } else {
      console.warn(
        `Requested to update filter state with id=[${filterId} which does not exist.]`
      );
    }
  }

  handleAddFilter(filterType: FilterType) {
    const filterId = generateRandomId();
    const filters = this.state.filters;
    filters.push({
      id: filterId,
      type: filterType,
      component: this.createFilterComponent(filterId, filterType),
    });
    this.setState({ filters: [...filters] });
  }

  createFilterComponent(filterId: string, filterType: FilterType) {
    switch (filterType) {
      case FilterType.TEMPORAL:
        return (
          <FilterContainer
            key={filterId}
            title="Temporal Filter"
            onClose={() => this.removeFilter(filterId)}
          >
            <DateFilter
              key={`datefilter-${filterId}`}
              onDateFilterChange={d => this.updateFilterState(filterId, d)}
            />
          </FilterContainer>
        );
      case FilterType.RELATED_FILE:
        return (
          <FilterContainer
            key={filterId}
            title="Related Dataset Filter"
            onClose={() => this.removeFilter(filterId)}
          >
            <RelatedFileFilter />
          </FilterContainer>
        );
      case FilterType.GEO_SPATIAL:
        return (
          <FilterContainer
            key={filterId}
            title="Geo-Spatial Filter"
            onClose={() => this.removeFilter(filterId)}
          >
            <GeoSpatialFilter
              key={`geospatialfilter-${filterId}`}
              onSelectCoordinates={c => this.updateFilterState(filterId, c)}
            />
          </FilterContainer>
        );
      case FilterType.SOURCE:
        return (
          <FilterContainer
            key={filterId}
            title="Source Filter"
            onClose={() => this.removeFilter(filterId)}
          >
            <SourceFilter
              key={`sourcefilter-${filterId}`}
              onSourcesChange={s => this.setState({sources: s}) }
            />
          </FilterContainer>
        );
      default:
        throw new Error(`Received not supported filter type=[${filterType}]`);
    }
  }

  async submitQuery() {
    if (this.validQuery()) {
      this.setState({ searchState: SearchState.SEARCH_REQUESTING });
      const validFilters = this.state.filters
        .map(f => f.state)
        .filter((f): f is api.FilterVariables => f !== undefined);
      api
        .search(this.state.query, validFilters, this.state.sources)
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

  renderFilters() {
    return this.state.filters.map(f => f.component);
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
            <div className="row" style={{ width: 780 }}>
              <div className="col-md-12">{this.renderFilters()}</div>
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
                {this.renderFilters()}
              </div>
            </div>
          )}
      </div>
    );
  }
}

export { App };
