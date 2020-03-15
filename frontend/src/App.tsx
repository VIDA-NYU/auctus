import React from 'react';
import { generateRandomId } from './utils';
import * as api from './api/rest';
import { VerticalLogo, HorizontalLogo } from './Logo';
import {
  AdvancedSearchBar,
  FilterType,
} from './components/AdvancedSearchBar/AdvancedSearchBar';
import { DateFilter } from './components/DateFilter/DateFilter';
import { RelatedFileFilter } from './components/RelatedFileFilter/RelatedFileFilter';
import { GeoSpatialFilter } from './components/GeoSpatialFilter/GeoSpatialFilter';
import { FilterContainer } from './components/FilterContainer/FilterContainer';
import { SourceFilter } from './components/SourceFilter/SourceFilter';
import { SearchBar } from './components/SearchBar/SearchBar';
import { SearchState } from './components/SearchResults/SearchState';
import { SearchResults } from './components/SearchResults/SearchResults';
import {
  SearchResponse,
  FilterVariables,
  TemporalVariable,
  GeoSpatialVariable,
} from './api/types';

interface Filter {
  id: string;
  type: FilterType;
  component: JSX.Element;
  state?: FilterVariables | File | string[];
}

interface AppState {
  query: string;
  filters: Filter[];
  searchState: SearchState;
  searchResponse?: SearchResponse;
  searchQuery?: api.SearchQuery;
}

class App extends React.Component<{}, AppState> {
  constructor(props: AppState) {
    super(props);
    this.state = this.initialState();
  }

  initialState() {
    return {
      query: '',
      searchQuery: undefined,
      searchResponse: undefined,
      searchState: SearchState.CLEAN,
      filters: [],
    };
  }

  resetQuery() {
    this.setState(this.initialState());
  }

  removeFilter(filterId: string) {
    this.setState({
      filters: this.state.filters.filter(f => f.id !== filterId),
    });
  }

  validQuery() {
    if (this.state.query && this.state.query.length > 0) return true;
    if (this.state.filters.filter(f => f.state).length > 0) return true;
    return false;
  }

  updateFilterState(
    filterId: string,
    state: TemporalVariable | GeoSpatialVariable | File | string[]
  ) {
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
    const filters = this.state.filters;
    if (
      filterType === FilterType.RELATED_FILE &&
      filters.filter(f => f.type === FilterType.RELATED_FILE).length > 0
    ) {
      return;
    }
    if (
      filterType === FilterType.SOURCE &&
      filters.filter(f => f.type === FilterType.SOURCE).length > 0
    ) {
      return;
    }
    const filterId = generateRandomId();
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
            <RelatedFileFilter
              key={`relatedfilefilter-${filterId}`}
              onSelectedFileChange={f => this.updateFilterState(filterId, f)}
            />
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
              onSourcesChange={s => this.updateFilterState(filterId, s)}
            />
          </FilterContainer>
        );
      default:
        throw new Error(`Received not supported filter type=[${filterType}]`);
    }
  }

  async submitQuery() {
    if (this.validQuery()) {
      const filterVariables = this.state.filters
        .filter(f => f.type !== FilterType.RELATED_FILE)
        .filter(f => f.type !== FilterType.SOURCE)
        .filter(f => f && f.state)
        .map(f => f.state as FilterVariables);

      const files: File[] = this.state.filters
        .filter(f => f.type === FilterType.RELATED_FILE)
        .map(f => f.state as File);

      const sources: string[][] = this.state.filters
        .filter(f => f.type === FilterType.SOURCE)
        .map(f => f.state as string[]);

      const query: api.SearchQuery = {
        query: this.state.query,
        filters: filterVariables,
        sources: sources[0],
        file: files[0],
      };

      this.setState({
        searchQuery: query,
        searchState: SearchState.SEARCH_REQUESTING,
      });

      api
        .search(query)
        .then(response => {
          if (response.status === api.RequestResult.SUCCESS && response.data) {
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

  renderFilters() {
    return this.state.filters.map(f => f.component);
  }

  render() {
    return (
      <div className="container-fluid">
        {this.state.searchQuery ? (
          <>
            <div className="row">
              <div className="col-md">
                <div className="d-flex flex-row mt-4 mb-3">
                  <div>
                    <HorizontalLogo onClick={() => this.resetQuery()} />
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
            <div className="row">
              <div className="col-md-12">
                <SearchResults
                  searchQuery={this.state.searchQuery}
                  searchState={this.state.searchState}
                  searchResponse={this.state.searchResponse}
                />
              </div>
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
            <div style={{ maxWidth: 1000, margin: '1.5rem auto' }}>
              {this.renderFilters()}
            </div>
          </div>
        )}
      </div>
    );
  }
}

export { App };
