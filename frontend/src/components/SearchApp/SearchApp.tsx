import React from 'react';
import {Link, match} from 'react-router-dom';
import {History, Location} from 'history';
import {generateRandomId} from '../../utils';
import * as api from '../../api/rest';
import {Session, AugmentationType} from '../../api/types';
import {VerticalLogo, HorizontalLogo} from '../Logo/Logo';
import {
  AdvancedSearchBar,
  FilterType,
} from '../AdvancedSearchBar/AdvancedSearchBar';
import {DateFilter} from '../DateFilter/DateFilter';
import {RelatedFileFilter} from '../RelatedFileFilter/RelatedFileFilter';
import {GeoSpatialFilter} from '../GeoSpatialFilter/GeoSpatialFilter';
import {FilterContainer} from '../FilterContainer/FilterContainer';
import {SourceFilter} from '../SourceFilter/SourceFilter';
import {SearchBar} from '../SearchBar/SearchBar';
import {SearchState} from '../SearchResults/SearchState';
import {SearchResults} from '../SearchResults/SearchResults';
import {DatasetTypeFilter} from '../DatasetTypeFilter/DatasetTypeFilter';
import {
  SearchResponse,
  FilterVariables,
  TemporalVariable,
  GeoSpatialVariable,
  RelatedFile,
} from '../../api/types';
import {Chip, ChipGroup} from '../Chip/Chip';
import * as Icon from 'react-feather';
import {aggregateResults} from '../../api/augmentation';
import {RelatedFileDialog} from '../RelatedFileFilter/RelatedFileDialog';

export interface Filter {
  id: string;
  type: FilterType;
  hidden: boolean;
  state?: FilterVariables | RelatedFile | string[];
}

interface SearchAppState {
  query: string;
  filters: Filter[];
  searchState: SearchState;
  searchResponse?: SearchResponse;
  searchQuery?: api.SearchQuery;
  sources: string[];
  session?: Session;
  selectedAugmentationType: AugmentationType;
  relatedFileDialog: RelatedFile | undefined;
  openDialog: boolean;
  currentPage?: number;
}

interface SearchAppProps {
  history: History;
  match: match;
  location: Location;
}

class SearchApp extends React.Component<SearchAppProps, SearchAppState> {
  constructor(props: SearchAppProps) {
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
      sources: api.DEFAULT_SOURCES,
      selectedAugmentationType: AugmentationType.JOIN,
      relatedFileDialog: undefined,
      openDialog: false,
    };
  }

  static filtersToQuery(state: SearchAppState): api.SearchQuery {
    const filterVariables = state.filters
      .filter(f => f.type !== FilterType.RELATED_FILE)
      .filter(f => f.type !== FilterType.SOURCE)
      .filter(f => f.type !== FilterType.DATA_TYPE)
      .filter(f => f && f.state)
      .map(f => f.state as FilterVariables);

    let relatedFile: RelatedFile = state.filters
      .filter(f => f.type === FilterType.RELATED_FILE)
      .map(f => f.state as RelatedFile)[0];
    if (state.session?.data_token) {
      relatedFile = {
        kind: 'localFile',
        name: 'session input',
        token: state.session.data_token,
      };
    }

    const sources: string[][] = state.filters
      .filter(f => f.type === FilterType.SOURCE)
      .map(f => f.state as string[]);

    const datasetTypes: string[][] = state.filters
      .filter(f => f.type === FilterType.DATA_TYPE)
      .map(f => f.state as string[]);

    const query: api.SearchQuery = {
      query: state.query,
      filters: filterVariables,
      sources: sources[0],
      datasetTypes: datasetTypes[0],
      relatedFile,
      augmentationType: relatedFile && state.selectedAugmentationType,
    };
    return query;
  }

  static queryToFilters(query: api.SearchQuery): {
    keywords: string;
    filters: Filter[];
  } {
    const filters: Filter[] = [];
    if (query.filters) {
      query.filters.forEach(v => {
        let type;
        if (v.type === 'geospatial_variable') {
          type = FilterType.GEO_SPATIAL;
        } else if (v.type === 'temporal_variable') {
          type = FilterType.TEMPORAL;
        } else {
          console.error('Unrecognized query variable ', v);
          return;
        }
        filters.push({
          id: generateRandomId(),
          type,
          hidden: false,
          state: v,
        });
      });
    }
    if (query.sources) {
      filters.push({
        id: generateRandomId(),
        type: FilterType.SOURCE,
        hidden: false,
        state: query.sources,
      });
    }
    if (query.datasetTypes) {
      filters.push({
        id: generateRandomId(),
        type: FilterType.DATA_TYPE,
        hidden: false,
        state: query.datasetTypes,
      });
    }
    if (query.relatedFile) {
      filters.push({
        id: generateRandomId(),
        type: FilterType.RELATED_FILE,
        hidden: false,
        state: query.relatedFile,
      });
    }
    return {keywords: query.query || '', filters};
  }

  static getDerivedStateFromProps(props: SearchAppProps) {
    const {location} = props;
    const params = new URLSearchParams(location.search);
    // Get session from URL
    const s = params.get('session');
    let session: Session | undefined = undefined;
    if (s) {
      session = JSON.parse(decodeURIComponent(s)) || undefined;
      if (session && session.session_id) {
        return {
          session: {...session, system_name: session.system_name || 'TA3'},
        };
      }
    }
    return {session: undefined};
  }

  updateSearchStateFromUrlParams(location: Location) {
    const params = new URLSearchParams(location.search);
    const q = params.get('q');
    if (q) {
      const query: api.SearchQuery = JSON.parse(decodeURIComponent(q));
      if (query) {
        // Update state to match
        let {keywords, filters} = SearchApp.queryToFilters(query);
        if (this.state.session?.data_token) {
          filters = filters.filter(f => f.type !== FilterType.RELATED_FILE);
        }
        this.setState(
          {
            query: keywords,
            filters,
          },
          () => {
            // Submit search
            this.fetchSearchResults(query);
          }
        );
        return;
      }
    }

    this.setState(this.initialState());
    this.fetchSources();
  }

  componentDidUpdate(prevProps: SearchAppProps) {
    if (this.state.searchQuery) {
      document.body.classList.add('searchresults');
    } else {
      document.body.classList.remove('searchresults');
    }
    const {location} = this.props;
    if (location !== prevProps.location) {
      this.updateSearchStateFromUrlParams(this.props.location);
    }
  }

  componentDidMount() {
    this.fetchSources();
    this.updateSearchStateFromUrlParams(this.props.location);
  }

  async fetchSources() {
    try {
      this.setState({sources: await api.sources()});
    } catch (e) {
      console.error('Unable to fetch list of sources:', e);
    }
  }

  removeFilter(filterId: string) {
    this.setState({
      filters: this.state.filters.filter(f => f.id !== filterId),
    });
  }

  validQuery() {
    const relatedFiles: RelatedFile[] = this.state.filters
      .filter(f => f.type === FilterType.RELATED_FILE)
      .map(f => f.state as RelatedFile);

    if (relatedFiles && relatedFiles.length > 0) {
      if (relatedFiles.filter(f => f && f.kind).length > 0) return true;
      return false;
    }
    if (this.state.filters.filter(f => f.state).length > 0) return true;
    if (this.state.query && this.state.query.length > 0) return true;
    return false;
  }

  updateFilterState(
    filterId: string,
    state?: TemporalVariable | GeoSpatialVariable | RelatedFile | string[]
  ) {
    this.setState(prevState => {
      let found = false;
      const filters = prevState.filters.map(filter => {
        if (filter.id === filterId) {
          found = true;
          return {...filter, state};
        } else {
          return filter;
        }
      });
      if (!found) {
        console.warn(
          `Requested to update filter state with id=[${filterId} which does not exist.]`
        );
      }
      return {filters};
    });
  }

  handleAddFilter(filterType: FilterType) {
    this.setState(prevState => {
      if (
        filterType === FilterType.RELATED_FILE ||
        filterType === FilterType.SOURCE ||
        filterType === FilterType.DATA_TYPE
      ) {
        // Can only have one of those
        if (prevState.filters.filter(f => f.type === filterType).length > 0) {
          return {filters: prevState.filters}; // No change
        }
      }
      const filterId = generateRandomId();
      const filter = {
        id: filterId,
        type: filterType,
        hidden: false,
      };
      return {filters: [filter, ...prevState.filters]};
    });
  }

  submitQuery() {
    if (this.validQuery()) {
      const query = SearchApp.filtersToQuery(this.state);

      // pushes the query into the URL, which will trigger fetching the search results
      const q = encodeURIComponent(JSON.stringify(query));
      let url = `${this.props.match.url}?q=${q}`;
      if (this.state.session) {
        const s = encodeURIComponent(JSON.stringify(this.state.session));
        url += `&session=${s}`;
      }
      this.props.history.push(url);
    }
  }

  fetchSearchResults(query: api.SearchQuery) {
    this.setState({
      searchQuery: query,
      searchState: SearchState.SEARCH_REQUESTING,
      filters: this.state.filters.map(f => ({...f, hidden: true})),
    });

    api
      .search(query)
      .then(response => {
        this.setState({
          searchState: SearchState.SEARCH_SUCCESS,
          searchResponse: {
            ...response,
            results: aggregateResults(response.results),
          },
        });
      })
      .catch(() => {
        this.setState({searchState: SearchState.SEARCH_FAILED});
      });
  }

  toggleFilter(filterId: string) {
    this.setState(() => {
      const filters = this.state.filters.map(f => {
        if (f.id === filterId) {
          return {...f, hidden: !f.hidden};
        } else {
          return f;
        }
      });
      return {filters};
    });
  }

  onSearchRelated(relatedFile: RelatedFile) {
    this.setState({
      relatedFileDialog: relatedFile,
      openDialog: true,
    });
  }

  runSearchRelatedQuery(
    updatedFilters: Filter[],
    updatedAugmentationType: AugmentationType
  ) {
    this.setState(() => {
      return {
        filters: updatedFilters,
        selectedAugmentationType: updatedAugmentationType,
        relatedFileDialog: undefined,
        openDialog: false,
      };
    }, this.submitQuery);
  }

  renderFilters() {
    const facets = this.state.searchResponse?.facets;
    const totalResults =
      this.state.searchResponse && this.state.searchResponse.total
        ? this.state.searchResponse.total
        : 0;
    return this.state.filters
      .filter(filter => !filter.hidden)
      .map(filter => {
        let title = undefined,
          component = undefined;
        switch (filter.type) {
          case FilterType.TEMPORAL:
            title = 'Temporal';
            component = (
              <DateFilter
                onDateFilterChange={d => this.updateFilterState(filter.id, d)}
                state={filter.state as TemporalVariable | undefined}
              />
            );
            break;
          case FilterType.RELATED_FILE:
            title = 'Related File';
            component = (
              <RelatedFileFilter
                onSelectedFileChange={f => this.updateFilterState(filter.id, f)}
                onAugmentationTypeChange={type =>
                  this.setState({selectedAugmentationType: type})
                }
                selectedAugmentationType={this.state.selectedAugmentationType}
                state={filter.state as RelatedFile | undefined}
              />
            );
            break;
          case FilterType.GEO_SPATIAL:
            title = 'Geo-Spatial';
            component = (
              <GeoSpatialFilter
                state={filter.state as GeoSpatialVariable | undefined}
                onSelectCoordinates={c => this.updateFilterState(filter.id, c)}
              />
            );
            break;
          case FilterType.SOURCE:
            title = 'Sources';
            component = (
              <SourceFilter
                sources={this.state.sources}
                checkedSources={filter.state as string[] | undefined}
                onSourcesChange={s => this.updateFilterState(filter.id, s)}
                facetBuckets={facets?.source}
                totalResults={totalResults}
              />
            );
            break;
          case FilterType.DATA_TYPE:
            title = 'Data Type';
            component = (
              <DatasetTypeFilter
                datasetTypes={api.DATASET_TYPES}
                checkedDatasetTypes={filter.state as string[] | undefined}
                onDatasetTypeChange={s => this.updateFilterState(filter.id, s)}
                facetBuckets={facets?.type}
                totalResults={totalResults}
              />
            );
            break;
          default:
            throw new Error(
              `Received not supported filter type=[${filter.type}]`
            );
        }
        return (
          <FilterContainer
            key={`filter-container-${filter.id}`}
            title={title}
            onClose={() => this.removeFilter(filter.id)}
          >
            {component}
          </FilterContainer>
        );
      });
  }

  renderCompactFilters() {
    const filters = this.state.filters.map(filter => {
      let icon = undefined,
        title = undefined;
      switch (filter.type) {
        case FilterType.TEMPORAL:
          title = 'Temporal';
          icon = Icon.Calendar;
          break;
        case FilterType.RELATED_FILE:
          title = 'Related File';
          icon = Icon.File;
          break;
        case FilterType.GEO_SPATIAL:
          title = 'Geo-Spatial';
          icon = Icon.MapPin;
          break;
        case FilterType.SOURCE:
          title = 'Sources';
          icon = Icon.Database;
          break;
        case FilterType.DATA_TYPE:
          title = 'Data Type';
          icon = Icon.Type;
          break;
        default:
          throw new Error(
            `Received not supported filter type=[${filter.type}]`
          );
      }
      return (
        <Chip
          key={`filter-chip-${filter.id}`}
          icon={icon}
          label={title}
          onClose={() => this.removeFilter(filter.id)}
          onEdit={() => this.toggleFilter(filter.id)}
        />
      );
    });
    if (this.state.session?.data_token) {
      filters.push(
        <Chip
          key={'filter-chip-session-file'}
          icon={Icon.File}
          label={`File From ${this.state.session.system_name}`}
        />
      );
    }
    return filters;
  }

  renderLandingPage(session?: Session) {
    return (
      <>
        <VerticalLogo />
        <SearchBar
          value={this.state.query}
          active={this.validQuery()}
          onQueryChange={q => this.setState({query: q})}
          onSubmitQuery={() => this.submitQuery()}
        />
        <AdvancedSearchBar
          onAddFilter={type => this.handleAddFilter(type)}
          relatedFileEnabled={!session?.data_token}
        />
        <div style={{maxWidth: 1000, margin: '1.5rem auto'}}>
          {this.renderFilters()}
        </div>
      </>
    );
  }

  handleCloseDialog() {
    this.setState({
      relatedFileDialog: undefined,
      openDialog: false,
    });
  }
  setCurrentPage(page: number) {
    this.setState({currentPage: page});
  }

  render() {
    const {searchQuery, searchState, searchResponse, session} = this.state;
    const compactFilters = this.renderCompactFilters();
    const expandedFilters = this.renderFilters();
    const hasExpandedFilters = expandedFilters.length > 0;
    const totalResultsText =
      this.state.currentPage && this.state.currentPage > 1
        ? 'Page ' + this.state.currentPage + ' of about '
        : 'About ';
    return (
      <>
        {searchQuery ? (
          <div
            className={`d-flex flex-column ${
              hasExpandedFilters ? 'container-vh-scroll' : 'container-vh-full'
            }`}
          >
            <div className="">
              <div className="d-flex flex-row mt-3">
                <div>
                  <Link
                    to={
                      session
                        ? `/?session=${JSON.stringify(this.state.session)}`
                        : '/'
                    }
                    style={{textDecoration: 'none'}}
                  >
                    <HorizontalLogo />
                  </Link>
                </div>
                <div className="ml-4">
                  <SearchBar
                    value={this.state.query}
                    active={this.validQuery()}
                    onQueryChange={q => this.setState({query: q})}
                    onSubmitQuery={() => this.submitQuery()}
                  />
                  <AdvancedSearchBar
                    onAddFilter={type => this.handleAddFilter(type)}
                    relatedFileEnabled={!session?.data_token}
                  />
                </div>
              </div>
              <div className="mt-2 mr-2 ml-2">
                <ChipGroup>{compactFilters}</ChipGroup>
              </div>
              {hasExpandedFilters && (
                <div className="mt-2 mr-2 ml-2">
                  <div className="mt-1 mb-1 ml-1" style={{maxWidth: 820}}>
                    {expandedFilters}
                  </div>
                </div>
              )}
              {this.state.searchResponse?.total !== undefined && (
                <div className="mt-2 mr-2 ml-2">
                  <span className="text-muted">
                    {totalResultsText}
                    {this.state.searchResponse.total.toLocaleString('en')}{' '}
                    results
                  </span>
                </div>
              )}
            </div>
            <div
              className={`${
                hasExpandedFilters ? '' : 'container-vh-full'
              } mt-2 mb-2`}
            >
              <SearchResults
                searchQuery={searchQuery}
                searchState={searchState}
                searchResponse={searchResponse}
                session={session}
                onSearchRelated={this.onSearchRelated.bind(this)}
                setCurrentPage={(page: number) => this.setCurrentPage(page)}
              />
            </div>
          </div>
        ) : (
          <div className="container-vh-scroll">
            {this.renderLandingPage(session)}
          </div>
        )}
        <RelatedFileDialog
          relatedFile={this.state.relatedFileDialog}
          openDialog={this.state.openDialog}
          filters={this.state.filters}
          handleCloseDialog={() => this.handleCloseDialog()}
          runSearchRelatedQuery={(
            filters: Filter[],
            augType: AugmentationType
          ) => this.runSearchRelatedQuery(filters, augType)}
        />
      </>
    );
  }
}

export {SearchApp};
