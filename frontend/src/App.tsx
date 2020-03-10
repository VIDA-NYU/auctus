import React from 'react';
import { generateRandomId } from './utils';
import * as api from './api/rest';
import * as Icon from 'react-feather';
import { VerticalLogo } from './Logo';
import { SearchBar } from './components/SearchBar/SearchBar';
import {
  AdvancedSearchBar,
  FilterType,
} from './components/AdvancedSearchBar/AdvancedSearchBar';
import { DateFilter } from './components/DateFilter/DateFilter';
import { RelatedFileFilter } from './components/RelatedFileFilter/RelatedFileFilter';
import { GeoSpatialFilter } from './components/GeoSpatialFilter/GeoSpatialFilter';

class FilterContainer extends React.PureComponent<{
  title: string;
  onClose: () => void;
}> {
  render() {
    return (
      <div style={{ maxWidth: 1000, margin: '1.5rem auto' }}>
        <div>
          <h6 className="d-inline">{this.props.title}</h6>
          <span
            onClick={() => this.props.onClose()}
            className="d-inline text-muted ml-1"
            style={{ cursor: 'pointer' }}
          >
            <Icon.X className="feather feather-lg" />
          </span>
        </div>
        <div className="d-block">{this.props.children}</div>
      </div>
    );
  }
}

interface Filter {
  id: string;
  component: JSX.Element;
}

interface AppState {
  query: string;
  filters: Filter[];
}

class App extends React.Component<{}, AppState> {
  constructor(props: AppState) {
    super(props);
    this.state = { filters: [], query: '' };
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
      case FilterType.GEOSPACIAL:
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
      const sr = await api.search(this.state.query);
      console.log(sr.data);
      if (sr.data) {
        sr.data.results.slice(10).map(r => console.log('id: ', r.id));
      }
    }
  }

  render() {
    return (
      <div className="App">
        <VerticalLogo />
        <SearchBar
          active={this.validQuery()}
          onQueryChange={q => this.setState({ query: q })}
          onSubmitQuery={() => this.submitQuery()}
        />
        <AdvancedSearchBar onAddFilter={type => this.handleAddFilter(type)} />
        {this.state.filters.map(f => f.component)}
      </div>
    );
  }
}

export { App };
