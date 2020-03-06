import React from 'react';
import * as Icon from 'react-feather';
import { VerticalLogo } from './Logo';
import { SearchBar } from './components/SearchBar/SearchBar';
import { AdvancedSearchBar } from './components/AdvancedSearchBar/AdvancedSearchBar';
import { DateFilter } from './components/DateFilter/DateFilter';
import { RelatedFileFilter } from './components/RelatedFileFilter/RelatedFileFilter';

class FilterContainer extends React.PureComponent<{ title: string, onClose: () => void }> {
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
        <div className="d-block" >
          {/* <div className="d-inline"> */}
            {this.props.children}
          {/* </div> */}
        </div>
      </div>
    );
  }
}

function App() {

  const filters = [
    (
      <FilterContainer key="temporal-filter" title="Temporal Filter" onClose={() => { }}>
        <DateFilter />
      </FilterContainer>
    ),
    (
      <FilterContainer key="dataset-filter" title="Related Dataset Filter" onClose={() => { }}>
        <RelatedFileFilter />
      </FilterContainer>
    ),
  ]

  return (
    <div className="App">
      <VerticalLogo />
      <SearchBar />
      <AdvancedSearchBar />
      {filters}
    </div>
  );
}

export { App };
