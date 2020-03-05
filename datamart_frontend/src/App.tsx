import React from 'react';
import { VerticalLogo } from './Logo';
import { SearchBar } from './components/SearchBar/SearchBar';
import { AdvancedSearchBar } from './components/AdvancedSearchBar/AdvancedSearchBar';

function App() {
  return (
    <div className="App">
      <VerticalLogo />
      <SearchBar />
      <AdvancedSearchBar />
    </div>
  );
}

export { App };
