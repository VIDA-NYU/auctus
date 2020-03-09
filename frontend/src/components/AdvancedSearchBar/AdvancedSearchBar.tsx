import './AdvancedSearchBar.css';
import React from 'react';
import * as Icon from 'react-feather';

class AdvancedSearchBar extends React.PureComponent {
  render() {
    return (
      <div className="AdvancedSearchBar">
        <span className="d-inline text-oswald AdvancedSearchBar-title">
          Advanced Search:{' '}
        </span>
        <div className="d-inline btn AdvancedSearchBar-item">
          <Icon.Calendar className="feather" />
          <span>Any Date</span>
          <Icon.ChevronDown className="feather" />
        </div>
        <div className="d-inline btn AdvancedSearchBar-item">
          <Icon.MapPin className="feather" />
          <span>Any Location</span>
          <Icon.ChevronDown className="feather" />
        </div>
        <div className="d-inline btn AdvancedSearchBar-item">
          <Icon.File className="feather" />
          <span>Related File</span>
          <Icon.ChevronDown className="feather" />
        </div>
        <div className="d-inline btn AdvancedSearchBar-item">
          <Icon.Database className="feather" />
          <span>Source</span>
          <Icon.ChevronDown className="feather" />
        </div>
      </div>
    );
  }
}

export { AdvancedSearchBar };
