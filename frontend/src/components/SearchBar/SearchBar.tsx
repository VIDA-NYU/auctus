import './SearchBar.css';
import React from 'react';
import * as Icon from 'react-feather';

interface SearchBarProps {
}

interface SearchBarState {
  value: string;
}

class SearchBar extends React.Component<SearchBarProps, SearchBarState> {

  constructor(props) {
    super(props);
    this.state = { value: '' };
    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
  }

  pristine() {
    return this.state.value === '';
  }

  handleChange(event) {
    this.setState({ value: event.target.value });
  }

  handleSubmit(event) {
    event.preventDefault();
    console.log(this.state.value);
  }

  render() {
    return (
      <form onSubmit={this.handleSubmit}>
        <div className="input-group SearchBar">
          <input
            type="text"
            className="form-control SearchBar-input"
            value={this.state.value}
            onChange={this.handleChange}
          />
          <div className="input-group-append">
            <span className={`input-group-text SearchBar-icon${this.pristine() ? '' : ' SearchBar-icon-active'}`}>
              <Icon.Search className="feather" />
            </span>
          </div>
        </div>
      </form>
    );
  }
}

export { SearchBar };
