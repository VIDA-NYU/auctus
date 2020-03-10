import './SearchBar.css';
import React from 'react';
import * as Icon from 'react-feather';

interface SearchBarProps {
  active: boolean;
  onQueryChange: (query: string) => void;
}

interface SearchBarState {
  value: string;
}

class SearchBar extends React.Component<SearchBarProps, SearchBarState> {
  constructor(props: SearchBarProps) {
    super(props);
    this.state = { value: '' };
    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
  }

  isActive() {
    return this.props.active || this.state.value !== '';
  }

  handleChange(event: React.ChangeEvent<HTMLInputElement>) {
    const query = event.target.value;
    this.setState({ value: query });
    this.props.onQueryChange(query);
  }

  handleSubmit(event: React.FormEvent<HTMLFormElement>) {
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
            <span
              className={`input-group-text SearchBar-icon${
                this.isActive() ? ' SearchBar-icon-active' : ''
              }`}
            >
              <Icon.Search className="feather" />
            </span>
          </div>
        </div>
      </form>
    );
  }
}

export { SearchBar };
