import React from 'react';
import { PersistentComponent } from '../visus/PersistentComponent/PersistentComponent';
import * as Icon from 'react-feather';

interface SourceFilterState {
  checked: {
    [source: string]: boolean;
  };
}

interface SourceFilterProps {
  sources: string[];
  onSourcesChange: (sources?: string[]) => void;
}

class SourceFilter extends PersistentComponent<
  SourceFilterProps,
  SourceFilterState
> {
  sources: string[];

  constructor(props: SourceFilterProps) {
    super(props);
    const initialState: SourceFilterState = { checked: {} };
    this.sources = props.sources;
    this.sources.forEach(s => {
      initialState.checked[s] = true;
    });
    this.state = initialState;
  }

  handleChange(event: React.ChangeEvent<HTMLInputElement>) {
    const input = event.currentTarget;
    this.state.checked[input.value] = !this.state.checked[input.value];
    const state = { checked: { ...this.state.checked } };
    this.notifyChange(state);
  }

  notifyChange(state: SourceFilterState) {
    this.setState(state);
    const checkedSources = Object.entries(state.checked)
      .filter(c => c[1] === true)
      .map(c => c[0]) as string[];
    this.props.onSourcesChange(
      checkedSources.length > 0 ? checkedSources : undefined
    );
  }

  setCheckedStateForAll(checked: boolean) {
    const state: SourceFilterState = { checked: {} };
    this.sources.forEach(s => {
      state.checked[s] = checked;
    });
    this.notifyChange(state);
  }

  render() {
    return (
      <>
        <div className="mb-1 mt-1">
          <button
            className="btn-link small ml-2"
            onClick={() => {
              this.setCheckedStateForAll(true);
            }}
          >
            <Icon.CheckSquare className="feather feather-sm mr-1" />
            Select all
          </button>
          <button
            className="btn-link small ml-2"
            onClick={() => {
              this.setCheckedStateForAll(false);
            }}
          >
            <Icon.Square className="feather feather-sm mr-1" />
            Unselect all
          </button>
        </div>
        {this.sources.map(source => (
          <div className="form-check ml-2" key={`div-${source}`}>
            <input
              className="form-check-input"
              type="checkbox"
              value={source}
              checked={this.state.checked[source]}
              id={`check-box-${source}`}
              onChange={e => this.handleChange(e)}
            />
            <label className="form-check-label" htmlFor={`check-box-${source}`}>
              {source}
            </label>
          </div>
        ))}
      </>
    );
  }
}

export { SourceFilter };
