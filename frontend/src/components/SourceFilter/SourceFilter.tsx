import React from 'react';
import { PersistentComponent } from '../visus/PersistentComponent/PersistentComponent';
import { DEFAULT_SOURCES } from '../../api/rest';

interface SourceFilterState {
  checked: {
    [source: string]: boolean;
  };
}

interface SourceFilterProps {
  onSourcesChange: (sources: string[]) => void;
}

class SourceFilter extends PersistentComponent<SourceFilterProps, SourceFilterState> {
  constructor(props: SourceFilterProps) {
    super(props);
    const initialState: SourceFilterState = { checked: {} };
    DEFAULT_SOURCES.forEach(s => {
      initialState.checked[s] = true;
    });
    this.state = initialState;
  }

  handleChange(event: React.ChangeEvent<HTMLInputElement>) {
    const input = event.currentTarget;
    this.state.checked[input.value] = !this.state.checked[input.value];
    const checked = {...this.state.checked};
    this.setState({ checked });
    const checkedSources = Object.entries(checked).filter(c => c[1]===true).map(c => c[0]) as string[];
    this.props.onSourcesChange(checkedSources);
  }

  render() {
    return (
      <>
        {DEFAULT_SOURCES.map(source => (
          <div className="form-check ml-2" key={`div-${source}`}>
            <input
              className="form-check-input"
              type="checkbox"
              value={source}
              checked={this.state.checked[source]}
              id={`check-box-${source}`}
              onChange={(e)=> this.handleChange(e)}
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
