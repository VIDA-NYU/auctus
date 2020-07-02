import React from 'react';
import { PersistentComponent } from '../visus/PersistentComponent/PersistentComponent';
import * as Icon from 'react-feather';

interface DataTypeFilterState {
  checked: {
    [datatype: string]: boolean;
  };
}

interface DataTypeFilterProps {
  datatypes: string[];
  onDataTypeChange: (datatypes?: string[]) => void;
}

class DataTypeFilter extends PersistentComponent<
  DataTypeFilterProps,
  DataTypeFilterState
> {
  datatypes: string[];

  constructor(props: DataTypeFilterProps) {
    super(props);
    const initialState: DataTypeFilterState = { checked: {} };
    this.datatypes = props.datatypes;
    this.datatypes.forEach(s => {
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

  notifyChange(state: DataTypeFilterState) {
    this.setState(state);
    const checkedDataTypes = Object.entries(state.checked)
      .filter(c => c[1] === true)
      .map(c => c[0]) as string[];
    this.props.onDataTypeChange(
      checkedDataTypes.length > 0 ? checkedDataTypes : undefined
    );
  }

  setCheckedStateForAll(checked: boolean) {
    const state: DataTypeFilterState = { checked: {} };
    this.datatypes.forEach(s => {
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
        {this.datatypes.map(datatype => (
          <div className="form-check ml-2" key={`div-${datatype}`}>
            <input
              className="form-check-input"
              type="checkbox"
              value={datatype}
              checked={this.state.checked[datatype]}
              id={`check-box-${datatype}`}
              onChange={e => this.handleChange(e)}
            />
            <label
              className="form-check-label"
              htmlFor={`check-box-${datatype}`}
            >
              {datatype}
            </label>
          </div>
        ))}
      </>
    );
  }
}

export { DataTypeFilter };
