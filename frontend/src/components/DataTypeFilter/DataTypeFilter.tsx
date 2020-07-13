import React from 'react';
import * as Icon from 'react-feather';

interface DataTypeFilterProps {
  datatypes: string[];
  checkedDataTypes?: string[];
  onDataTypeChange: (checkedDataTypes: string[]) => void;
}

class DataTypeFilter extends React.PureComponent<DataTypeFilterProps> {
  handleChange(event: React.ChangeEvent<HTMLInputElement>) {
    const input = event.currentTarget;
    let found = false;
    const checkedDataTypes = this.getCheckedDataTypes().filter(s => {
      if (s === input.value) {
        found = true;
        return false; // Remove it from checked (uncheck it)
      } else {
        return true;
      }
    });
    if (!found) {
      checkedDataTypes.push(input.value); // Add it to checked
    }
    this.props.onDataTypeChange(checkedDataTypes);
  }

  getCheckedDataTypes() {
    // If 'checkedDataTypes' prop is undefined, consider all data types checked
    return this.props.checkedDataTypes === undefined
      ? this.props.datatypes
      : this.props.checkedDataTypes;
  }

  setCheckedStateForAll(checked: boolean) {
    if (checked) {
      this.props.onDataTypeChange(this.props.datatypes);
    } else {
      this.props.onDataTypeChange([]);
    }
  }

  render() {
    const dataTypes: { [dataType: string]: boolean } = {};
    this.props.datatypes.forEach(dataType => {
      dataTypes[dataType] = false;
    });
    this.getCheckedDataTypes().forEach(dataType => {
      dataTypes[dataType] = true;
    });
    const dataTypesList = Object.entries(dataTypes);
    dataTypesList.sort((a, b) => {
      if (a[0] < b[0]) {
        return -1;
      } else if (a[0] > b[0]) {
        return 1;
      } else {
        return 0;
      }
    });
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
        {dataTypesList.map(([dataType, checked]) => (
          <div className="form-check ml-2" key={`div-${dataType}`}>
            <input
              className="form-check-input"
              type="checkbox"
              value={dataType}
              checked={checked}
              id={`check-box-${dataType}`}
              onChange={e => this.handleChange(e)}
            />
            <label
              className="form-check-label"
              htmlFor={`check-box-${dataType}`}
            >
              {dataType.charAt(0).toUpperCase() + dataType.slice(1)}
            </label>
          </div>
        ))}
      </>
    );
  }
}

export { DataTypeFilter };
