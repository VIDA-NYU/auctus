import React from 'react';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';
import './DateFilter.css';

interface DateFilterProps {
  className?: string;
}

class DateFilter extends React.PureComponent<DateFilterProps> {
  state = {
    startDate: undefined,
    endDate: undefined,
  };

  handleStartDateChange = (date: Date | null) => {
    this.setState({ startDate: date });
  };

  handleEndDateChange = (date: Date | null) => {
    this.setState({ endDate: date });
  };

  render() {
    return (
      <div
        className={`input-group${
          this.props.className ? ` ${this.props.className}` : ''
        }`}
      >
        <div className="d-inline">
          <span className="ml-2 mr-1">Start: </span>
          <DatePicker
            selected={this.state.startDate}
            onChange={this.handleStartDateChange}
          />
        </div>
        <div className="d-inline">
          <span className="ml-2 mr-1">End: </span>
          <DatePicker
            selected={this.state.endDate}
            onChange={this.handleEndDateChange}
          />
        </div>
      </div>
    );
  }
}

export { DateFilter };
