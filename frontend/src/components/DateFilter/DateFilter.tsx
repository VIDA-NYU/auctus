import React from 'react';
import DatePicker from 'react-datepicker';
import 'react-datepicker/dist/react-datepicker.css';
import './DateFilter.css';
import { TemporalVariable } from '../../api/types';
import { PersistentComponent } from '../visus/PersistentComponent/PersistentComponent';

interface DateFilterProps {
  className?: string;
  onDateFilterChange: (state: TemporalVariable) => void;
}

export interface DateFilterState {
  start?: Date;
  end?: Date;
}

class DateFilter extends PersistentComponent<DateFilterProps, DateFilterState> {
  constructor(props: DateFilterProps) {
    super(props);
    this.state = {};
  }

  onStartDateChange(date: Date | null) {
    const start = date ? date : undefined;
    this.setState({ start }, () => this.notifyDateChange());
  }

  onEndDateChange(date: Date | null) {
    const end = date ? date : undefined;
    this.setState({ end }, () => this.notifyDateChange());
  }

  notifyDateChange() {
    this.props.onDateFilterChange({
      type: 'temporal_variable',
      start: this.formatDate(this.state.start),
      end: this.formatDate(this.state.end),
    });
  }

  formatDate(date?: Date) {
    return date ? date.toISOString().substring(0, 10) : undefined;
  }

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
            selected={this.state.start}
            onChange={e => this.onStartDateChange(e)}
          />
        </div>
        <div className="d-inline">
          <span className="ml-2 mr-1">End: </span>
          <DatePicker
            selected={this.state.end}
            onChange={e => this.onEndDateChange(e)}
          />
        </div>
      </div>
    );
  }
}

export { DateFilter };
