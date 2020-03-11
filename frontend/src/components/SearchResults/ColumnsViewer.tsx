import * as React from 'react';
import { generateRandomId } from '../../utils';
import { ColumnMetadata } from '../../api/types';

interface ColumnsViewerProps {
  columns: ColumnMetadata[];
}

interface ColumnsViewerState {
  hidden: boolean;
}

class ColumnsViewer extends React.PureComponent<
  ColumnsViewerProps,
  ColumnsViewerState
> {
  id = generateRandomId();

  constructor(props: ColumnsViewerProps) {
    super(props);
    this.state = { hidden: true };
  }

  splitColumns(columns: Array<{ name: string }>) {
    const visibleColumns: string[] = [];
    const hiddenColumns: string[] = [];
    const maxLength = 100;
    let characters = 0;
    columns.forEach(c => {
      if (characters + c.name.length > maxLength) {
        hiddenColumns.push(c.name);
      } else {
        visibleColumns.push(c.name);
        characters += c.name.length + 5; // add 5 extra to account for extra space of badges
      }
    });
    return { visibleColumns, hiddenColumns };
  }

  render() {
    const { visibleColumns, hiddenColumns } = this.splitColumns(
      this.props.columns
    );
    return (
      <>
        Columns:&nbsp;
        {visibleColumns.map(cname => (
          <span
            key={`${this.id}-${cname}`}
            className="badge badge-pill badge-secondary mr-1"
          >
            {cname}
          </span>
        ))}
        {!this.state.hidden &&
          hiddenColumns.map(cname => (
            <span
              key={`${this.id}-${cname}`}
              className="badge badge-pill badge-secondary mr-1"
            >
              {cname}
            </span>
          ))}
        {hiddenColumns.length > 0 && (
          <a
            className="text-muted small"
            style={{ cursor: 'pointer', textDecoration: 'underline' }}
            onClick={() => this.setState({ hidden: !this.state.hidden })}
          >
            {this.state.hidden
              ? `Show ${hiddenColumns.length} more...`
              : 'Hide'}
          </a>
        )}
      </>
    );
  }
}

export { ColumnsViewer };
