import React from 'react';
import * as Icon from 'react-feather';

class FilterContainer extends React.PureComponent<{
  title: string;
  isLandingPage: boolean;
  onClose: () => void;
  onCloseEditingMode: () => void;
}> {
  render() {
    return (
      <div className="mt-2 mb-3">
        <div>
          <h6 className="d-inline">{this.props.title}</h6>
          {!this.props.isLandingPage && (
            <h6 className="d-inline">
              <span className="chip-label-font">
                &nbsp;
                <button
                  className="btn-link"
                  title="Close editing mode"
                  onClick={() => this.props.onCloseEditingMode()}
                >
                  (minimize)
                </button>
              </span>
            </h6>
          )}
          <span
            onClick={() => this.props.onClose()}
            className="d-inline text-muted ml-1"
            style={{ cursor: 'pointer' }}
            title="Remove this filter"
          >
            <Icon.Trash2
              className="feather feather"
              style={{ marginBottom: '2px' }}
            />
          </span>
        </div>
        <div className="d-block">{this.props.children}</div>
      </div>
    );
  }
}

export { FilterContainer };
