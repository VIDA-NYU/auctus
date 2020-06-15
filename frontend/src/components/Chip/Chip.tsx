import * as React from 'react';
import * as Icon from 'react-feather';
import './Chip.css';

interface ChipProps {
  label: string;
  isOpen: boolean;
  onClose?: () => void;
  onEdit?: () => void;
  icon?: Icon.Icon;
}

function Chip(props: ChipProps) {
  let classes = 'chip chip-outline';
  // chip-primary
  // chip-clickable
  if (props.onClose) {
    classes += ' chip-closeable';
  }
  const chipStatus = props.isOpen ? '(edit)' : '(close)';

  return (
    <div className={classes} tabIndex={0} role="button">
      {props.icon && (
        <div className="chip-icon">
          <props.icon className="feather" />
        </div>
      )}
      <span className="chip-label">
        {props.label}
        &nbsp;
        {props.onEdit && (
          <button className="btn-link" onClick={props.onEdit}>
            {chipStatus}
          </button>
        )}
      </span>
      {props.icon && (
        <div
          className="chip-btn-close"
          onClick={props.onClose}
          title="Remove this filter"
        >
          <Icon.Trash2 className="feather" />
        </div>
      )}
    </div>
  );
}

function ChipGroup(props: React.PropsWithChildren<{}>) {
  return <div className="chip-group">{props.children}</div>;
}

export { Chip, ChipGroup };
