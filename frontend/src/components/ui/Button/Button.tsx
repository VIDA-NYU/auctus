import {Tooltip} from '@material-ui/core';
import React from 'react';
import {Spinner} from '../../visus/Loading/Spinner';
import './Button.css';

const SubmitButton = (props: {label: string; loading: boolean}) => (
  <button type="submit" className="btn btn-primary" disabled={props.loading}>
    {props.loading && (
      <span className="mr-2">
        <Spinner />
      </span>
    )}
    {props.label}
  </button>
);

function ButtonGroup(props: React.PropsWithChildren<{}>) {
  return <div className="button-group">{props.children}</div>;
}

function LinkButton(
  props: React.PropsWithChildren<{href: string; message?: string}>
) {
  return (
    <Tooltip
      title={props.message === undefined ? '' : props.message}
      placement="top"
      arrow
    >
      <a className="btn btn-sm btn-outline-primary" href={props.href}>
        {props.children}
      </a>
    </Tooltip>
  );
}

export {SubmitButton, ButtonGroup, LinkButton};
