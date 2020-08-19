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

function LinkButton(props: React.PropsWithChildren<{href: string}>) {
  return (
    <a className="btn btn-sm btn-outline-primary" href={props.href}>
      {props.children}
    </a>
  );
}

export {SubmitButton, ButtonGroup, LinkButton};
