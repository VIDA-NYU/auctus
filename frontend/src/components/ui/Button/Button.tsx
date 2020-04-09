import React from 'react';
import { Spinner } from '../../visus/Loading/Spinner';

const SubmitButton = (props: { label: string; loading: boolean }) => (
  <button type="submit" className="btn btn-primary" disabled={props.loading}>
    {props.loading && (
      <span className="mr-2">
        <Spinner />
      </span>
    )}
    {props.label}
  </button>
);

export { SubmitButton };
