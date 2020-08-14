import React from 'react';
import {Spinner} from './Spinner';

interface LoadingProps {
  message?: string;
}

class Loading extends React.PureComponent<LoadingProps> {
  render() {
    const msg = this.props.message || 'Loading...';
    return (
      <span>
        <Spinner /> {msg}
      </span>
    );
  }
}

export {Loading};
