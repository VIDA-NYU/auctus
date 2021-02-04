import * as React from 'react';
import {SearchFacet} from '../../api/types';

interface SimpleBarProps {
  facetBuckets: SearchFacet;
  keyname: string;
  totalResults: number;
}
class SimpleBar extends React.PureComponent<SimpleBarProps> {
  render() {
    const {facetBuckets, keyname, totalResults} = this.props;
    const rectangleWidth = 200;
    return (
      <svg width="400" height="20">
        <rect
          x="0"
          y="5"
          width={
            (facetBuckets.buckets[keyname] * rectangleWidth) / totalResults
          }
          height="14"
          style={{
            fill: '#C0C0C0',
            stroke: '#BEBEBE',
            strokeWidth: 1,
            opacity: 0.5,
          }}
        />
        <text x="3" y="17" fontFamily="Verdana" fontSize="9" fill="#707070">
          {facetBuckets.buckets[keyname]}
        </text>
      </svg>
    );
  }
}

export {SimpleBar};
