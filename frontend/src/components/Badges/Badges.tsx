import React from 'react';
import * as Icon from 'react-feather';
import './Badges.css';

export function SpatialBadge() {
  return (
    <span className="badge badge-primary badge-pill">
      <Icon.MapPin className="feather-xs" /> Spatial
    </span>
  );
}

export function TemporalBadge() {
  return (
    <span className="badge badge-info badge-pill">
      <Icon.Calendar className="feather-xs" /> Temporal
    </span>
  );
}

interface BadgeGroupProps {
  className?: string;
}

export const BadgeGroup: React.FunctionComponent<BadgeGroupProps> = props => (
  <div
    className={`badge-group${props.className ? ` ${props.className} ` : ''}`}
  >
    {props.children}
  </div>
);
