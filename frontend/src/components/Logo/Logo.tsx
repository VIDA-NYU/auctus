import React from 'react';
//
// auctus-logo.min.svg is a minified file generated from auctus-logo.svg
// After updating source file, it can be with re-minified with:
//   npx svgo auctus-logo.svg -o auctus-logo.min.svg
//
import logo from './auctus-logo.min.svg';
import './Logo.css';

function VerticalLogo() {
  return (
    <div className="text-center logo-vertical">
      <img src={logo} className="d-block" alt="Auctus Logo" />
      <span className="d-block text-oswald">Auctus Dataset Search</span>
    </div>
  );
}

function HorizontalLogo(props: {onClick?: () => void}) {
  const style = props.onClick ? {cursor: 'pointer'} : undefined;
  return (
    <div
      className="d-inline text-center logo-horizontal"
      style={style}
      onClick={props.onClick}
    >
      <img src={logo} className="d-inline" alt="Auctus Logo" />
      <span className="d-inline text-oswald">Auctus</span>
    </div>
  );
}

function CenteredHorizontalLogo(props: {onClick?: () => void}) {
  return (
    <div className="logo-centered-horizontal">
      <HorizontalLogo onClick={props.onClick} />
    </div>
  );
}

export {VerticalLogo, HorizontalLogo, CenteredHorizontalLogo};
