import React from 'react';
//
// auctus-logo.min.svg is a minified file generated from auctus-logo.svg
// After updating source file, it can be with re-minified with:
//   npx svgo auctus-logo.svg -o auctus-logo.min.svg
//
import logo from './auctus-logo.min.svg';

function VerticalLogo() {
  return (
    <div className="text-center" style={{ paddingTop: 30, paddingBottom: 30 }}>
      <img
        src={logo}
        className="d-block"
        style={{ width: 190, margin: '0 auto' }}
        alt="Auctus Logo"
      />
      <span
        className="d-block text-oswald"
        style={{ fontSize: '60px', lineHeight: '1', marginTop: -10 }}
      >
        Auctus Datamart
      </span>
    </div>
  );
}

function HorizontalLogo(props: { onClick?: () => void }) {
  const style = props.onClick ? { cursor: 'pointer' } : undefined;
  return (
    <div className="d-inline text-center" style={style} onClick={props.onClick}>
      <img
        src={logo}
        className="d-inline"
        style={{ width: 80, margin: '0 auto' }}
        alt="Auctus Logo"
      />
      <span
        className="d-inline text-oswald"
        style={{ fontSize: '35px', lineHeight: '1', marginTop: -10 }}
      >
        Auctus
      </span>
    </div>
  );
}

function CenteredHorizontalLogo(props: { onClick?: () => void }) {
  const style: React.CSSProperties = {
    textAlign: 'center',
    paddingTop: 30,
    paddingBottom: 30,
  };
  return (
    <div style={style}>
      <HorizontalLogo onClick={props.onClick} />
    </div>
  );
}

export { VerticalLogo, HorizontalLogo, CenteredHorizontalLogo };
