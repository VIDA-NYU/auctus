import React from 'react';
import logo from './auctus-logo.svg';

function VerticalLogo() {
  return (
    <div className="text-center" style={{ marginTop: 100, marginBottom: 30 }}>
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

function HorizontalLogo() {
  return (
    <div className="d-inline text-center">
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

export { VerticalLogo, HorizontalLogo };
