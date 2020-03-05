import React from 'react';
import logo from './auctus-logo.svg';

function VerticalLogo() {
  return (
    <div className="text-center" style={{marginTop: 100}} >
      <img src={logo} className="d-block" style={{width: 190, margin: '0 auto'}} alt="Auctus Logo" />
      <span className="d-block text-oswald" style={{fontSize: '60px', lineHeight: '1', marginTop: -10}}>Auctus Datamart</span>
    </div>
  );
};

export { VerticalLogo };