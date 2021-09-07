import * as React from 'react';
import styled from 'styled-components';
import './card.css';

interface CardProps {
  title: string;
  className?: string;
  style?: React.CSSProperties;
}

class Card extends React.PureComponent<CardProps> {
  render() {
    const cardClassName = this.props.className
      ? 'card ' + this.props.className
      : 'card';
    return (
      <div className={cardClassName} style={this.props.style}>
        <div className="card-body">
          {this.props.title ? (
            <h5 className="card-title">{this.props.title}</h5>
          ) : (
            ''
          )}
          {this.props.children}
        </div>
      </div>
    );
  }
}

interface CardShadowProps {
  className?: string;
  height?: string;
}

class CardShadow extends React.PureComponent<CardShadowProps> {
  render() {
    const cardClassName = this.props.className
      ? 'card-hover card card-attributes' + this.props.className
      : 'card-hover card card-attributes';
    return (
      <div
        className={cardClassName}
        style={{
          boxShadow: '1px 1px 5px #aaa',
          height: this.props.height ? this.props.height : '250px',
          padding: 0,
        }}
      >
        <div className="card-body">{this.props.children}</div>
      </div>
    );
  }
}

interface CardAttrFieldProps {
  textAlign?: string;
  width?: string;
  fontWeight?: string;
  padding?: string;
}

const CardAttrField = styled.div<CardAttrFieldProps>`
  font-weight: ${({fontWeight}) => fontWeight || 'normal'};
  text-align: ${({textAlign}) => textAlign || 'right'};
  width: ${({width}) => width || '110px'};
  padding: ${({padding}) => padding || '0 15px'};
`;

const CardAttrValue = styled.div`
  flex: 1;
  padding-right: 15px;
  overflow-wrap: break-word;
  word-wrap: break-word;
  word-break: break-word;
`;

export const CardButton = styled.div`
  display: flex;
  justify-content: center;
  flex-direction: column;
  text-align: center;
  height: 100%;
  cursor: pointer;
`;

export {Card, CardShadow, CardAttrField, CardAttrValue};
