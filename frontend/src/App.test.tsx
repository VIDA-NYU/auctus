import React from 'react';
import * as ReactDOM from 'react-dom';
import { render } from '@testing-library/react';
import { App } from './App';
import 'jest-canvas-mock';

test('renders main app', () => {
  const { getByText } = render(<App />);
  const linkElement = getByText(/Auctus/i);
  expect(linkElement).toBeInTheDocument();
});

it('renders without crashing', () => {
  const div = document.createElement('div');
  ReactDOM.render(<App />, div);
  ReactDOM.unmountComponentAtNode(div);
});
