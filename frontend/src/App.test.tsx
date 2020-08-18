import React from 'react';
import * as ReactDOM from 'react-dom';
import * as api from './api/rest';
import {render} from '@testing-library/react';
import {App} from './App';
import 'jest-canvas-mock';

beforeEach(() => {
  jest.spyOn(api, 'status').mockImplementation(() =>
    Promise.resolve({
      recent_discoveries: [],
      sources_counts: {
        remi: 23,
        fernando: 37,
      },
    })
  );
});

afterEach(() => jest.restoreAllMocks());

test('renders main app', () => {
  const {getByText} = render(<App />);
  const linkElement = getByText(/Auctus/i);
  expect(linkElement).toBeInTheDocument();
});

test('renders without crashing', () => {
  const div = document.createElement('div');
  ReactDOM.render(<App />, div);
  ReactDOM.unmountComponentAtNode(div);
});
