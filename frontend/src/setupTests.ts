// jest-dom adds custom jest matchers for asserting on DOM nodes.
// allows you to do things like:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom/extend-expect';

// Setup Jest canvas mock. This is required to test components that use canvas
// (e.g., Open Layers library requires this)
import 'jest-canvas-mock';
