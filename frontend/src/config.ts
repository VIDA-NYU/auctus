/**
 * This function loads the "base_path" from the HTML. It allows us to
 * dynamically configure the base path for the application. It can useful
 * when configuring the system to run behind a proxy under a non-root path
 * such as: http://example.com/auctus/{all-paths}.
 *
 * This can be configured by adding an HTML meta-tag to the static HTML.
 *     <meta name="base_path" content="/auctus/">
 *     <meta name="api_path" content="https://api.auctus/v1/">
 */
function loadVariableFromHTML(name: string): string {
  const meta = document.getElementsByName(name)[0];
  let value: string | null = meta ? meta.getAttribute('content') : null;
  if (!value) {
    value = '';
  } else if(value.endsWith('/')) {
    value = value.substring(0, value.length - 1);
  }
  return value;
}

/*
 * During web development,
 * - the web server is started via "npm start" and it runs at localhost:3000;
 * - the API server should be running at the address configured in the "proxy"
 *   key from the package.json file in the project's root directory
 *
 * In the client code, we always send the API requests to address where the
 * page is being served from. In 'development' mode, create-react-app dev
 * server will proxy the requests to the appropriate backend running the REST
 * API. In production, the app is already served from the same server where
 * the REST API is server, so the requests will work seamlessly.
 */

const isDev = process.env.NODE_ENV === 'development';
let baseHost = `${window.location.host}`;
if (isDev && process.env.REACT_APP_API_HOST && process.env.REACT_APP_API_PORT) {
  baseHost = `${process.env.REACT_APP_API_HOST}:${process.env.REACT_APP_API_PORT}`;
} else if (isDev && process.env.REACT_APP_API_HOST) {
  baseHost = `${process.env.REACT_APP_API_HOST}`;
}

const BASE_URL: string = loadVariableFromHTML('base_url');
const API_URL = loadVariableFromHTML('api_url') || BASE_URL;

console.log('BASE_URL', BASE_URL);
console.log('API_URL', API_URL);

export { BASE_URL, API_URL };
