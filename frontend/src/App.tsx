import React from 'react';
import {CenteredHorizontalLogo} from './components/Logo/Logo';
import {MainMenu} from './components/MainMenu/MainMenu';
import {BrowserRouter as Router, Link, Switch, Route} from 'react-router-dom';
import {Upload} from './components/Upload/Upload';
import {Statistics} from './components/Statistics/Statistics';
import {SearchApp} from './components/SearchApp/SearchApp';

class App extends React.Component {
  render() {
    return (
      <div className="container-vh-full">
        <Router>
          <Switch>
            <Route
              path="/upload"
              render={() => (
                <div className="container-vh-scroll">
                  <MainMenu />
                  <Link to="/" style={{textDecoration: 'none'}}>
                    <CenteredHorizontalLogo />
                  </Link>
                  <Upload />
                </div>
              )}
            />
            <Route
              path="/statistics"
              render={() => (
                <div className="container-vh-scroll">
                  <MainMenu />
                  <Link to="/" style={{textDecoration: 'none'}}>
                    <CenteredHorizontalLogo />
                  </Link>
                  <Statistics />
                </div>
              )}
            />
            <Route
              path="/"
              render={routeProps => (
                <>
                  <MainMenu />
                  <SearchApp
                    history={routeProps.history}
                    match={routeProps.match}
                    location={routeProps.location}
                  />
                </>
              )}
            />
          </Switch>
        </Router>
      </div>
    );
  }
}

export {App};
