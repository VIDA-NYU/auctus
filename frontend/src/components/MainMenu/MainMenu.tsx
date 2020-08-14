import * as React from 'react';
import * as Icon from 'react-feather';
import './MainMenu.css';
import {DropdownMenu} from '../ui/DropdownMenu/DropdownMenu';
import {Link as RouterLink} from 'react-router-dom';

function Link(props: {path: string; label: string; icon: Icon.Icon}) {
  const content = (
    <span className="text-oswald">
      <props.icon className="feather-lg mr-1" /> {props.label}
    </span>
  );
  return (
    <div className="menu-link">
      {props.path.startsWith('http:') || props.path.startsWith('https:') ? (
        <a href={props.path}>{content}</a>
      ) : (
        <RouterLink to={props.path}>{content}</RouterLink>
      )}
    </div>
  );
}

class MainMenu extends React.PureComponent {
  render() {
    return (
      <DropdownMenu>
        {({active, onClick}) => (
          <>
            <div className="d-flex flex-column main-menu">
              <div className="d-flex justify-content-end" onClick={onClick}>
                <span style={{cursor: 'pointer'}}>
                  <Icon.Menu />
                </span>
              </div>
              {active && (
                <div className="card shadow-sm card-menu mt-2">
                  <Link
                    icon={Icon.UploadCloud}
                    path="/upload"
                    label="Upload Dataset"
                  />
                  <Link
                    icon={Icon.BarChart2}
                    path="/statistics"
                    label="Dataset Statistics"
                  />
                  <Link
                    icon={Icon.BookOpen}
                    path="https://docs.auctus.vida-nyu.org/"
                    label="Documentation"
                  />
                </div>
              )}
            </div>
          </>
        )}
      </DropdownMenu>
    );
  }
}

export {MainMenu};
