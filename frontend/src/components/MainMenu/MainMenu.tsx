import * as React from 'react';
import * as Icon from 'react-feather';
import './MainMenu.css';
import { DropdownMenu } from '../ui/DropdownMenu/DropdownMenu';
import { Link as RouterLink } from 'react-router-dom';

function Link(props: { path: string; label: string; icon: Icon.Icon }) {
  return (
    <div className="menu-link">
      <RouterLink to={props.path}>
        <span className="text-oswald">
          <props.icon className="feather-lg mr-1" /> {props.label}
        </span>
      </RouterLink>
    </div>
  );
}

class MainMenu extends React.PureComponent {
  render() {
    return (
      <DropdownMenu>
        {({ active, onClick }) => (
          <>
            <div className="d-flex flex-column main-menu">
              <div className="d-flex justify-content-end" onClick={onClick}>
                <span style={{ cursor: 'pointer' }}>
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
                </div>
              )}
            </div>
          </>
        )}
      </DropdownMenu>
    );
  }
}

export { MainMenu };
