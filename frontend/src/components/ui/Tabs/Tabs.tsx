import * as React from 'react';
import './Tabs.css';

class Tabs extends React.PureComponent {
  render() {
    return <ul className={'nav nav-tabs'}>{this.props.children}</ul>;
  }
}

interface TabProps {
  onClick: ((event: React.MouseEvent) => void) | undefined;
  selected: boolean;
}

class Tab extends React.PureComponent<TabProps> {
  render() {
    const tabClassName = this.props.selected ? 'nav-link active' : 'nav-link';
    return (
      <li className="nav-item">
        <button className={tabClassName} onClick={this.props.onClick}>
          {this.props.children}
        </button>
      </li>
    );
  }
}

class TabContent extends React.PureComponent {
  render() {
    return <div className="tab-content p-3">{this.props.children}</div>;
  }
}

interface TabPaneProps {
  id: string;
  active: boolean;
}

class TabPane extends React.PureComponent<TabPaneProps> {
  render() {
    const tabPaneClassName = this.props.active
      ? 'tab-pane fade show active'
      : 'tab-pane fade';
    return (
      <div className={tabPaneClassName} role="tabpanel" id={this.props.id}>
        {this.props.children}
      </div>
    );
  }
}

export {Tabs, Tab, TabContent, TabPane};
