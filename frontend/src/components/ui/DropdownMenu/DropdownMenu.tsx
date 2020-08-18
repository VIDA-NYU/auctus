import * as React from 'react';

interface Props {
  children: (api: {onClick: () => void; active: boolean}) => JSX.Element;
}

interface State {
  active: boolean;
}

class DropdownMenu extends React.Component<Props, State> {
  ref: HTMLDivElement | null = null;

  constructor(props: Props) {
    super(props);
    this.state = {active: false};
    this.toggleState = this.toggleState.bind(this);
    this.handleClickOutside = this.handleClickOutside.bind(this);
  }

  toggleState() {
    this.setState({active: !this.state.active});
  }

  handleClickOutside(e: MouseEvent) {
    if (this.ref && !this.ref.contains(e.target as Node)) {
      if (this.state.active) {
        this.toggleState();
      }
    }
  }

  componentDidMount() {
    document.addEventListener('mousedown', this.handleClickOutside, false);
  }

  componentWillUnmount() {
    document.removeEventListener('mousedown', this.handleClickOutside, false);
  }

  render() {
    return (
      <div ref={node => (this.ref = node)}>
        {this.props.children({
          onClick: this.toggleState,
          active: this.state.active,
        })}
      </div>
    );
  }
}

export {DropdownMenu};
