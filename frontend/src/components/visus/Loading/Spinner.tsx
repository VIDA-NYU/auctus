import React from 'react';
import styled, {keyframes} from 'styled-components';

export const keyFrameInfiniteSpin = keyframes`
  from {transform: rotate(0deg)}
  to {transform: rotate(360deg)}
`;

export const SpinningSvg = styled.svg`
  animation-name: ${keyFrameInfiniteSpin};
  transition-property: transform;
  animation-iteration-count: infinite;
  animation-timing-function: linear;
`;

interface SpinnerProps {
  color?: string;
  speed?: string;
  gap?: number;
  thickness?: number;
  size?: string;
}

class Spinner extends React.PureComponent<SpinnerProps> {
  static defaultProps = {
    color: 'rgba(0,0,0,0.4)',
    gap: 4,
    thickness: 4,
    size: '1.0em',
  };

  speedSwitch(speed?: string) {
    if (speed === 'fast') {
      return 600;
    }
    if (speed === 'slow') {
      return 900;
    }
    return 750;
  }

  render() {
    return (
      <SpinningSvg
        height={this.props.size}
        width={this.props.size}
        style={{
          animationDuration: `${this.speedSwitch(this.props.speed)}ms`,
          marginBottom: 4,
        }}
        role="img"
        viewBox="0 0 32 32"
      >
        <circle
          role="presentation"
          cx={16}
          cy={16}
          r={14 - this.props.thickness! / 2}
          stroke={this.props.color}
          fill="none"
          strokeWidth={this.props.thickness}
          strokeDasharray={Math.PI * 2 * (11 - this.props.gap!)}
          strokeLinecap="round"
        />
      </SpinningSvg>
    );
  }
}

export {Spinner};
