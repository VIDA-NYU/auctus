import {PureComponent} from 'react';
import {shallowEqual} from '../../../utils';

const cache = new Map<string, {}>();

// Patch PureComponent type declaration so that we can access React internal
// variables. We disable eslint here because the declaration has to match the
// the declaration from @types/react package.
declare module 'react' {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  interface PureComponent<P = {}, S = {}, SS = any>
    extends React.Component<P, S, SS> {
    _reactInternalFiber: {
      key: string;
      type: {
        displayName: string;
        name: string;
      };
    };
  }
}

/**
 * This component uses the key provided to a component to generate a cache key for the data.
 * We chose to use key for the following reasons:
 * 1. React uses key to identify if the element associated with the component.
 *    In some cases this helps it to identify that two instance are the same, and avoid re-constructing the instance.
 *    It is expected that this strategy will help react to avoid destroying a component unnecessarily.
 * 2. React does some work to avoid siblings with the same key, This should provide some warnings when reusing a key.
 * 3. Since it is an internal from each component, it doesn't pollute the props of components.
 *
 */
export class PersistentComponent<
  TProps = {},
  TState = {}
> extends PureComponent<TProps, TState> {
  componentDidMount() {
    if (!this._reactInternalFiber.key) {
      console.warn(
        'When using PersistentComponent please provide the key prop'
      );
    }
    const cacheKey = this.getCacheKey();
    const previousState = cache.get(cacheKey);
    if (previousState && !shallowEqual(this.state, previousState)) {
      this.setState(previousState);
    }
  }

  componentWillUnmount() {
    const key = this.getCacheKey();
    cache.set(key, this.state);
  }

  private getCacheKey() {
    const name =
      this._reactInternalFiber.type.displayName ||
      this._reactInternalFiber.type.name;
    return `${name}-${this._reactInternalFiber.key}`;
  }
}
