import React from 'react';
import * as Icon from 'react-feather';
import {API_URL} from '../../config';
import {SearchResult, ColumnMetadata, Session} from '../../api/types';
import {RequestStatus, downloadToSession} from '../../api/rest';
import {generateRandomId} from '../../utils';
import {GeoSpatialCoverageMap} from '../GeoSpatialCoverageMap/GeoSpatialCoverageMap';
import {BadgeGroup, DatasetTypeBadge, ColumnBadge} from '../Badges/Badges';
import {ButtonGroup, LinkButton} from '../ui/Button/Button';

export function SpatialCoverage(props: {hit: SearchResult}) {
  const {spatial_coverage} = props.hit.metadata;
  if (!spatial_coverage) {
    return <></>;
  }
  return (
    <>
      <h6>Spatial Coverage</h6>
      <span> This is the approximate spatial coverage of the data.</span>
      {spatial_coverage.map((s, i) => (
        <GeoSpatialCoverageMap key={`spatial-coverage-map-${i}`} coverage={s} />
      ))}
    </>
  );
}

export function DatasetTypes(props: {hit: SearchResult; label?: boolean}) {
  const {hit, label} = props;
  const types = hit.metadata.types;
  if (!(types && types.length > 0)) {
    return null;
  }
  return (
    <div className="mt-2">
      <BadgeGroup>
        {label && <b>Data Types:</b>}
        {hit.metadata.types.map(t => (
          <DatasetTypeBadge type={t} key={`dt-badge-${hit.id}-${t}`} />
        ))}
      </BadgeGroup>
    </div>
  );
}

export class AddToSession extends React.PureComponent<
  {hit: SearchResult; session: Session; label?: string},
  {result?: RequestStatus}
> {
  constructor(props: {hit: SearchResult; session: Session; label?: string}) {
    super(props);
    this.state = {result: undefined};
  }

  render() {
    const {hit, session, label} = this.props;

    const clicked = (e: React.MouseEvent) => {
      e.preventDefault();
      downloadToSession(hit.id, session).then(
        () => this.setState({result: RequestStatus.SUCCESS}),
        () => this.setState({result: RequestStatus.ERROR})
      );
      this.setState({result: RequestStatus.IN_PROGRESS});
    };

    const {result} = this.state;
    if (result === undefined) {
      return (
        <>
          <b>{label}</b>
          <button
            className="btn btn-sm btn-outline-primary ml-2"
            onClick={clicked}
          >
            <Icon.Download className="feather" /> Add to {session.system_name}
          </button>
        </>
      );
    } else if (result === RequestStatus.IN_PROGRESS) {
      return (
        <button className="btn btn-sm btn-outline-primary ml-2 disabled">
          <Icon.Download className="feather" /> Adding to {session.system_name}
          ...
        </button>
      );
    } else if (result === RequestStatus.SUCCESS) {
      return (
        <button className="btn btn-sm btn-outline-primary ml-2 disabled">
          <Icon.Download className="feather" /> Added to {session.system_name}!
        </button>
      );
    } else if (result === RequestStatus.ERROR) {
      return (
        <button
          className="btn btn-sm btn-outline-primary ml-2"
          onClick={clicked}
        >
          <Icon.Download className="feather" /> Error adding to session
        </button>
      );
    } else {
      throw new Error('Invalid RequestStatus');
    }
  }
}

export function DownloadButton(props: {
  hit: SearchResult;
  format?: string;
  canPostOnly: boolean;
  children: React.ReactNode;
}) {
  const {hit, format, canPostOnly, children} = props;
  if (!canPostOnly) {
    // Direct link to GET the file
    let url = `${API_URL}/download/${hit.id}`;
    if (format !== undefined) {
      url += '?format=' + format;
    }
    return <LinkButton href={url}>{children}</LinkButton>;
  } else {
    // Button that POST the JSON to obtain the file
    let url = `${API_URL}/download`;
    if (format !== undefined) {
      url += '?format=' + format;
    }
    const postAndDownload = () => {
      // We can't use XHR/fetch to do the POST, because it doesn't behave
      // correctly after redirects. If the API server sends a 302, the browser
      // will refuse to follow it unless the target has CORS headers.
      // So we build and submit a form instead
      const formElement = document.createElement('form');
      formElement.setAttribute('method', 'POST');
      formElement.setAttribute('action', url);
      const taskElement = document.createElement('input');
      taskElement.setAttribute('type', 'hidden');
      taskElement.setAttribute('name', 'task');
      taskElement.setAttribute(
        'value',
        JSON.stringify({id: hit.id, metadata: hit.metadata})
      );
      formElement.appendChild(taskElement);
      document.body.appendChild(formElement);
      formElement.submit();
      document.body.removeChild(formElement);
    };
    return (
      <button
        className="btn btn-sm btn-outline-primary"
        onClick={postAndDownload}
      >
        {children}
      </button>
    );
  }
}

export function DownloadButtons(props: {
  hit: SearchResult;
  session?: Session;
  label?: string;
  canPostOnly: boolean;
}) {
  const {hit, session, label, canPostOnly} = props;
  if (session) {
    return (
      <div className="mt-2">
        <AddToSession hit={hit} label={label} session={session} />
      </div>
    );
  }
  return (
    <ButtonGroup>
      <b>{label || 'Download'}: </b>
      <DownloadButton hit={hit} canPostOnly={canPostOnly}>
        <Icon.Download className="feather" /> CSV
      </DownloadButton>
      <DownloadButton hit={hit} format="d3m" canPostOnly={canPostOnly}>
        <Icon.Download className="feather" /> D3M
      </DownloadButton>
    </ButtonGroup>
  );
}

interface DescriptionProps {
  hit: SearchResult;
  label?: boolean;
}
interface DescriptionState {
  hidden: boolean;
}

export class Description extends React.PureComponent<
  DescriptionProps,
  DescriptionState
> {
  constructor(props: DescriptionProps) {
    super(props);
    this.state = {hidden: true};
  }
  render() {
    const limitLength = 100;
    const {description} = this.props.hit.metadata;
    const showLabel = this.props.label ? this.props.label : false;
    const displayedDescription =
      description && description.length > limitLength && this.state.hidden
        ? description.substring(0, limitLength - 3) + '...'
        : description;
    return (
      <div className="mt-2">
        {showLabel && <b>Description: </b>}
        {description ? (
          <>
            {displayedDescription}
            {description.length > limitLength && (
              <button
                className="text-muted small"
                style={{
                  cursor: 'pointer',
                  textDecoration: 'underline',
                  background: 'transparent',
                  border: 0,
                }}
                onClick={() => this.setState({hidden: !this.state.hidden})}
              >
                {this.state.hidden ? 'Show more...' : 'Show less'}
              </button>
            )}
          </>
        ) : (
          <span className="text-muted">[No description]</span>
        )}
      </div>
    );
  }
}

interface ColumnsViewerProps {
  columns: ColumnMetadata[];
  maxLength?: number;
  label?: boolean;
}

interface ColumnsViewerState {
  hidden: boolean;
}

export class DatasetColumns extends React.PureComponent<
  ColumnsViewerProps,
  ColumnsViewerState
> {
  id = generateRandomId();

  constructor(props: ColumnsViewerProps) {
    super(props);
    this.state = {hidden: true};
  }

  splitColumns(columns: ColumnMetadata[]) {
    const visibleColumns: ColumnMetadata[] = [];
    const hiddenColumns: ColumnMetadata[] = [];
    const maxLength = this.props.maxLength ? this.props.maxLength : 100;
    let characters = 0;
    columns.forEach(c => {
      if (characters + c.name.length > maxLength) {
        hiddenColumns.push(c);
      } else {
        visibleColumns.push(c);
        // add extra chars to account for the badges' extra space
        characters += c.name.length + 9;
      }
    });
    return {visibleColumns, hiddenColumns};
  }

  renderShowMoreButton(hiddenColumns: number) {
    return (
      <button
        className="text-muted small"
        style={{
          cursor: 'pointer',
          textDecoration: 'underline',
          background: 'transparent',
          border: 0,
        }}
        onClick={() => this.setState({hidden: !this.state.hidden})}
      >
        {this.state.hidden
          ? `Show ${hiddenColumns} more columns...`
          : 'Hide columns...'}
      </button>
    );
  }

  render() {
    const {visibleColumns, hiddenColumns} = this.splitColumns(
      this.props.columns
    );
    const showLabel = this.props.label ? this.props.label : false;
    return (
      <div className="mt-2">
        <BadgeGroup>
          {showLabel && <b>Columns:</b>}
          {visibleColumns.map(column => (
            <ColumnBadge column={column} key={`${this.id}-${column.name}`} />
          ))}
          {!this.state.hidden &&
            hiddenColumns.map(column => (
              <ColumnBadge column={column} key={`${this.id}-${column.name}`} />
            ))}
          {hiddenColumns.length > 0 &&
            this.renderShowMoreButton(hiddenColumns.length)}
        </BadgeGroup>
      </div>
    );
  }
}
