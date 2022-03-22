import React from 'react';
import * as Icon from 'react-feather';
import {API_URL} from '../../config';
import {SearchResult, ColumnMetadata, Session} from '../../api/types';
import {RequestStatus, downloadToSession} from '../../api/rest';
import {generateRandomId} from '../../utils';
import {GeoSpatialCoverageMap} from '../GeoSpatialCoverageMap/GeoSpatialCoverageMap';
import {BadgeGroup, DatasetTypeBadge, ColumnBadge} from '../Badges/Badges';
import {ButtonGroup, LinkButton} from '../ui/Button/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogTitle from '@material-ui/core/DialogTitle';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogActions from '@material-ui/core/DialogActions';

export function SpatialCoverage(props: {hit: SearchResult}) {
  const metadata = props.hit.metadata;
  const {spatial_coverage} = metadata;
  if (!spatial_coverage) {
    return <></>;
  }
  const sampled =
    (metadata.nb_rows &&
      metadata.nb_profiled_rows &&
      metadata.nb_profiled_rows < metadata.nb_rows) ||
    false;
  return (
    <>
      <h6>Spatial Coverage</h6>
      <span> This is the approximate spatial coverage of the data.</span>
      {spatial_coverage.map((s, i) => (
        <GeoSpatialCoverageMap
          key={`spatial-coverage-map-${i}`}
          coverage={s}
          sampled={sampled}
        />
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
  {hit: SearchResult; session: Session},
  {result?: RequestStatus}
> {
  constructor(props: {hit: SearchResult; session: Session}) {
    super(props);
    this.state = {result: undefined};
  }

  render() {
    const {hit, session} = this.props;

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
        <button
          className="btn btn-sm btn-outline-primary ml-2"
          onClick={clicked}
        >
          <Icon.Download className="feather" /> Add to {session.system_name}
        </button>
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

export function DownloadButtons(props: {hit: SearchResult; session?: Session}) {
  const {hit, session} = props;
  const [open, setOpen] = React.useState(false);
  const handleClickOpen = () => {
    setOpen(true);
  };
  const handleClose = () => {
    setOpen(false);
  };
  if (session) {
    return (
      <div className="mt-2">
        <AddToSession hit={hit} session={session} />
      </div>
    );
  }
  return (
    <>
      <ButtonGroup>
        <button
          className="btn btn-sm btn-outline-primary"
          onClick={handleClickOpen}
        >
          <Icon.Info className="feather" /> ID
        </button>
        <LinkButton href={`${API_URL}/download/${hit.id}`}>
          <Icon.Download className="feather" /> CSV
        </LinkButton>
        <LinkButton href={`${API_URL}/download/${hit.id}?format=d3m`}>
          <Icon.Download className="feather" /> D3M
        </LinkButton>
      </ButtonGroup>
      <Dialog
        open={open}
        onClose={handleClose}
        aria-labelledby="alert-dialog-title"
        aria-describedby="alert-dialog-description"
      >
        <DialogTitle id="alert-dialog-title"> Dataset ID: </DialogTitle>
        <DialogContent>
          <DialogContentText id="alert-dialog-description">
            <div className="d-flex flex-row">
              {hit.id}
              <div
                className="chip-btn-download ml-4"
                onClick={() => navigator.clipboard.writeText(hit.id)}
              >
                <Icon.Copy className="feather" />
              </div>
            </div>
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <button
            className="btn btn-sm btn-outline-primary"
            onClick={handleClose}
            style={{marginRight: 15, marginBottom: 5}}
          >
            <Icon.XCircle className="feather" /> Close
          </button>
        </DialogActions>
      </Dialog>
    </>
  );
}

interface DescriptionProps {
  hit: SearchResult;
  label?: boolean;
  length: number;
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
    const limitLength = this.props.length;
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
          {showLabel && <b>Column Names:</b>}
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
