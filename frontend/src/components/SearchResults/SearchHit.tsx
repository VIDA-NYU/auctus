import * as React from 'react';
import * as Icon from 'react-feather';
import {API_URL} from '../../config';
import {formatSize} from '../../utils';
import {
  SearchResult,
  RelatedFile,
  Session,
  TabularVariable,
} from '../../api/types';
import {
  Description,
  DatasetTypes,
  DatasetColumns,
  AddToSession,
} from './Metadata';
import {ButtonGroup, LinkButton} from '../ui/Button/Button';

interface SearchHitProps {
  hit: SearchResult;
  selectedHit?: boolean;
  session?: Session;
  onSearchHitExpand: (hit: SearchResult) => void;
  onSearchRelated: (relatedFile: RelatedFile) => void;
  onAugmentationOptions: (hit: SearchResult) => void;
}

interface SearchHitState {
  hidden: boolean;
}

function DownloadViewDetails(props: {
  hit: SearchResult;
  session?: Session;
  onSearchHitExpand: () => void;
  onSearchRelated: () => void;
  onAugmentationOptions: () => void;
}) {
  return (
    <div className="mt-2">
      <ButtonGroup>
        {props.session ? (
          <AddToSession hit={props.hit} session={props.session} />
        ) : (
          <LinkButton href={`${API_URL}/download/${props.hit.id}`}>
            <Icon.Download className="feather" /> Download
          </LinkButton>
        )}
        <button
          className="btn btn-sm btn-outline-primary"
          onClick={props.onSearchHitExpand}
        >
          <Icon.Info className="feather" /> View Details
        </button>
        <button
          className="btn btn-sm btn-outline-primary"
          onClick={props.onSearchRelated}
        >
          <Icon.Search className="feather" /> Search Related
        </button>
        {!(
          !props.hit.augmentation || props.hit.augmentation.type === 'none'
        ) && (
          <button
            className="btn btn-sm btn-outline-primary"
            onClick={props.onAugmentationOptions}
          >
            <Icon.PlusCircle className="feather" /> Augment Options
          </button>
        )}
      </ButtonGroup>
    </div>
  );
}

function HitTitle(props: {hit: SearchResult}) {
  return (
    <span
      className="text-primary"
      style={{fontSize: '1.2rem', fontFamily: 'Source Sans Pro'}}
    >
      {props.hit.metadata.name}{' '}
      <span className="small text-muted">
        ({formatSize(props.hit.metadata.size)})
      </span>
    </span>
  );
}

class SearchHit extends React.PureComponent<SearchHitProps, SearchHitState> {
  constructor(props: SearchHitProps) {
    super(props);
    this.state = {
      hidden: true,
    };
    this.onSearchHitExpand = this.onSearchHitExpand.bind(this);
    this.onSearchRelated = this.onSearchRelated.bind(this);
    this.onAugmentationOptions = this.onAugmentationOptions.bind(this);
  }

  onSearchHitExpand() {
    this.props.onSearchHitExpand(this.props.hit);
  }

  onSearchRelated() {
    // tabular variable
    // TODO: handle 'relationship'
    // for now, it assumes the relationship is 'contains'
    const tabularVariables: TabularVariable = {
      type: 'tabular_variable',
      columns: Array.from(
        new Array(this.props.hit.metadata.columns.length).keys()
      ),
      relationship: 'contains',
    };
    const relatedFile: RelatedFile = {
      kind: 'searchResult',
      datasetId: this.props.hit.id,
      name: this.props.hit.metadata.name,
      tabularVariables,
    };
    this.props.onSearchRelated(relatedFile);
  }

  onAugmentationOptions() {
    this.props.onAugmentationOptions(this.props.hit);
  }

  render() {
    const {hit, selectedHit, session} = this.props;
    return (
      <div
        className="card shadow-sm d-flex flex-row"
        style={{
          backgroundColor: selectedHit ? '#f5f4fa' : 'white',
        }}
      >
        <div className="card-body d-flex flex-column">
          <HitTitle hit={hit} />
          <span className="small">{hit.metadata.source}</span>
          <Description hit={hit} label={false} length={100} />
          <DatasetColumns columns={hit.metadata.columns} label={false} />
          <DatasetTypes hit={hit} label={false} />
          <DownloadViewDetails
            hit={hit}
            session={session}
            onSearchHitExpand={this.onSearchHitExpand}
            onSearchRelated={this.onSearchRelated}
            onAugmentationOptions={this.onAugmentationOptions}
          />
        </div>
        <div
          className="d-flex align-items-stretch"
          style={{cursor: 'pointer'}}
          onClick={this.onSearchHitExpand}
        >
          <div style={{margin: 'auto 3px'}}>
            <Icon.ChevronRight className="feather feather-lg" />
          </div>
        </div>
      </div>
    );
  }
}

export {SearchHit};
