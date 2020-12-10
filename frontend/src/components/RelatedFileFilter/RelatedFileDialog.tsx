import * as React from 'react';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import {ButtonGroup} from '../ui/Button/Button';
import {RelatedFileFilter} from './RelatedFileFilter';
import {AugmentationType, RelatedFile} from '../../api/types';
import * as Icon from 'react-feather';
import {Filter} from '../SearchApp/SearchApp';
import {FilterType} from '../AdvancedSearchBar/AdvancedSearchBar';
import {generateRandomId} from '../../utils';

interface RelatedFileDialogStates {
  augmentationType: AugmentationType;
  filters: Filter[];
}

interface RelatedFileDialogProps {
  relatedFile?: RelatedFile | undefined;
  openDialog: boolean;
  filters: Filter[];
  handleCloseDialog: () => void;
  runSearchRelatedQuery: (
    filters: Filter[],
    augmentationType: AugmentationType
  ) => void;
}

class RelatedFileDialog extends React.PureComponent<
  RelatedFileDialogProps,
  RelatedFileDialogStates
> {
  constructor(props: RelatedFileDialogProps) {
    super(props);
    this.state = {
      augmentationType: AugmentationType.JOIN,
      filters: [],
    };
  }
  componentDidUpdate(prevProps: RelatedFileDialogProps) {
    if (
      prevProps.openDialog !== this.props.openDialog &&
      this.props.relatedFile !== undefined
    ) {
      this.onSearchRelated(this.props.relatedFile);
    }
  }

  onSearchRelated(relatedFile: RelatedFile | undefined) {
    if (relatedFile !== undefined) {
      this.setState(() => {
        const prevFilters = this.props.filters;
        const relatedFileFilters = prevFilters.filter(
          f => f.type === FilterType.RELATED_FILE
        );
        let filters;
        if (relatedFileFilters.length > 0) {
          // Update existing filter
          filters = prevFilters.map(filter => {
            if (filter.id === relatedFileFilters[0].id) {
              return {
                ...relatedFileFilters[0],
                state: relatedFile,
              };
            } else {
              return filter;
            }
          });
        } else {
          // Add new filter
          const filterId = generateRandomId();
          const filter: Filter = {
            id: filterId,
            type: FilterType.RELATED_FILE,
            hidden: false,
            state: relatedFile,
          };
          filters = [...prevFilters, filter];
        }
        return {
          filters: filters,
          augmentationType: AugmentationType.JOIN,
        };
      });
    }
  }

  updateRelatedFileFilter(filterId: string, state?: RelatedFile) {
    this.setState(prevState => {
      let found = false;
      const filters = prevState.filters.map(filter => {
        if (filter.id === filterId) {
          found = true;
          return {...filter, state};
        } else {
          return filter;
        }
      });
      if (!found) {
        console.warn(
          `Requested to update filter state with id=[${filterId} which does not exist.]`
        );
      }
      return {filters};
    });
  }

  updateAugmentationType(type: AugmentationType) {
    this.setState({
      augmentationType: type,
    });
  }

  runSearchRelatedQuery() {
    this.props.runSearchRelatedQuery(
      this.state.filters,
      this.state.augmentationType
    );
  }

  render() {
    return (
      <div>
        <Dialog
          open={this.props.openDialog}
          onClose={() => this.props.handleCloseDialog()}
          aria-labelledby="alert-dialog-title"
          aria-describedby="alert-dialog-description"
          maxWidth={'md'}
        >
          <DialogTitle id="alert-dialog-title">
            {'Search Related Datasets'}
          </DialogTitle>
          <DialogContent>
            <DialogContentText id="alert-dialog-description">
              <span className="small">
                We will find datasets that can be augmented with the selected
                dataset.
              </span>
            </DialogContentText>
            {this.state.filters
              .filter(f => f.type === FilterType.RELATED_FILE)
              .map(filter => {
                return (
                  <RelatedFileFilter
                    key={'relatedfilter_' + filter.id}
                    onSelectedFileChange={f =>
                      this.updateRelatedFileFilter(filter.id, f)
                    }
                    onAugmentationTypeChange={type =>
                      this.updateAugmentationType(type)
                    }
                    selectedAugmentationType={this.state.augmentationType}
                    state={filter.state as RelatedFile | undefined}
                  />
                );
              })}
          </DialogContent>
          <DialogActions>
            <ButtonGroup>
              <button
                className="btn btn-sm btn-outline-primary"
                onClick={() => this.props.handleCloseDialog()}
              >
                <Icon.XCircle className="feather" /> Cancel
              </button>
              <button
                className="btn btn-sm btn-outline-primary"
                onClick={() => this.runSearchRelatedQuery()}
                style={{marginLeft: 15, marginRight: 15}}
              >
                <Icon.Search className="feather" /> Search
              </button>
            </ButtonGroup>
          </DialogActions>
        </Dialog>
      </div>
    );
  }
}

export {RelatedFileDialog};
