import React from 'react';
import Dropzone from 'react-dropzone';
import {CardShadow, CardButton} from '../visus/Card/Card';
import {formatSize, shallowEqual, capitalize} from '../../utils';
import {
  Metadata,
  RelatedFile,
  TabularVariable,
  AugmentationType,
} from '../../api/types';
import {ProfileResult, profile, metadata, RequestStatus} from '../../api/rest';
import {RelatedFileColumnsSelector} from './RelatedFileColumnsSelector';
import {Loading} from '../visus/Loading/Loading';
import * as Icon from 'react-feather';

interface RelatedFileFilterState {
  profile?: Metadata;
  selectedTabularVars?: TabularVariable;
  loadingState?: RequestStatus;
}

interface RelatedFileFilterProps {
  onSelectedFileChange: (relatedFile: RelatedFile) => void;
  onAugmentationTypeChange: (type: AugmentationType) => void;
  selectedAugmentationType: AugmentationType;
  state?: RelatedFile;
}

class RelatedFileFilter extends React.PureComponent<
  RelatedFileFilterProps,
  RelatedFileFilterState
> {
  profileQuery?: Promise<ProfileResult | Metadata>;
  profileQueryFile?: RelatedFile;

  constructor(props: RelatedFileFilterProps) {
    super(props);
    this.state = {
      loadingState: undefined,
    };
    this.handleFailedLoading = this.handleFailedLoading.bind(this);
    if (props.state) {
      this.getProfile(props.state);
    }
  }

  getProfile(relatedFile: RelatedFile): Promise<ProfileResult | Metadata> {
    let profileQuery: Promise<ProfileResult | Metadata>;
    if (relatedFile.kind === 'localFile') {
      profileQuery = profile(relatedFile.token);
    } else if (relatedFile.kind === 'searchResult') {
      profileQuery = metadata(relatedFile.datasetId);
    } else {
      throw new Error('Invalid RelatedFile prop');
    }
    this.profileQuery = profileQuery;
    this.profileQueryFile = relatedFile;
    profileQuery.then(p => {
      // Check that this is still the current query
      // (JavaScript can't cancel promises)
      if (this.profileQuery === profileQuery) {
        this.setState({
          profile: p,
          selectedTabularVars: relatedFile.tabularVariables,
        });
      }
    });
    return profileQuery;
  }

  componentDidUpdate() {
    if (!this.props.state) {
      this.profileQuery = undefined;
    } else if (
      !this.profileQueryFile ||
      !shallowEqual(this.props.state, this.profileQueryFile)
    ) {
      // Get profile for this file (asynchronously)
      this.getProfile(this.props.state);
    }
  }

  handleFailedLoading() {
    this.setState({
      loadingState: RequestStatus.ERROR,
    });
  }

  handleSelectedFile(acceptedFiles: File[]) {
    if (acceptedFiles.length > 0) {
      const file = acceptedFiles[0];
      this.setState({loadingState: RequestStatus.IN_PROGRESS});
      const profileQuery = profile(file);
      this.profileQuery = profileQuery;
      profileQuery
        .then(response => {
          // Check that this is still the current query
          // (JavaScript can't cancel promises)
          if (this.profileQuery === profileQuery) {
            // tabular variable
            // TODO: handle 'relationship'
            // for now, it assumes the relationship is 'contains'
            const tabularVariables: TabularVariable = {
              type: 'tabular_variable',
              columns: Array.from(new Array(response.columns.length).keys()),
              relationship: 'contains',
            };
            const relatedFile: RelatedFile = {
              kind: 'localFile',
              token: response.token,
              name: file.name,
              fileSize: file.size,
              tabularVariables,
            };
            this.profileQueryFile = relatedFile;
            this.setState({
              profile: response,
              selectedTabularVars: tabularVariables,
              loadingState: RequestStatus.SUCCESS,
            });
            this.props.onSelectedFileChange(relatedFile);
          } else {
            this.handleFailedLoading();
          }
        })
        .catch(() => {
          this.handleFailedLoading();
        });
    } else {
      this.handleFailedLoading();
    }
  }

  updateSelectedFile(colIndexes: number[]) {
    if (this.state.selectedTabularVars && this.profileQueryFile) {
      const updatedTabularVars = {
        ...this.state.selectedTabularVars,
        columns: colIndexes,
      };
      this.setState({selectedTabularVars: updatedTabularVars});
      const updatedRelatedFile: RelatedFile = {
        ...this.profileQueryFile,
        tabularVariables: updatedTabularVars,
      };
      this.props.onSelectedFileChange(updatedRelatedFile);
    }
  }

  onRemove(columnName: string) {
    if (this.state.selectedTabularVars && this.state.profile) {
      const index = this.state.profile.columns.findIndex(
        el => el.name === columnName
      );
      const colIndexes = this.state.selectedTabularVars.columns.filter(
        i => !(i === index)
      );
      this.updateSelectedFile(colIndexes);
    }
  }

  onAdd(columnName: string) {
    if (this.state.selectedTabularVars && this.state.profile) {
      const index = this.state.profile.columns.findIndex(
        el => el.name === columnName
      );
      const colIndexes = this.state.selectedTabularVars.columns;
      colIndexes.push(index);
      this.updateSelectedFile(colIndexes);
    }
  }

  render() {
    const maxSize = 100 * 1024 * 1024; // maximum file size
    const relatedFile = this.props.state;
    const {profile, selectedTabularVars} = this.state;
    if (this.state.loadingState === RequestStatus.IN_PROGRESS) {
      return (
        <div>
          <CardShadow height={'auto'}>
            <Loading message="Loading and profiling data..." />
          </CardShadow>
        </div>
      );
    }
    if (this.state.loadingState === RequestStatus.ERROR) {
      return (
        <div>
          <CardShadow height={'auto'}>
            <span className="text-danger">
              Failed to load or profile the data. Please try again, or load a
              different file.
            </span>
            <br />
            <button
              className="btn btn-sm btn-outline-primary mt-2"
              onClick={() =>
                this.setState({
                  loadingState: undefined,
                })
              }
            >
              <Icon.XCircle className="feather" /> Close
            </button>
          </CardShadow>
        </div>
      );
    }
    if (
      relatedFile &&
      (this.state.loadingState === RequestStatus.SUCCESS ||
        this.state.loadingState === undefined)
    ) {
      let totalColumns = 0;
      if (profile !== undefined) {
        totalColumns = profile.columns.length;
      }
      return (
        <div>
          <CardShadow height={'auto'}>
            <span className="font-weight-bold">Selected dataset:</span>{' '}
            {relatedFile.name}
            {relatedFile.fileSize !== undefined
              ? ' (' + formatSize(relatedFile.fileSize) + ')'
              : undefined}
            {totalColumns > 0
              ? ` contains ${totalColumns} columns.`
              : undefined}
            <div className="d-inline">
              <span className="font-weight-bold ml-2 mr-1">
                Augmentation Type:{' '}
              </span>
              <select
                className="custom-select"
                style={{width: 'auto'}}
                value={this.props.selectedAugmentationType}
                onChange={e =>
                  this.props.onAugmentationTypeChange(
                    e.target.value as AugmentationType
                  )
                }
                defaultValue={AugmentationType.JOIN}
              >
                {Object.values(AugmentationType)
                  .filter(f => f !== AugmentationType.NONE)
                  .map(value => (
                    <option value={value} key={value}>
                      {capitalize(value)}
                    </option>
                  ))}
              </select>
            </div>
            {profile && selectedTabularVars && (
              <RelatedFileColumnsSelector
                profile={profile}
                selectedTabularVars={selectedTabularVars}
                onAdd={(c: string) => this.onAdd(c)}
                onRemove={(c: string) => this.onRemove(c)}
                onUpdateTabularVariables={(c: number[]) =>
                  this.updateSelectedFile(c)
                }
              />
            )}
          </CardShadow>
        </div>
      );
    }
    return (
      <div>
        <CardShadow>
          <Dropzone
            multiple={false}
            accept=".xls,.xlsx,.csv,.sav,.tsv,text/csv"
            minSize={0}
            maxSize={maxSize}
            onDrop={acceptedFiles => this.handleSelectedFile(acceptedFiles)}
          >
            {({
              getRootProps,
              getInputProps,
              isDragActive,
              isDragReject,
              rejectedFiles,
            }) => {
              const isFileTooLarge =
                rejectedFiles.length > 0 && rejectedFiles[0].size > maxSize;
              return (
                <CardButton {...getRootProps()}>
                  <div className="dropzone dropzone-container text-center">
                    <input {...getInputProps()} />
                    {!isDragActive && (
                      <>
                        <h6 className="d-block text-bold">
                          Search with a dataset instead of text
                        </h6>
                        <span>
                          We will find datasets that can be augmented with your
                          dataset.
                          <br />
                          Click (or drag and drop a file) here to upload a CSV
                          file.
                        </span>
                      </>
                    )}
                    {isDragActive && !isDragReject && 'Drop it here!'}
                    {isDragReject && (
                      <span className="text-danger">
                        File type not accepted. Only CSV files are supported.
                      </span>
                    )}
                    {isFileTooLarge && (
                      <div className="text-danger">File is too large.</div>
                    )}
                  </div>
                </CardButton>
              );
            }}
          </Dropzone>
        </CardShadow>
      </div>
    );
  }
}

export {RelatedFileFilter};
