import React from 'react';
import Dropzone from 'react-dropzone';
import {CardShadow, CardButton} from '../visus/Card/Card';
import {formatSize, shallowEqual} from '../../utils';
import {Metadata, RelatedFile, TabularVariable} from '../../api/types';
import {ProfileResult, profile, metadata} from '../../api/rest';
import {RelatedFileColumnsSelector} from './RelatedFileColumnsSelector';

interface RelatedFileFilterState {
  profile?: Metadata;
  selectedTabularVars?: TabularVariable;
}

interface RelatedFileFilterProps {
  onSelectedFileChange: (relatedFile: RelatedFile) => void;
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
    this.state = {};
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
          selectedTabularVars: relatedFile.tabular_variables,
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

  handleSelectedFile(acceptedFiles: File[]) {
    if (acceptedFiles.length > 0) {
      const file = acceptedFiles[0];
      const profileQuery = profile(file);
      this.profileQuery = profileQuery;
      profileQuery.then(p => {
        // Check that this is still the current query
        // (JavaScript can't cancel promises)
        if (this.profileQuery === profileQuery) {
          // tabular variable
          // TODO: handle 'relationship'
          // for now, it assumes the relationship is 'contains'
          const tabularVariables: TabularVariable = {
            type: 'tabular_variable',
            columns: Array.from(new Array(p.columns.length).keys()),
            relationship: 'contains',
          };
          const relatedFile: RelatedFile = {
            kind: 'localFile',
            token: p.token,
            name: file.name,
            fileSize: file.size,
            tabular_variables: tabularVariables,
          };
          this.profileQueryFile = relatedFile;
          this.setState({profile: p, selectedTabularVars: tabularVariables});
          this.props.onSelectedFileChange(relatedFile);
        }
      });
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
        tabular_variables: updatedTabularVars,
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
    if (relatedFile) {
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
