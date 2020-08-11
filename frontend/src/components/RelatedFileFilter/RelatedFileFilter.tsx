import React from 'react';
import Dropzone from 'react-dropzone';
import {CardShadow, CardButton} from '../visus/Card/Card';
import {formatSize, shallowEqual} from '../../utils';
import {
  Metadata,
  RelatedFile,
  TabularVariable,
  ColumnMetadata,
} from '../../api/types';
import {ProfileResult, profile, metadata} from '../../api/rest';
import {columnType, BadgeGroup} from '../Badges/Badges';
import {IconAbc} from '../Badges/IconAbc';
import * as Icon from 'react-feather';

function iconForType(types: {
  textual?: boolean;
  temporal?: boolean;
  numerical?: boolean;
  spatial?: boolean;
}) {
  if (types.spatial) {
    return Icon.Globe;
  } else if (types.temporal) {
    return Icon.Calendar;
  } else if (types.numerical) {
    return Icon.Hash;
  } else {
    return IconAbc;
  }
}

export function ColumnBadgeRelatedFile(props: {
  column: ColumnMetadata;
  type: 'Add' | 'Remove';
  onEdit: () => void;
}) {
  const label = props.column.name;
  const types = columnType(props.column);
  const badgeClass = types.numerical ? 'badge-numerical' : 'badge-textual';
  const BadgeIcon = iconForType(types);

  return (
    <span className={`badge badge-pill ${badgeClass}`}>
      <BadgeIcon className="feather-xs-w" />
      {label}
      {props.type === 'Add' ? (
        <button
          type="button"
          title="Add this column"
          className="btn btn-link badge-button"
          onClick={() => props.onEdit()}
          style={{marginRight: '-6px'}}
        >
          <Icon.PlusCircle size={11} />
        </button>
      ) : (
        <button
          type="button"
          title="Remove this column"
          className="btn btn-link badge-button"
          onClick={() => props.onEdit()}
          style={{marginRight: '-6px'}}
        >
          <Icon.XCircle size={11} />
        </button>
      )}
    </span>
  );
}

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

  updateSelectedFile(newState: number[]) {
    if (this.state.selectedTabularVars && this.profileQueryFile) {
      const updatedTabularVars = {
        ...this.state.selectedTabularVars,
        columns: newState,
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
      const newState = this.state.selectedTabularVars.columns.filter(
        i => !(i === index)
      );
      this.updateSelectedFile(newState);
    }
  }

  onAdd(columnName: string) {
    if (this.state.selectedTabularVars && this.state.profile) {
      const index = this.state.profile.columns.findIndex(
        el => el.name === columnName
      );
      const newState = this.state.selectedTabularVars.columns;
      newState.push(index);
      this.updateSelectedFile(newState);
    }
  }

  render() {
    const maxSize = 100 * 1024 * 1024; // maximum file size
    const relatedFile = this.props.state;
    if (relatedFile) {
      let columns = '';
      if (this.state.profile !== undefined) {
        columns = this.state.profile.columns.map(c => c.name).join(', ');
      }
      return (
        <div>
          <CardShadow height={'auto'}>
            <span className="font-weight-bold">Selected dataset:</span>{' '}
            {relatedFile.name}
            {relatedFile.fileSize !== undefined
              ? ' (' + formatSize(relatedFile.fileSize) + ')'
              : undefined}
            {columns ? ` (${columns})` : undefined}
            {this.state.profile && this.state.selectedTabularVars && (
              <div className="row">
                <div className="col-sm border-right">
                  <b className="mt-2 ">Available columns:</b>
                  <br />
                  <span className="small">
                    {this.state.selectedTabularVars.columns.length ===
                    this.state.profile.columns.length
                      ? 'All columns were selected.'
                      : 'Select which columns should be added to the search.'}
                  </span>
                  <BadgeGroup>
                    {this.state.profile.columns
                      .filter(
                        (unit, index) =>
                          this.state.selectedTabularVars &&
                          !this.state.selectedTabularVars.columns.includes(
                            index
                          )
                      )
                      .map((c, i) => (
                        <ColumnBadgeRelatedFile
                          key={`badge-bin-${'uniqueBinId'}-column-${i}`}
                          type={'Add'}
                          column={c}
                          onEdit={() => this.onAdd(c.name)}
                        />
                      ))}
                  </BadgeGroup>
                </div>
                <div className="col-sm">
                  <b className="mt-2">Selected columns:</b>
                  <br />
                  <span className="small">
                    These columns will be added to the search.
                  </span>
                  <BadgeGroup>
                    {this.state.selectedTabularVars.columns
                      .map(
                        index =>
                          this.state.profile &&
                          this.state.profile.columns[index]
                      )
                      .map(
                        (c, i) =>
                          c && (
                            <ColumnBadgeRelatedFile
                              key={`badge-bin-${'uniqueBinId'}-column-${i}`}
                              type={'Remove'}
                              column={c}
                              onEdit={() => this.onRemove(c.name)}
                            />
                          )
                      )}
                  </BadgeGroup>
                </div>
              </div>
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
