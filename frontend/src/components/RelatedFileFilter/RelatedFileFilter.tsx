import React from 'react';
import Dropzone from 'react-dropzone';
import { CardShadow, CardButton } from '../visus/Card/Card';
import { formatSize } from '../../utils';
import { PersistentComponent } from '../visus/PersistentComponent/PersistentComponent';
import { RelatedFile } from '../../api/types';

interface RelatedFileFilterState {
  relatedFile?: RelatedFile;
}

interface RelatedFileFilterProps {
  onSelectedFileChange: (relatedFile: RelatedFile) => void;
  relatedFile?: RelatedFile;
}

class RelatedFileFilter extends PersistentComponent<
  RelatedFileFilterProps,
  RelatedFileFilterState
> {
  constructor(props: RelatedFileFilterProps) {
    super(props);
    if (props.relatedFile) {
      this.state = { relatedFile: props.relatedFile };
    } else {
      this.state = { relatedFile: undefined };
    }
  }

  handleSelectedFile(acceptedFiles: File[]) {
    const file = acceptedFiles[0];
    const relatedFile: RelatedFile = { kind: 'localFile', file };
    this.setState({ relatedFile });
    this.props.onSelectedFileChange(relatedFile);
  }

  render() {
    const maxSize = 100 * 1024 * 1024; // maximum file size
    const relatedFile = this.props.relatedFile
      ? this.props.relatedFile
      : this.state.relatedFile;
    if (!relatedFile) {
    } else if (relatedFile.kind === 'localFile') {
      return (
        <div>
          <CardShadow height={'auto'}>
            <span className="font-weight-bold">Selected file:</span>{' '}
            {relatedFile.file.name} ({formatSize(relatedFile.file.size)})
          </CardShadow>
        </div>
      );
    } else if (relatedFile.kind === 'searchResult') {
      return (
        <div>
          <CardShadow height={'auto'}>
            <span className="font-weight-bold">Selected dataset:</span>{' '}
            {relatedFile.datasetName} ({formatSize(relatedFile.datasetSize)})
          </CardShadow>
        </div>
      );
    } else {
      throw new Error('Invalid RelatedFile argument');
    }
    return (
      <div>
        <CardShadow>
          <Dropzone
            multiple={false}
            accept="text/csv"
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
                          We will find datasets that can be joined with your
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

export { RelatedFileFilter };
