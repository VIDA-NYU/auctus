import React from 'react';
import Dropzone from 'react-dropzone';
import { CardShadow, CardButton } from '../visus/Card/Card';
import { formatSize } from '../../utils';
import { PersistentComponent } from '../visus/PersistentComponent/PersistentComponent';

interface RelatedFileFilterState {
  file?: File;
}

interface RelatedFileFilterProps {
  onSelectedFileChange: (file: File) => void;
}

class RelatedFileFilter extends PersistentComponent<
  RelatedFileFilterProps,
  RelatedFileFilterState
> {
  constructor(props: RelatedFileFilterProps) {
    super(props);
    this.state = { file: undefined };
  }

  handleSelectedFile(acceptedFiles: File[]) {
    const file = acceptedFiles[0];
    this.setState({ file });
    this.props.onSelectedFileChange(file);
  }

  render() {
    const maxSize = 100 * 1024 * 1024; // maximum file size
    const file = this.state.file;
    if (file) {
      return (
        <div>
          <CardShadow height={'auto'}>
            <span className="font-weight-bold">Selected file:</span> {file.name}{' '}
            ({formatSize(file.size)})
          </CardShadow>
        </div>
      );
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
