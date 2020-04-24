import * as React from 'react';
import * as Icon from 'react-feather';
import { SearchResult, AugmentationInfo } from '../../api/types';
import * as api from '../../api/rest';
import { SearchQuery } from '../../api/rest';
import { triggerFileDownload, cloneObject } from '../../utils';

interface AugmentationOptionsProps {
  hit: SearchResult;
  searchQuery: SearchQuery;
}

interface AugmentationOptionsState {
  checked: {
    [id: string]: boolean;
  };
}

function getAugmentationColumns(aug?: AugmentationInfo) {
  if (!aug) {
    return [];
  }
  const columns: Array<{
    leftColumn: string;
    rightColumn: string;
    key: string;
    idx: number;
  }> = [];

  for (let idx = 0; idx < aug.left_columns_names.length; idx++) {
    const leftColumn = aug.left_columns_names[idx].join(', ');
    const rightColumn = aug.right_columns_names[idx].join(', ');
    const key = `${leftColumn}, ${rightColumn}`;
    columns.push({ leftColumn, rightColumn, key, idx });
  }
  return columns;
}

class AugmentationOptions extends React.PureComponent<
  AugmentationOptionsProps,
  AugmentationOptionsState
> {
  constructor(props: AugmentationOptionsProps) {
    super(props);
    const initialState: AugmentationOptionsState = { checked: {} };
    const columns = getAugmentationColumns(props.hit.augmentation);
    columns.forEach(c => {
      initialState.checked[c.idx.toString()] = true;
    });
    this.state = initialState;
  }

  handleChange(event: React.ChangeEvent<HTMLInputElement>) {
    const input = event.currentTarget;
    const checked = this.state.checked;
    checked[input.value] = !checked[input.value];
    this.setState({ checked: { ...checked } });
  }

  findIndexesOfCheckedColumn() {
    return Object.entries(this.state.checked)
      .filter(c => c[1] === true)
      .map(c => +c[0]); // cast index back to number
  }

  submitAugmentationForm(hit: SearchResult) {
    // find indexes of columns that are checked
    const checkedIndexes = this.findIndexesOfCheckedColumn();

    // hit.augmentation should never undefined at this point
    const original = hit.augmentation!;
    // make a copy of the original so we can modify it
    const augmentation = cloneObject(original);

    augmentation.left_columns = [];
    augmentation.left_columns_names = [];
    augmentation.right_columns = [];
    augmentation.right_columns_names = [];

    // copy only the selected indexes from the original
    // augmentation info to our augmentation request
    augmentation.type = original.type;
    for (const i of checkedIndexes) {
      augmentation.left_columns.push(original.left_columns[i]);
      augmentation.left_columns_names.push(original.left_columns_names[i]);
      augmentation.right_columns.push(original.right_columns[i]);
      augmentation.right_columns_names.push(original.right_columns_names[i]);
    }

    // clone object because we need to modify it for sending as an API parameter
    const task = cloneObject(hit);
    task.augmentation = augmentation;

    const relatedFile = this.props.searchQuery.relatedFile!;
    api.augment(relatedFile, task).then(response => {
      const zipFile = response.data;
      if (zipFile) {
        triggerFileDownload(zipFile, 'augmentation.zip');
      } else {
        console.error('Augment API call returned invalid file: ', zipFile);
      }
    });
  }

  renderAugmentButton(hit: SearchResult, type: string) {
    const btnActive = this.findIndexesOfCheckedColumn().length > 0;
    const btnClass = `btn btn-sm btn-outline-primary mt-2${
      btnActive ? '' : ' disabled'
    }`;
    const btnOnClick = btnActive
      ? () => this.submitAugmentationForm(hit)
      : undefined;
    return (
      <button className={btnClass} onClick={btnOnClick}>
        <Icon.Download className="feather" /> Merge{' '}
        <span style={{ textTransform: 'uppercase' }}>({type}) </span>
        &amp; Download
      </button>
    );
  }

  render() {
    const { hit } = this.props;
    if (!hit.augmentation || hit.augmentation.type === 'none') {
      return null;
    }

    const { type } = hit.augmentation;
    const columns = getAugmentationColumns(hit.augmentation);

    return (
      <div className="mt-3">
        <b>
          Augmentation{' '}
          <span style={{ textTransform: 'uppercase' }}>({type})</span>:
        </b>
        {columns.map((c, i) => (
          <div className="form-check ml-2" key={`div-aug-${i}`}>
            <input
              className="form-check-input"
              type="checkbox"
              value={c.idx}
              checked={this.state.checked[c.idx]}
              id={`checkbox-${c.key}`}
              onChange={e => this.handleChange(e)}
            />
            <label className="form-check-label" htmlFor={`checkbox-${c.key}`}>
              <span className="badge badge-pill badge-secondary mr-1">
                {c.leftColumn}
              </span>
              and
              <span className="badge badge-pill badge-secondary ml-1">
                {c.rightColumn}
              </span>
            </label>
          </div>
        ))}
        {this.renderAugmentButton(hit, type)}
      </div>
    );
  }
}

export { AugmentationOptions };
