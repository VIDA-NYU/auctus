import React from 'react';
import {Metadata, TabularVariable, ColumnMetadata} from '../../api/types';
import {columnType, BadgeGroup} from '../Badges/Badges';
import {IconAbc} from '../Badges/IconAbc';
import * as Icon from 'react-feather';
import './BadgesRelatedFile.css';

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
        >
          <Icon.PlusCircle size={13} />
        </button>
      ) : (
        <button
          type="button"
          title="Remove this column"
          className="btn btn-link badge-button"
          onClick={() => props.onEdit()}
        >
          <Icon.XCircle size={13} />
        </button>
      )}
    </span>
  );
}

export function BadgesRelatedFile(props: {
  profile: Metadata;
  selectedTabularVars: TabularVariable;
  onAdd: (colName: string) => void;
  onRemove: (colName: string) => void;
  onUpdateTabularVariables: (colIndexes: number[]) => void;
}) {
  const {profile, selectedTabularVars} = props;
  return (
    <div className="row">
      <div className="col-sm border-right">
        <b className="mt-2 ">Available columns:</b>
        {selectedTabularVars.columns.length !== profile.columns.length && (
          <button
            className="text-muted small label-button"
            onClick={() =>
              props.onUpdateTabularVariables(
                Array.from(new Array(profile.columns.length).keys())
              )
            }
          >
            Add All
          </button>
        )}
        <br />
        <span className="small">
          {selectedTabularVars.columns.length === profile.columns.length
            ? 'All columns were selected.'
            : 'Select which columns should be included in the search.'}
        </span>
        <BadgeGroup>
          {profile.columns
            .filter(
              (unit, index) =>
                selectedTabularVars &&
                !selectedTabularVars.columns.includes(index)
            )
            .map((c, i) => (
              <ColumnBadgeRelatedFile
                key={`badge-bin-${'uniqueBinId'}-column-${i}`}
                type={'Add'}
                column={c}
                onEdit={() => props.onAdd(c.name)}
              />
            ))}
        </BadgeGroup>
      </div>
      <div className="col-sm">
        <b className="mt-2">Selected columns:</b>
        {selectedTabularVars.columns.length > 0 && (
          <button
            className="text-muted small label-button"
            onClick={() => props.onUpdateTabularVariables([])}
          >
            Remove All
          </button>
        )}
        <br />
        {selectedTabularVars.columns.length === 0 ? (
          <span className="small">
            {' '}
            Select <b className="label danger"> at least one column </b> to be
            included in the search.
          </span>
        ) : (
          <span className="small">
            These columns will be included in the search.
          </span>
        )}
        <BadgeGroup>
          {selectedTabularVars.columns
            .map(index => profile && profile.columns[index])
            .map(
              (c, i) =>
                c && (
                  <ColumnBadgeRelatedFile
                    key={`badge-bin-${'uniqueBinId'}-column-${i}`}
                    type={'Remove'}
                    column={c}
                    onEdit={() => props.onRemove(c.name)}
                  />
                )
            )}
        </BadgeGroup>
      </div>
    </div>
  );
}
