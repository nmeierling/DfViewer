import { createFeatureSelector, createSelector } from '@ngrx/store';
import { DatasetState } from './dataset.state';

export const selectDatasetState = createFeatureSelector<DatasetState>('dataset');

// Health
export const selectDuckdbSizeBytes = createSelector(selectDatasetState, s => s.duckdbSizeBytes);

// List
export const selectDatasets = createSelector(selectDatasetState, s => s.datasets);
export const selectListLoading = createSelector(selectDatasetState, s => s.listLoading);

// View
export const selectCurrentDatasetId = createSelector(selectDatasetState, s => s.currentDatasetId);
export const selectCurrentDataset = createSelector(selectDatasetState, s => s.currentDataset);
export const selectColumns = createSelector(selectDatasetState, s => s.columns);
export const selectRows = createSelector(selectDatasetState, s => s.rows);
export const selectTotalRows = createSelector(selectDatasetState, s => s.totalRows);
export const selectNullColumns = createSelector(selectDatasetState, s => s.nullColumns);
export const selectHiddenColumns = createSelector(selectDatasetState, s => s.hiddenColumns);
export const selectColumnWidths = createSelector(selectDatasetState, s => s.columnWidths);
export const selectColumnOrder = createSelector(selectDatasetState, s => s.columnOrder);
export const selectColumnJoins = createSelector(selectDatasetState, s => s.columnJoins);
export const selectJoinsLoaded = createSelector(selectDatasetState, s => s.joinsLoaded);

// All columns ordered (for the column settings panel — includes hidden + null)
export const selectOrderedAllColumns = createSelector(
  selectColumns,
  selectColumnOrder,
  (columns, order) => {
    if (!order || order.length === 0) return columns;
    const orderIndex = new Map(order.map((name, i) => [name, i]));
    return [...columns].sort((a, b) => {
      const ia = orderIndex.get(a.name) ?? 999999;
      const ib = orderIndex.get(b.name) ?? 999999;
      return ia - ib;
    });
  }
);

// Visible columns (filtered + ordered + virtual display columns)
export const selectVisibleColumns = createSelector(
  selectColumns,
  selectNullColumns,
  selectHiddenColumns,
  selectColumnOrder,
  selectColumnJoins,
  (columns, nullCols, hiddenCols, order, joins) => {
    const visible = columns.filter(c => !nullCols.includes(c.name) && !hiddenCols.includes(c.name));
    let ordered = visible;
    if (order && order.length > 0) {
      const orderIndex = new Map(order.map((name, i) => [name, i]));
      ordered = [...visible].sort((a, b) => {
        const ia = orderIndex.get(a.name) ?? 999999;
        const ib = orderIndex.get(b.name) ?? 999999;
        return ia - ib;
      });
    }
    if (joins && joins.length > 0) {
      const result: { name: string; type: string }[] = [];
      for (const col of ordered) {
        result.push(col);
        const join = joins.find(j => j.sourceColumn === col.name && j.mode === 'add');
        if (join) {
          result.push({ name: col.name + '_display', type: 'VARCHAR' });
        }
      }
      return result;
    }
    return ordered;
  }
);

// Transform rows: apply join display substitutions
export const selectDisplayRows = createSelector(
  selectRows,
  selectColumnJoins,
  (rows, joins) => {
    if (!joins || joins.length === 0) return rows;
    const replaceJoins = joins.filter(j => j.mode === 'replace');
    if (replaceJoins.length === 0) return rows;

    return rows.map(row => {
      const newRow = { ...row };
      for (const join of replaceJoins) {
        const displayKey = join.sourceColumn + '_display';
        if (row[displayKey] != null) {
          newRow[join.sourceColumn] = row[displayKey];
        }
      }
      return newRow;
    });
  }
);

// Joined column set (for quick lookup in templates)
export const selectJoinedColumnSet = createSelector(
  selectColumnJoins,
  (joins) => new Set(joins.map(j => j.sourceColumn))
);

export const selectDataLoading = createSelector(selectDatasetState, s => s.dataLoading);
export const selectFiltered = createSelector(selectDatasetState, s => s.filtered);
export const selectUploading = createSelector(selectDatasetState, s => s.uploading);
export const selectUploadDone = createSelector(selectDatasetState, s => s.uploadDone);
export const selectDatasetError = createSelector(selectDatasetState, s => s.error);
