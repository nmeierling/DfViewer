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
export const selectVisibleColumns = createSelector(
  selectColumns,
  selectNullColumns,
  selectHiddenColumns,
  selectColumnOrder,
  (columns, nullCols, hiddenCols, order) => {
    const visible = columns.filter(c => !nullCols.includes(c.name) && !hiddenCols.includes(c.name));
    if (!order || order.length === 0) return visible;
    // Sort by custom order, columns not in order go to the end
    const orderIndex = new Map(order.map((name, i) => [name, i]));
    return [...visible].sort((a, b) => {
      const ia = orderIndex.get(a.name) ?? 999999;
      const ib = orderIndex.get(b.name) ?? 999999;
      return ia - ib;
    });
  }
);
export const selectDataLoading = createSelector(selectDatasetState, s => s.dataLoading);
export const selectFiltered = createSelector(selectDatasetState, s => s.filtered);
export const selectUploading = createSelector(selectDatasetState, s => s.uploading);
export const selectDatasetError = createSelector(selectDatasetState, s => s.error);
