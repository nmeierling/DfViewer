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
export const selectVisibleColumns = createSelector(
  selectColumns,
  selectNullColumns,
  (columns, nullCols) => columns.filter(c => !nullCols.includes(c.name))
);
export const selectDataLoading = createSelector(selectDatasetState, s => s.dataLoading);
export const selectFiltered = createSelector(selectDatasetState, s => s.filtered);
export const selectDatasetError = createSelector(selectDatasetState, s => s.error);
