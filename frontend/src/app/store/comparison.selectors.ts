import { createFeatureSelector, createSelector } from '@ngrx/store';
import { ComparisonState } from './comparison.state';

export const selectComparison = createFeatureSelector<ComparisonState>('comparison');

export const selectLeftDatasetId = createSelector(selectComparison, s => s.leftDatasetId);
export const selectRightDatasetId = createSelector(selectComparison, s => s.rightDatasetId);
export const selectLeftColumns = createSelector(selectComparison, s => s.leftColumns);
export const selectKeyColumns = createSelector(selectComparison, s => s.keyColumns);
export const selectIgnoreColumns = createSelector(selectComparison, s => s.ignoreColumns);
export const selectComparing = createSelector(selectComparison, s => s.comparing);
export const selectSummary = createSelector(selectComparison, s => s.summary);
export const selectCompareError = createSelector(selectComparison, s => s.error);

export const selectAddedRows = createSelector(selectComparison, s => s.addedRows);
export const selectAddedTotal = createSelector(selectComparison, s => s.addedTotal);
export const selectRemovedRows = createSelector(selectComparison, s => s.removedRows);
export const selectRemovedTotal = createSelector(selectComparison, s => s.removedTotal);
export const selectChangedRows = createSelector(selectComparison, s => s.changedRows);
export const selectChangedTotal = createSelector(selectComparison, s => s.changedTotal);
export const selectTabLoading = createSelector(selectComparison, s => s.tabLoading);

export const selectColumnChanges = createSelector(selectComparison, s => s.columnChanges);
export const selectSelectedColumn = createSelector(selectComparison, s => s.selectedColumn);
export const selectColumnData = createSelector(selectComparison, s => s.columnData);
export const selectColumnDataTotal = createSelector(selectComparison, s => s.columnDataTotal);
