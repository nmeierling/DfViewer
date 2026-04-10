import { createFeatureSelector, createSelector } from '@ngrx/store';
import { ComparisonState, initialSingleResult } from './comparison.state';

export const selectComparison = createFeatureSelector<ComparisonState>('comparison');

export const selectLeftDatasetId = createSelector(selectComparison, s => s.leftDatasetId);
export const selectRightDatasetIds = createSelector(selectComparison, s => s.rightDatasetIds);
export const selectActiveRightId = createSelector(selectComparison, s => s.activeRightId);
export const selectLeftColumns = createSelector(selectComparison, s => s.leftColumns);
export const selectKeyColumns = createSelector(selectComparison, s => s.keyColumns);
export const selectIgnoreColumns = createSelector(selectComparison, s => s.ignoreColumns);

// All chosen dataset IDs (left + all rights) for filtering dropdowns
export const selectChosenDatasetIds = createSelector(
  selectLeftDatasetId,
  selectRightDatasetIds,
  (left, rights) => {
    const ids = new Set<number>(rights);
    if (left) ids.add(left);
    return ids;
  }
);

// Active comparison result
export const selectAllResults = createSelector(selectComparison, s => s.results);
export const selectActiveResult = createSelector(
  selectAllResults,
  selectActiveRightId,
  (results, activeId) => activeId ? (results[activeId] || initialSingleResult(activeId)) : null
);

// Is any comparison still running?
export const selectAnyComparing = createSelector(
  selectAllResults,
  results => Object.values(results).some(r => r.comparing)
);

// Active result sub-selectors
export const selectActiveSummary = createSelector(selectActiveResult, r => r?.summary ?? null);
export const selectActiveComparing = createSelector(selectActiveResult, r => r?.comparing ?? false);
export const selectActiveError = createSelector(selectActiveResult, r => r?.error ?? '');
export const selectActiveAddedRows = createSelector(selectActiveResult, r => r?.addedRows ?? []);
export const selectActiveAddedTotal = createSelector(selectActiveResult, r => r?.addedTotal ?? 0);
export const selectActiveRemovedRows = createSelector(selectActiveResult, r => r?.removedRows ?? []);
export const selectActiveRemovedTotal = createSelector(selectActiveResult, r => r?.removedTotal ?? 0);
export const selectActiveChangedRows = createSelector(selectActiveResult, r => r?.changedRows ?? []);
export const selectActiveChangedTotal = createSelector(selectActiveResult, r => r?.changedTotal ?? 0);
export const selectActiveTabLoading = createSelector(selectActiveResult, r => r?.tabLoading ?? false);
export const selectActiveColumnChangesLoading = createSelector(selectActiveResult, r => r?.columnChangesLoading ?? false);
export const selectActiveColumnChanges = createSelector(selectActiveResult, r => r?.columnChanges ?? []);
export const selectActiveSelectedColumn = createSelector(selectActiveResult, r => r?.selectedColumn ?? null);
export const selectActiveColumnData = createSelector(selectActiveResult, r => r?.columnData ?? []);
export const selectActiveColumnDataTotal = createSelector(selectActiveResult, r => r?.columnDataTotal ?? 0);

// Multi-compare
export const selectCompareColumns = createSelector(selectComparison, s => s.compareColumns);
export const selectMultiColumnChanges = createSelector(selectComparison, s => s.multiColumnChanges);
export const selectMultiColumnChangesLoading = createSelector(selectComparison, s => s.multiColumnChangesLoading);
export const selectMultiSelectedColumn = createSelector(selectComparison, s => s.multiSelectedColumn);
export const selectMultiColumnData = createSelector(selectComparison, s => s.multiColumnData);
export const selectMultiColumnDataTotal = createSelector(selectComparison, s => s.multiColumnDataTotal);
export const selectMultiLoading = createSelector(selectComparison, s => s.multiLoading);
export const selectMultiColumnHeaders = createSelector(selectMultiColumnData, rows => rows.length > 0 ? Object.keys(rows[0]) : []);
export const selectIsMultiMode = createSelector(selectRightDatasetIds, ids => ids.length > 1);

// Derived
export const selectKeyColumnSet = createSelector(selectKeyColumns, kc => new Set(kc));
export const selectAddedColumns = createSelector(selectActiveAddedRows, rows => rows.length > 0 ? Object.keys(rows[0]) : []);
export const selectRemovedColumns = createSelector(selectActiveRemovedRows, rows => rows.length > 0 ? Object.keys(rows[0]) : []);
export const selectChangedColumns = createSelector(selectActiveChangedRows, rows => rows.length > 0 ? Object.keys(rows[0]) : []);
export const selectColumnDetailHeaders = createSelector(selectActiveColumnData, rows => rows.length > 0 ? Object.keys(rows[0]) : []);
