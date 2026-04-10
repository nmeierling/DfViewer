import { createReducer, on } from '@ngrx/store';
import { CompareActions } from './comparison.actions';
import { ComparisonState, SingleComparisonResult, initialComparisonState, initialSingleResult } from './comparison.state';

function updateResult(state: ComparisonState, rightDatasetId: number, changes: Partial<SingleComparisonResult>): ComparisonState {
  const existing = state.results[rightDatasetId] || initialSingleResult(rightDatasetId);
  return { ...state, results: { ...state.results, [rightDatasetId]: { ...existing, ...changes } } };
}

export const comparisonReducer = createReducer(
  initialComparisonState,

  on(CompareActions.selectLeft, (state, { datasetId }): ComparisonState => ({
    ...state, leftDatasetId: datasetId, results: {}
  })),
  on(CompareActions.addRight, (state, { datasetId }): ComparisonState => ({
    ...state,
    rightDatasetIds: [...state.rightDatasetIds, datasetId],
    activeRightId: state.activeRightId ?? datasetId,
    results: { ...state.results, [datasetId]: initialSingleResult(datasetId) }
  })),
  on(CompareActions.removeRight, (state, { datasetId }): ComparisonState => {
    const ids = state.rightDatasetIds.filter(id => id !== datasetId);
    const { [datasetId]: _, ...results } = state.results;
    return {
      ...state,
      rightDatasetIds: ids,
      activeRightId: state.activeRightId === datasetId ? (ids[0] ?? null) : state.activeRightId,
      results
    };
  }),
  on(CompareActions.setActiveRight, (state, { datasetId }): ComparisonState => ({
    ...state, activeRightId: datasetId
  })),
  on(CompareActions.leftSchemaLoaded, (state, { columns }): ComparisonState => ({
    ...state,
    leftColumns: columns,
    keyColumns: columns.some(c => c.name === 'id') ? ['id'] : [],
    ignoreColumns: columns.filter(c => c.name === 'timestamp').map(c => c.name)
  })),
  on(CompareActions.setKeyColumns, (state, { keyColumns }): ComparisonState => ({
    ...state, keyColumns
  })),
  on(CompareActions.setIgnoreColumns, (state, { ignoreColumns }): ComparisonState => ({
    ...state, ignoreColumns
  })),

  // Run: mark all as comparing
  on(CompareActions.runComparison, (state): ComparisonState => {
    const results: Record<number, SingleComparisonResult> = {};
    for (const id of state.rightDatasetIds) {
      results[id] = { ...initialSingleResult(id), comparing: true };
    }
    return { ...state, results };
  }),

  on(CompareActions.comparisonComplete, (state, { rightDatasetId, summary }) =>
    updateResult(state, rightDatasetId, { comparing: false, summary })
  ),
  on(CompareActions.comparisonError, (state, { rightDatasetId, error }) =>
    updateResult(state, rightDatasetId, { comparing: false, error })
  ),

  on(CompareActions.loadAdded, (state, { rightDatasetId }) =>
    updateResult(state, rightDatasetId, { tabLoading: true })
  ),
  on(CompareActions.addedLoaded, (state, { rightDatasetId, data }) =>
    updateResult(state, rightDatasetId, { addedRows: data.data, addedTotal: data.totalRows, tabLoading: false })
  ),
  on(CompareActions.loadRemoved, (state, { rightDatasetId }) =>
    updateResult(state, rightDatasetId, { tabLoading: true })
  ),
  on(CompareActions.removedLoaded, (state, { rightDatasetId, data }) =>
    updateResult(state, rightDatasetId, { removedRows: data.data, removedTotal: data.totalRows, tabLoading: false })
  ),
  on(CompareActions.loadChanged, (state, { rightDatasetId }) =>
    updateResult(state, rightDatasetId, { tabLoading: true })
  ),
  on(CompareActions.changedLoaded, (state, { rightDatasetId, data }) =>
    updateResult(state, rightDatasetId, { changedRows: data.data, changedTotal: data.totalRows, tabLoading: false })
  ),

  on(CompareActions.loadColumnChanges, (state, { rightDatasetId }) =>
    updateResult(state, rightDatasetId, { columnChangesLoading: true })
  ),
  on(CompareActions.columnChangesLoaded, (state, { rightDatasetId, columnChanges }) =>
    updateResult(state, rightDatasetId, { columnChanges, columnChangesLoading: false })
  ),
  on(CompareActions.selectColumn, (state, { rightDatasetId, column }) =>
    updateResult(state, rightDatasetId, { selectedColumn: column, columnData: [], columnDataTotal: 0, tabLoading: true })
  ),
  on(CompareActions.columnDataLoaded, (state, { rightDatasetId, data }) =>
    updateResult(state, rightDatasetId, { columnData: data.data, columnDataTotal: data.totalRows, tabLoading: false })
  ),

  // Multi-compare
  on(CompareActions.setCompareColumns, (state, { columns }): ComparisonState => ({
    ...state, compareColumns: columns
  })),
  on(CompareActions.runMultiComparison, (state): ComparisonState => ({
    ...state, multiColumnChanges: [], multiColumnChangesLoading: true, multiSelectedColumn: null, multiColumnData: [], multiLoading: true
  })),
  on(CompareActions.multiColumnSummaryLoaded, (state, { columnChanges }): ComparisonState => ({
    ...state, multiColumnChanges: columnChanges, multiColumnChangesLoading: false, multiLoading: false
  })),
  on(CompareActions.selectMultiColumn, (state, { column }): ComparisonState => ({
    ...state, multiSelectedColumn: column, multiColumnData: [], multiColumnDataTotal: 0, multiLoading: true
  })),
  on(CompareActions.loadMultiColumn, (state): ComparisonState => ({
    ...state, multiLoading: true
  })),
  on(CompareActions.multiColumnDataLoaded, (state, { data }): ComparisonState => ({
    ...state, multiColumnData: data.data, multiColumnDataTotal: data.totalRows, multiLoading: false
  })),
  on(CompareActions.multiLoading, (state, { loading }): ComparisonState => ({
    ...state, multiLoading: loading
  })),

  on(CompareActions.reset, (): ComparisonState => initialComparisonState),
);
