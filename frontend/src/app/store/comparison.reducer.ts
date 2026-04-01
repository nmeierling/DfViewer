import { createReducer, on } from '@ngrx/store';
import { CompareActions } from './comparison.actions';
import { ComparisonState, initialComparisonState } from './comparison.state';

export const comparisonReducer = createReducer(
  initialComparisonState,

  on(CompareActions.selectLeft, (state, { datasetId }): ComparisonState => ({
    ...state, leftDatasetId: datasetId, summary: null, error: ''
  })),
  on(CompareActions.selectRight, (state, { datasetId }): ComparisonState => ({
    ...state, rightDatasetId: datasetId, summary: null, error: ''
  })),
  on(CompareActions.leftSchemaLoaded, (state, { columns }): ComparisonState => ({
    ...state, leftColumns: columns, keyColumns: columns.some(c => c.name === 'id') ? ['id'] : []
  })),
  on(CompareActions.setKeyColumns, (state, { keyColumns }): ComparisonState => ({
    ...state, keyColumns
  })),
  on(CompareActions.setIgnoreColumns, (state, { ignoreColumns }): ComparisonState => ({
    ...state, ignoreColumns
  })),

  on(CompareActions.runComparison, (state): ComparisonState => ({
    ...state, comparing: true, summary: null, error: '',
    addedRows: [], addedTotal: 0, removedRows: [], removedTotal: 0, changedRows: [], changedTotal: 0
  })),
  on(CompareActions.comparisonComplete, (state, { summary }): ComparisonState => ({
    ...state, comparing: false, summary
  })),
  on(CompareActions.comparisonError, (state, { error }): ComparisonState => ({
    ...state, comparing: false, error
  })),

  on(CompareActions.loadAdded, (state): ComparisonState => ({ ...state, tabLoading: true })),
  on(CompareActions.addedLoaded, (state, { data }): ComparisonState => ({
    ...state, addedRows: data.data, addedTotal: data.totalRows, tabLoading: false
  })),
  on(CompareActions.loadRemoved, (state): ComparisonState => ({ ...state, tabLoading: true })),
  on(CompareActions.removedLoaded, (state, { data }): ComparisonState => ({
    ...state, removedRows: data.data, removedTotal: data.totalRows, tabLoading: false
  })),
  on(CompareActions.loadChanged, (state): ComparisonState => ({ ...state, tabLoading: true })),
  on(CompareActions.changedLoaded, (state, { data }): ComparisonState => ({
    ...state, changedRows: data.data, changedTotal: data.totalRows, tabLoading: false
  })),

  // Per-column
  on(CompareActions.columnChangesLoaded, (state, { columnChanges }): ComparisonState => ({
    ...state, columnChanges
  })),
  on(CompareActions.selectColumn, (state, { column }): ComparisonState => ({
    ...state, selectedColumn: column, columnData: [], columnDataTotal: 0, tabLoading: true
  })),
  on(CompareActions.columnDataLoaded, (state, { data }): ComparisonState => ({
    ...state, columnData: data.data, columnDataTotal: data.totalRows, tabLoading: false
  })),

  on(CompareActions.reset, (): ComparisonState => initialComparisonState),
);
