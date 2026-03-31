import { createReducer, on } from '@ngrx/store';
import { DatasetActions } from './dataset.actions';
import { DatasetState, initialDatasetState } from './dataset.state';

export const datasetReducer = createReducer(
  initialDatasetState,

  // List
  on(DatasetActions.loadDatasets, (state): DatasetState => ({
    ...state, listLoading: true
  })),
  on(DatasetActions.datasetsLoaded, (state, { datasets }): DatasetState => ({
    ...state, datasets, listLoading: false
  })),
  on(DatasetActions.datasetsLoadError, (state): DatasetState => ({
    ...state, listLoading: false
  })),
  on(DatasetActions.datasetDeleted, (state, { id }): DatasetState => ({
    ...state, datasets: state.datasets.filter(d => d.id !== id)
  })),

  // View
  on(DatasetActions.openDataset, (state, { id }): DatasetState => ({
    ...state,
    currentDatasetId: id,
    currentDataset: null,
    columns: [],
    rows: [],
    totalRows: 0,
    dataLoading: true,
    error: ''
  })),
  on(DatasetActions.datasetLoaded, (state, { dataset }): DatasetState => ({
    ...state, currentDataset: dataset
  })),
  on(DatasetActions.schemaLoaded, (state, { columns }): DatasetState => ({
    ...state, columns
  })),
  on(DatasetActions.datasetLoadError, (state, { error }): DatasetState => ({
    ...state, dataLoading: false, error
  })),

  // Data
  on(DatasetActions.loadData, (state): DatasetState => ({
    ...state, dataLoading: true
  })),
  on(DatasetActions.dataLoaded, (state, { data }): DatasetState => ({
    ...state, rows: data.data, totalRows: data.totalRows, dataLoading: false
  })),
  on(DatasetActions.dataLoadError, (state): DatasetState => ({
    ...state, dataLoading: false
  })),
);
