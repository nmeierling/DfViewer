import { createReducer, on } from '@ngrx/store';
import { DatasetActions } from './dataset.actions';
import { DatasetState, initialDatasetState } from './dataset.state';

export const datasetReducer = createReducer(
  initialDatasetState,

  // Health
  on(DatasetActions.healthLoaded, (state, { duckdbSizeBytes }): DatasetState => ({
    ...state, duckdbSizeBytes
  })),

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

  // Upload
  on(DatasetActions.uploadFile, (state): DatasetState => ({ ...state, uploading: true })),
  on(DatasetActions.uploadComplete, (state): DatasetState => ({ ...state, uploading: false })),
  on(DatasetActions.uploadError, (state): DatasetState => ({ ...state, uploading: false })),

  // View
  on(DatasetActions.openDataset, (state, { id }): DatasetState => ({
    ...state,
    currentDatasetId: id,
    currentDataset: null,
    columns: [],
    rows: [],
    totalRows: 0,
    nullColumns: [],
    hiddenColumns: [],
    columnWidths: {},
    columnOrder: [],
    dataLoading: true,
    error: ''
  })),
  on(DatasetActions.datasetLoaded, (state, { dataset }): DatasetState => ({
    ...state, currentDataset: dataset
  })),
  on(DatasetActions.schemaLoaded, (state, { columns }): DatasetState => ({
    ...state, columns
  })),
  on(DatasetActions.nullColumnsLoaded, (state, { nullColumns }): DatasetState => ({
    ...state, nullColumns
  })),
  on(DatasetActions.hiddenColumnsLoaded, (state, { hiddenColumns }): DatasetState => ({
    ...state, hiddenColumns
  })),
  on(DatasetActions.setHiddenColumns, (state, { hiddenColumns }): DatasetState => ({
    ...state, hiddenColumns
  })),
  on(DatasetActions.columnWidthsLoaded, (state, { columnWidths }): DatasetState => ({
    ...state, columnWidths
  })),
  on(DatasetActions.setColumnWidths, (state, { columnWidths }): DatasetState => ({
    ...state, columnWidths
  })),
  on(DatasetActions.columnOrderLoaded, (state, { columnOrder }): DatasetState => ({
    ...state, columnOrder
  })),
  on(DatasetActions.setColumnOrder, (state, { columnOrder }): DatasetState => ({
    ...state, columnOrder
  })),
  on(DatasetActions.datasetLoadError, (state, { error }): DatasetState => ({
    ...state, dataLoading: false, error
  })),

  // Data
  on(DatasetActions.loadData, (state, { filters }): DatasetState => ({
    ...state, dataLoading: true, filtered: !!filters && Object.keys(filters).length > 0
  })),
  on(DatasetActions.dataLoaded, (state, { data }): DatasetState => ({
    ...state, rows: data.data, totalRows: data.totalRows, dataLoading: false
  })),
  on(DatasetActions.dataLoadError, (state): DatasetState => ({
    ...state, dataLoading: false
  })),
);
