import { createActionGroup, emptyProps, props } from '@ngrx/store';
import { Dataset, ColumnInfo, DataPage } from '../models/dataset.model';

export const DatasetActions = createActionGroup({
  source: 'Dataset',
  events: {
    // Health
    'Load Health': emptyProps(),
    'Health Loaded': props<{ duckdbSizeBytes: number }>(),

    // List
    'Load Datasets': emptyProps(),
    'Datasets Loaded': props<{ datasets: Dataset[] }>(),
    'Datasets Load Error': props<{ error: string }>(),
    'Delete Dataset': props<{ id: number }>(),
    'Dataset Deleted': props<{ id: number }>(),

    // View
    'Open Dataset': props<{ id: number }>(),
    'Dataset Loaded': props<{ dataset: Dataset }>(),
    'Schema Loaded': props<{ columns: ColumnInfo[] }>(),
    'Null Columns Loaded': props<{ nullColumns: string[] }>(),
    'Dataset Load Error': props<{ error: string }>(),

    // Data
    'Load Data': props<{ page: number; size: number; sortField?: string; sortOrder?: string; filters?: Record<string, string> }>(),
    'Data Loaded': props<{ data: DataPage }>(),
    'Data Load Error': props<{ error: string }>(),
  }
});
