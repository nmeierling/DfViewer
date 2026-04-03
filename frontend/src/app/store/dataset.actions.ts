import { createActionGroup, emptyProps, props } from '@ngrx/store';
import { Dataset, ColumnInfo, DataPage, ColumnJoinConfig } from '../models/dataset.model';

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

    // Upload
    'Upload File': props<{ file: File; name: string }>(),
    'Upload Complete': props<{ datasetId: number }>(),
    'Upload Error': props<{ error: string }>(),

    // View
    'Open Dataset': props<{ id: number }>(),
    'Dataset Loaded': props<{ dataset: Dataset }>(),
    'Schema Loaded': props<{ columns: ColumnInfo[] }>(),
    'Null Columns Loaded': props<{ nullColumns: string[] }>(),
    'Hidden Columns Loaded': props<{ hiddenColumns: string[] }>(),
    'Set Hidden Columns': props<{ hiddenColumns: string[] }>(),
    'Column Widths Loaded': props<{ columnWidths: Record<string, number> }>(),
    'Set Column Widths': props<{ columnWidths: Record<string, number> }>(),
    'Column Order Loaded': props<{ columnOrder: string[] }>(),
    'Set Column Order': props<{ columnOrder: string[] }>(),
    'Column Joins Loaded': props<{ columnJoins: ColumnJoinConfig[] }>(),
    'Set Column Joins': props<{ columnJoins: ColumnJoinConfig[] }>(),
    'Dataset Load Error': props<{ error: string }>(),

    // Data
    'Load Data': props<{ page: number; size: number; sortField?: string; sortOrder?: string; filters?: Record<string, string> }>(),
    'Data Loaded': props<{ data: DataPage }>(),
    'Data Load Error': props<{ error: string }>(),
  }
});
