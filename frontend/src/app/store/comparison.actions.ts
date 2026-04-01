import { createActionGroup, emptyProps, props } from '@ngrx/store';
import { ComparisonSummary, DataPage, ColumnInfo, ColumnChangeSummary } from '../models/dataset.model';

export const CompareActions = createActionGroup({
  source: 'Compare',
  events: {
    'Select Left': props<{ datasetId: number }>(),
    'Select Right': props<{ datasetId: number }>(),
    'Left Schema Loaded': props<{ columns: ColumnInfo[] }>(),
    'Set Key Columns': props<{ keyColumns: string[] }>(),
    'Set Ignore Columns': props<{ ignoreColumns: string[] }>(),

    'Run Comparison': emptyProps(),
    'Comparison Complete': props<{ summary: ComparisonSummary }>(),
    'Comparison Error': props<{ error: string }>(),

    'Load Added': props<{ page: number; size: number }>(),
    'Added Loaded': props<{ data: DataPage }>(),
    'Load Removed': props<{ page: number; size: number }>(),
    'Removed Loaded': props<{ data: DataPage }>(),
    'Load Changed': props<{ page: number; size: number }>(),
    'Changed Loaded': props<{ data: DataPage }>(),

    // Per-column
    'Load Column Changes': emptyProps(),
    'Column Changes Loaded': props<{ columnChanges: ColumnChangeSummary[] }>(),
    'Select Column': props<{ column: string }>(),
    'Column Data Loaded': props<{ column: string; data: DataPage }>(),
    'Load Column Page': props<{ column: string; page: number; size: number }>(),

    'Reset': emptyProps(),
  }
});
