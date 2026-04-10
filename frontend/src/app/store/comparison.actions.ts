import { createActionGroup, emptyProps, props } from '@ngrx/store';
import { ComparisonSummary, DataPage, ColumnInfo, ColumnChangeSummary } from '../models/dataset.model';

export const CompareActions = createActionGroup({
  source: 'Compare',
  events: {
    'Select Left': props<{ datasetId: number }>(),
    'Add Right': props<{ datasetId: number }>(),
    'Remove Right': props<{ datasetId: number }>(),
    'Set Active Right': props<{ datasetId: number }>(),
    'Left Schema Loaded': props<{ columns: ColumnInfo[] }>(),
    'Set Key Columns': props<{ keyColumns: string[] }>(),
    'Set Ignore Columns': props<{ ignoreColumns: string[] }>(),

    // Runs comparison for ALL right datasets
    'Run Comparison': emptyProps(),
    // Per-right results
    'Comparison Complete': props<{ rightDatasetId: number; summary: ComparisonSummary }>(),
    'Comparison Error': props<{ rightDatasetId: number; error: string }>(),

    'Load Added': props<{ rightDatasetId: number; page: number; size: number }>(),
    'Added Loaded': props<{ rightDatasetId: number; data: DataPage }>(),
    'Load Removed': props<{ rightDatasetId: number; page: number; size: number }>(),
    'Removed Loaded': props<{ rightDatasetId: number; data: DataPage }>(),
    'Load Changed': props<{ rightDatasetId: number; page: number; size: number }>(),
    'Changed Loaded': props<{ rightDatasetId: number; data: DataPage }>(),

    'Load Column Changes': props<{ rightDatasetId: number }>(),
    'Column Changes Loaded': props<{ rightDatasetId: number; columnChanges: ColumnChangeSummary[] }>(),
    'Select Column': props<{ rightDatasetId: number; column: string }>(),
    'Column Data Loaded': props<{ rightDatasetId: number; column: string; data: DataPage }>(),
    'Load Column Page': props<{ rightDatasetId: number; column: string; page: number; size: number }>(),

    // Multi-compare
    'Set Compare Columns': props<{ columns: string[] }>(),
    'Run Multi Comparison': emptyProps(),
    'Multi Column Summary Loaded': props<{ columnChanges: ColumnChangeSummary[] }>(),
    'Multi Column Data Loaded': props<{ data: DataPage }>(),
    'Load Multi Column': props<{ column: string; page: number; size: number }>(),
    'Select Multi Column': props<{ column: string }>(),
    'Multi Loading': props<{ loading: boolean }>(),

    'Reset': emptyProps(),
  }
});
