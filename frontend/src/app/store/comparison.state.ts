import { ComparisonSummary, ColumnInfo, ColumnChangeSummary } from '../models/dataset.model';

export interface SingleComparisonResult {
  rightDatasetId: number;
  comparing: boolean;
  summary: ComparisonSummary | null;
  error: string;

  addedRows: Record<string, unknown>[];
  addedTotal: number;
  removedRows: Record<string, unknown>[];
  removedTotal: number;
  changedRows: Record<string, unknown>[];
  changedTotal: number;
  tabLoading: boolean;

  columnChangesLoading: boolean;
  columnChanges: ColumnChangeSummary[];
  selectedColumn: string | null;
  columnData: Record<string, unknown>[];
  columnDataTotal: number;
}

export const initialSingleResult = (rightDatasetId: number): SingleComparisonResult => ({
  rightDatasetId,
  comparing: false,
  summary: null,
  error: '',
  addedRows: [],
  addedTotal: 0,
  removedRows: [],
  removedTotal: 0,
  changedRows: [],
  changedTotal: 0,
  tabLoading: false,
  columnChangesLoading: false,
  columnChanges: [],
  selectedColumn: null,
  columnData: [],
  columnDataTotal: 0,
});

export interface ComparisonState {
  leftDatasetId: number | null;
  rightDatasetIds: number[];
  activeRightId: number | null;
  leftColumns: ColumnInfo[];
  keyColumns: string[];
  ignoreColumns: string[];

  // Columns to compare (multi-compare mode)
  compareColumns: string[];

  // Results keyed by rightDatasetId
  results: Record<number, SingleComparisonResult>;

  // Multi-compare state
  multiColumnChanges: ColumnChangeSummary[];
  multiColumnChangesLoading: boolean;
  multiSelectedColumn: string | null;
  multiColumnData: Record<string, unknown>[];
  multiColumnDataTotal: number;
  multiLoading: boolean;
}

export const initialComparisonState: ComparisonState = {
  leftDatasetId: null,
  rightDatasetIds: [],
  activeRightId: null,
  leftColumns: [],
  keyColumns: [],
  ignoreColumns: [],
  compareColumns: [],
  results: {},
  multiColumnChanges: [],
  multiColumnChangesLoading: false,
  multiSelectedColumn: null,
  multiColumnData: [],
  multiColumnDataTotal: 0,
  multiLoading: false,
};
