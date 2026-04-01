import { ComparisonSummary, ColumnInfo, ColumnChangeSummary } from '../models/dataset.model';

export interface ComparisonState {
  leftDatasetId: number | null;
  rightDatasetId: number | null;
  leftColumns: ColumnInfo[];
  keyColumns: string[];
  ignoreColumns: string[];
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

  // Per-column
  columnChanges: ColumnChangeSummary[];
  selectedColumn: string | null;
  columnData: Record<string, unknown>[];
  columnDataTotal: number;
}

export const initialComparisonState: ComparisonState = {
  leftDatasetId: null,
  rightDatasetId: null,
  leftColumns: [],
  keyColumns: [],
  ignoreColumns: [],
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

  columnChanges: [],
  selectedColumn: null,
  columnData: [],
  columnDataTotal: 0,
};
