export interface Dataset {
  id: number;
  name: string;
  sourceUri: string;
  sourceType: string;
  entityPath?: string;
  runTimestamp?: string;
  importedAt: string;
  rowCount: number;
  sizeBytes: number;
  schemaJson: string;
}

export interface ColumnInfo {
  name: string;
  type: string;
}

export interface ComparisonSummary {
  leftDatasetId: number;
  rightDatasetId: number;
  leftName: string;
  rightName: string;
  keyColumns: string[];
  totalLeft: number;
  totalRight: number;
  addedCount: number;
  removedCount: number;
  changedCount: number;
  unchangedCount: number;
}

export interface ColumnChangeSummary {
  column: string;
  changedCount: number;
}

export interface DataPage {
  data: Record<string, unknown>[];
  totalRows: number;
  page: number;
  pageSize: number;
  totalPages: number;
}
