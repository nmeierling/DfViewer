export interface Dataset {
  id: number;
  name: string;
  sourceUri: string;
  sourceType: string;
  entityPath?: string;
  runTimestamp?: string;
  importedAt: string;
  rowCount: number;
  schemaJson: string;
}

export interface ColumnInfo {
  name: string;
  type: string;
}

export interface DataPage {
  data: Record<string, unknown>[];
  totalRows: number;
  page: number;
  pageSize: number;
  totalPages: number;
}
