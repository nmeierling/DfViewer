import { Dataset, ColumnInfo, ColumnJoinConfig } from '../models/dataset.model';

export interface DatasetState {
  // Health
  duckdbSizeBytes: number;

  // List
  datasets: Dataset[];
  listLoading: boolean;

  // View
  currentDatasetId: number | null;
  currentDataset: Dataset | null;
  columns: ColumnInfo[];
  rows: Record<string, unknown>[];
  totalRows: number;
  nullColumns: string[];
  hiddenColumns: string[];
  columnWidths: Record<string, number>;
  columnOrder: string[];
  columnJoins: ColumnJoinConfig[];
  joinsLoaded: boolean;
  dataLoading: boolean;
  filtered: boolean;
  uploading: boolean;
  uploadDone: boolean;
  error: string;
}

export const initialDatasetState: DatasetState = {
  duckdbSizeBytes: 0,

  datasets: [],
  listLoading: false,

  currentDatasetId: null,
  currentDataset: null,
  columns: [],
  rows: [],
  totalRows: 0,
  nullColumns: [],
  hiddenColumns: [],
  columnWidths: {},
  columnOrder: [],
  columnJoins: [],
  joinsLoaded: false,
  dataLoading: false,
  filtered: false,
  uploading: false,
  uploadDone: false,
  error: '',
};
