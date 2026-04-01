import { Dataset, ColumnInfo } from '../models/dataset.model';

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
  dataLoading: boolean;
  filtered: boolean;
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
  dataLoading: false,
  filtered: false,
  error: '',
};
