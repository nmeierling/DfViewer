import { Dataset, ColumnInfo } from '../models/dataset.model';

export interface DatasetState {
  // List
  datasets: Dataset[];
  listLoading: boolean;

  // View
  currentDatasetId: number | null;
  currentDataset: Dataset | null;
  columns: ColumnInfo[];
  rows: Record<string, unknown>[];
  totalRows: number;
  dataLoading: boolean;
  error: string;
}

export const initialDatasetState: DatasetState = {
  datasets: [],
  listLoading: false,

  currentDatasetId: null,
  currentDataset: null,
  columns: [],
  rows: [],
  totalRows: 0,
  dataLoading: false,
  error: '',
};
