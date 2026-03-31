export interface S3Credentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  region: string;
}

export interface S3Status {
  configured: boolean;
  region: string;
}

export interface S3ScanResult {
  uri: string;
  files: S3FileEntry[];
  etlRuns: EtlRunGroup[];
}

export interface S3FileEntry {
  key: string;
  uri: string;
  size: number;
  type: 'PARQUET' | 'CSV' | 'UNKNOWN';
  lastModified?: string;
}

export interface EtlRunGroup {
  entityPath: string;
  runs: EtlRun[];
}

export interface EtlRun {
  timestamp: string;
  files: S3FileEntry[];
  totalSize: number;
}

export interface ScanProgress {
  phase: string;
  objectsScanned: number;
  dataFilesFound: number;
  message: string;
}

export interface S3ImportRequest {
  files: string[];
  name: string;
  entityPath?: string;
  runTimestamp?: string;
}

export interface ImportProgress {
  phase: string;
  fileIndex: number;
  totalFiles: number;
  fileName: string;
  bytesDownloaded: number;
  bytesTotal: number;
  datasetId?: number;
  error?: string;
  done: boolean;
}
