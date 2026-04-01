import { S3ScanResult, S3FileEntry, ScanProgress, ImportProgress, ScanCacheEntry } from '../models/s3.model';

export interface NotificationState {
  message: string;
  detail?: string;
  type: 'info' | 'success' | 'error';
  progress?: { mode: 'determinate' | 'indeterminate'; value?: number };
}

export interface S3State {
  // Credentials
  configured: boolean;
  region: string;
  tokenExpired: boolean;
  configuring: boolean;

  // Cache
  scanCacheEntries: ScanCacheEntry[];

  // Scan
  scanUri: string;
  maxObjects: number | null;
  scanning: boolean;
  scanProgress: ScanProgress | null;
  scanResult: S3ScanResult | null;
  flatFiles: S3FileEntry[];

  // Import
  importing: boolean;
  showImportDialog: boolean;
  importProgress: ImportProgress | null;

  // Notification
  notification: NotificationState | null;
}

const LS_SCAN_URI = 'dfviewer_s3_scan_uri';

export const initialS3State: S3State = {
  configured: false,
  region: '',
  tokenExpired: false,
  configuring: false,

  scanCacheEntries: [],

  scanUri: localStorage.getItem(LS_SCAN_URI) || '',
  maxObjects: null,
  scanning: false,
  scanProgress: null,
  scanResult: null,
  flatFiles: [],

  importing: false,
  showImportDialog: false,
  importProgress: null,

  notification: null,
};
