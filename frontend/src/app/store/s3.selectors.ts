import { createFeatureSelector, createSelector } from '@ngrx/store';
import { S3State } from './s3.state';

export const selectS3 = createFeatureSelector<S3State>('s3');

// Credentials
export const selectConfigured = createSelector(selectS3, s => s.configured);
export const selectRegion = createSelector(selectS3, s => s.region);
export const selectTokenExpired = createSelector(selectS3, s => s.tokenExpired);
export const selectConfiguring = createSelector(selectS3, s => s.configuring);

// Cache
export const selectScanCacheEntries = createSelector(selectS3, s => s.scanCacheEntries);

// Scan
export const selectScanUri = createSelector(selectS3, s => s.scanUri);
export const selectMaxObjects = createSelector(selectS3, s => s.maxObjects);
export const selectScanning = createSelector(selectS3, s => s.scanning);
export const selectScanProgress = createSelector(selectS3, s => s.scanProgress);
export const selectScanResult = createSelector(selectS3, s => s.scanResult);
export const selectFlatFiles = createSelector(selectS3, s => s.flatFiles);

// Derived: all unique timestamps from scan result
export const selectAllTimestamps = createSelector(
  selectScanResult,
  result => {
    if (!result) return [];
    const tsSet = new Set<string>();
    result.etlRuns.forEach(g => g.runs.forEach(r => tsSet.add(r.timestamp)));
    return [...tsSet].sort().reverse();
  }
);

// Import
export const selectImporting = createSelector(selectS3, s => s.importing);
export const selectShowImportDialog = createSelector(selectS3, s => s.showImportDialog);
export const selectImportProgress = createSelector(selectS3, s => s.importProgress);

// Notification
export const selectNotification = createSelector(selectS3, s => s.notification);
