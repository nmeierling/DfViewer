import { createReducer, on } from '@ngrx/store';
import { S3Actions } from './s3.actions';
import { S3State, initialS3State } from './s3.state';

export const s3Reducer = createReducer(
  initialS3State,

  // Credentials
  on(S3Actions.configure, (state): S3State => ({ ...state, configuring: true })),
  on(S3Actions.configureSuccess, (state, { region }): S3State => ({
    ...state, configured: true, tokenExpired: false, region, configuring: false
  })),
  on(S3Actions.configureFailure, (state): S3State => ({ ...state, configuring: false })),
  on(S3Actions.statusLoaded, (state, { configured, region }): S3State => ({
    ...state, configured, region
  })),
  on(S3Actions.tokenExpired, (state): S3State => ({
    ...state, tokenExpired: true, configured: false,
    notification: { message: 'AWS credentials expired', detail: 'Please reconfigure', type: 'error' }
  })),

  // Cache
  on(S3Actions.scanCacheLoaded, (state, { entries }): S3State => ({
    ...state, scanCacheEntries: entries
  })),

  // Scan
  on(S3Actions.loadCachedResult, (state, { uri }): S3State => ({
    ...state,
    scanUri: uri,
    scanning: true,
    scanResult: null,
    scanProgress: null,
    flatFiles: [],
    notification: { message: 'Loading cached scan...', type: 'info', progress: { mode: 'indeterminate' } }
  })),
  on(S3Actions.startScan, (state, { uri, maxObjects }): S3State => ({
    ...state,
    scanUri: uri,
    maxObjects: maxObjects ?? null,
    scanning: true,
    scanResult: null,
    scanProgress: null,
    flatFiles: [],
    notification: { message: 'Scanning S3...', detail: 'Starting scan', type: 'info', progress: { mode: 'indeterminate' } }
  })),
  on(S3Actions.scanProgress, (state, { progress }): S3State => ({
    ...state,
    scanProgress: progress,
    notification: {
      message: progress.phase === 'analyzing' ? 'Analyzing...' : 'Scanning S3...',
      detail: progress.phase === 'analyzing'
        ? `Discovering ETL runs in ${progress.dataFilesFound.toLocaleString()} files`
        : `${progress.objectsScanned.toLocaleString()} objects, ${progress.dataFilesFound.toLocaleString()} data files`,
      type: 'info',
      progress: { mode: 'indeterminate' }
    }
  })),
  on(S3Actions.scanWSComplete, (state, { summary }): S3State => ({
    ...state,
    notification: {
      message: 'Loading results...',
      detail: `${summary.fileCount.toLocaleString()} files, ${summary.etlGroupCount} entities, ${summary.runCount} runs`,
      type: 'info',
      progress: { mode: 'indeterminate' }
    }
  })),
  on(S3Actions.scanResultLoaded, (state, { result }): S3State => {
    const etlFileUris = new Set(
      result.etlRuns.flatMap(g => g.runs.flatMap(r => r.files.map(f => f.uri)))
    );
    const flatFiles = result.files.filter(f => !etlFileUris.has(f.uri));
    const runs = result.etlRuns.reduce((sum, g) => sum + g.runs.length, 0);
    return {
      ...state,
      scanning: false,
      scanProgress: null,
      scanResult: result,
      flatFiles,
      notification: {
        message: 'Scan complete',
        detail: `${result.files.length.toLocaleString()} files, ${result.etlRuns.length} entities, ${runs} runs`,
        type: 'success'
      }
    };
  }),
  on(S3Actions.scanError, (state, { error }): S3State => ({
    ...state,
    scanning: false,
    scanProgress: null,
    notification: { message: 'Scan failed', detail: error, type: 'error' }
  })),

  // Import
  on(S3Actions.startImport, (state): S3State => ({
    ...state,
    importing: true,
    showImportDialog: true,
    importProgress: null,
    notification: { message: 'Importing...', detail: 'Starting download', type: 'info', progress: { mode: 'indeterminate' } }
  })),
  on(S3Actions.importProgress, (state, { progress }): S3State => {
    let notification = state.notification;
    if (progress.phase === 'downloading') {
      const pct = progress.bytesTotal > 0 ? Math.round(progress.bytesDownloaded / progress.bytesTotal * 100) : 0;
      notification = {
        message: 'Importing...',
        detail: `File ${progress.fileIndex}/${progress.totalFiles}: ${progress.fileName}`,
        type: 'info',
        progress: { mode: 'determinate', value: pct }
      };
    } else if (progress.phase === 'ingesting') {
      notification = {
        message: 'Importing...',
        detail: 'Loading into database...',
        type: 'info',
        progress: { mode: 'indeterminate' }
      };
    }
    return { ...state, importProgress: progress, notification };
  }),
  on(S3Actions.importComplete, (state, { datasetId }): S3State => ({
    ...state,
    importing: false,
    importProgress: { ...state.importProgress!, phase: 'done', datasetId, done: true },
    notification: { message: 'Import complete', detail: 'Ready to view', type: 'success' }
  })),
  on(S3Actions.importError, (state, { error }): S3State => ({
    ...state,
    importing: false,
    importProgress: { phase: 'error', fileIndex: 0, totalFiles: 0, fileName: '', bytesDownloaded: 0, bytesTotal: 0, error, done: true },
    notification: { message: 'Import failed', detail: error, type: 'error' }
  })),
  on(S3Actions.dismissImportDialog, (state): S3State => ({
    ...state, showImportDialog: false
  })),

  // Notification
  on(S3Actions.setNotification, (state, { notification }): S3State => ({
    ...state, notification
  })),
);
