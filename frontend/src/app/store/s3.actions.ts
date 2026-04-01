import { createActionGroup, emptyProps, props } from '@ngrx/store';
import { S3Credentials, S3ScanResult, ScanProgress, ImportProgress, ScanCacheEntry } from '../models/s3.model';
import { ScanCompleteSummary } from '../services/s3.service';

export const S3Actions = createActionGroup({
  source: 'S3',
  events: {
    // Credentials
    'Configure': props<{ credentials: S3Credentials }>(),
    'Configure Success': props<{ region: string }>(),
    'Configure Failure': props<{ error: string }>(),
    'Check Status': emptyProps(),
    'Status Loaded': props<{ configured: boolean; region: string }>(),
    'Token Expired': emptyProps(),

    // Cache
    'Load Scan Cache': emptyProps(),
    'Scan Cache Loaded': props<{ entries: ScanCacheEntry[] }>(),

    // Scan
    'Load Cached Result': props<{ uri: string }>(),
    'Start Scan': props<{ uri: string; maxObjects?: number; forceRescan?: boolean }>(),
    'Scan Progress': props<{ progress: ScanProgress }>(),
    'Scan WS Complete': props<{ summary: ScanCompleteSummary; taskId: string }>(),
    'Scan Result Loaded': props<{ result: S3ScanResult }>(),
    'Scan Error': props<{ error: string }>(),

    // Import
    'Start Import': props<{ files: string[]; name: string; entityPath?: string; runTimestamp?: string }>(),
    'Import Progress': props<{ progress: ImportProgress }>(),
    'Import Complete': props<{ datasetId: number }>(),
    'Import Error': props<{ error: string }>(),
    'Dismiss Import Dialog': emptyProps(),

    // Notification
    'Set Notification': props<{ notification: { message: string; detail?: string; type: 'info' | 'success' | 'error'; progress?: { mode: 'determinate' | 'indeterminate'; value?: number } } | null }>(),
  }
});
