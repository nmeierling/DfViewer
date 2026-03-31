import { Injectable } from '@angular/core';
import { S3ScanResult, S3FileEntry, ScanProgress, ImportProgress } from '../models/s3.model';

const LS_CREDENTIALS = 'dfviewer_s3_credentials';
const LS_SCAN_URI = 'dfviewer_s3_scan_uri';

/**
 * Holds S3 browser state across navigation.
 * Singleton service — survives route changes.
 */
@Injectable({ providedIn: 'root' })
export class S3StateService {
  // Credentials
  s3Configured = false;
  region = '';
  tokenExpired = false;

  // Scan
  scanUri = localStorage.getItem(LS_SCAN_URI) || '';
  maxObjects: number | null = null;
  scanning = false;
  scanProgress: ScanProgress | null = null;
  scanResult: S3ScanResult | null = null;
  flatFiles: S3FileEntry[] = [];

  // Import
  importing = false;
  showProgress = false;
  progress: ImportProgress | null = null;

  saveScanUri() {
    localStorage.setItem(LS_SCAN_URI, this.scanUri);
  }

  static getSavedCredentials(): any {
    try {
      const saved = localStorage.getItem(LS_CREDENTIALS);
      return saved ? JSON.parse(saved) : null;
    } catch {
      return null;
    }
  }

  static saveCredentials(creds: any) {
    localStorage.setItem(LS_CREDENTIALS, JSON.stringify(creds));
  }
}
