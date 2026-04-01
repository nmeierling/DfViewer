import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { S3Credentials, S3Status, S3ScanResult, S3ImportRequest, ImportProgress, ScanProgress, ScanCacheEntry } from '../models/s3.model';
import { WebSocketService } from './websocket.service';

export interface ScanCompleteSummary {
  fileCount: number;
  etlGroupCount: number;
  runCount: number;
}

export type ScanEvent =
  | { type: 'progress'; data: ScanProgress }
  | { type: 'complete'; data: ScanCompleteSummary; taskId: string }
  | { type: 'error'; data: { error: string } };

let taskCounter = 0;
function nextTaskId(): string {
  return `task-${Date.now()}-${++taskCounter}`;
}

@Injectable({ providedIn: 'root' })
export class S3Service {
  private baseUrl = '/api/s3';

  constructor(private http: HttpClient, private ws: WebSocketService) {}

  getStatus(): Observable<S3Status> {
    return this.http.get<S3Status>(`${this.baseUrl}/status`);
  }

  configure(credentials: S3Credentials): Observable<{ configured: boolean }> {
    return this.http.post<{ configured: boolean }>(`${this.baseUrl}/configure`, credentials);
  }

  /** Fetch the full scan result via REST (after WS signals completion) */
  getScanResult(taskId: string): Observable<S3ScanResult> {
    return this.http.get<S3ScanResult>(`${this.baseUrl}/scan/${taskId}/result`);
  }

  getScanCache(): Observable<ScanCacheEntry[]> {
    return this.http.get<ScanCacheEntry[]>(`${this.baseUrl}/scan-cache`);
  }

  getCachedResult(uri: string): Observable<S3ScanResult> {
    return this.http.get<S3ScanResult>(`${this.baseUrl}/scan-cache/result`, {
      params: { uri }
    });
  }

  scan(uri: string, maxObjects?: number, forceRescan: boolean = false): Observable<ScanEvent> {
    return new Observable(observer => {
      const taskId = nextTaskId();

      this.ws.whenConnected().subscribe(() => {
        const { messages$, unsubscribe } = this.ws.subscribe<{ type: string; data: any }>(`/topic/scan/${taskId}`);

        messages$.subscribe({
          next: (event) => {
            console.log(`[S3] Scan event: ${event.type}`);
            if (event.type === 'progress') {
              observer.next({ type: 'progress', data: event.data });
            } else if (event.type === 'complete') {
              observer.next({ type: 'complete', data: event.data, taskId });
              unsubscribe();
              observer.complete();
            } else if (event.type === 'error') {
              observer.next({ type: 'error', data: event.data });
              unsubscribe();
              observer.complete();
            }
          },
          error: (err) => observer.error(err)
        });

        console.log(`[S3] Triggering scan, taskId=${taskId}`);
        this.http.post(`${this.baseUrl}/scan`, { uri, maxObjects, taskId, forceRescan }).subscribe({
          error: (err) => {
            console.error('[S3] Scan HTTP error', err);
            unsubscribe();
            observer.error(err);
          }
        });
      });
    });
  }

  importFiles(request: S3ImportRequest): Observable<ImportProgress> {
    return new Observable(observer => {
      const taskId = nextTaskId();

      this.ws.whenConnected().subscribe(() => {
        const { messages$, unsubscribe } = this.ws.subscribe<{ type: string; data: ImportProgress }>(`/topic/import/${taskId}`);

        messages$.subscribe({
          next: (msg) => {
            console.log(`[S3] Import event: ${msg.data.phase}`);
            observer.next(msg.data);
            if (msg.data.done) {
              unsubscribe();
              observer.complete();
            }
          },
          error: (err) => observer.error(err)
        });

        console.log(`[S3] Triggering import, taskId=${taskId}`);
        this.http.post(`${this.baseUrl}/import`, { ...request, taskId }).subscribe({
          error: (err) => {
            console.error('[S3] Import HTTP error', err);
            unsubscribe();
            observer.error(err);
          }
        });
      });
    });
  }
}
