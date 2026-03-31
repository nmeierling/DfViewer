import { Injectable, NgZone } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { S3Credentials, S3Status, S3ScanResult, S3ImportRequest, ImportProgress, ScanProgress } from '../models/s3.model';

export type ScanEvent =
  | { type: 'progress'; data: ScanProgress }
  | { type: 'result'; data: S3ScanResult }
  | { type: 'error'; data: { error: string } };

@Injectable({ providedIn: 'root' })
export class S3Service {
  private baseUrl = '/api/s3';

  constructor(private http: HttpClient, private zone: NgZone) {}

  getStatus(): Observable<S3Status> {
    return this.http.get<S3Status>(`${this.baseUrl}/status`);
  }

  configure(credentials: S3Credentials): Observable<{ configured: boolean }> {
    return this.http.post<{ configured: boolean }>(`${this.baseUrl}/configure`, credentials);
  }

  scan(uri: string, maxObjects?: number): Observable<ScanEvent> {
    let url = `${this.baseUrl}/scan?uri=${encodeURIComponent(uri)}`;
    if (maxObjects) url += `&maxObjects=${maxObjects}`;
    return this.readSseStream<ScanEvent>(url, (eventName, data) => {
      if (eventName === 'progress') return { type: 'progress', data } as ScanEvent;
      if (eventName === 'result') return { type: 'result', data } as ScanEvent;
      if (eventName === 'error') return { type: 'error', data } as ScanEvent;
      return null;
    });
  }

  importFiles(request: S3ImportRequest): Observable<ImportProgress> {
    return this.readSseStream<ImportProgress>(
      `${this.baseUrl}/import`,
      (_eventName, data) => data as ImportProgress,
      { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(request) }
    );
  }

  /**
   * Generic SSE stream reader using fetch + ReadableStream.
   * Works reliably through proxies unlike EventSource.
   */
  private readSseStream<T>(url: string, mapEvent: (eventName: string, data: unknown) => T | null, fetchInit?: RequestInit): Observable<T> {
    return new Observable(observer => {
      const controller = new AbortController();

      fetch(url, { ...fetchInit, signal: controller.signal }).then(response => {
        if (!response.ok) {
          response.json().then(body => {
            this.zone.run(() => {
              observer.error(body);
            });
          }).catch(() => {
            this.zone.run(() => observer.error(new Error(`HTTP ${response.status}`)));
          });
          return;
        }

        const reader = response.body!.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        const read = (): void => {
          reader.read().then(({ done, value }) => {
            if (done) {
              this.zone.run(() => observer.complete());
              return;
            }

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            let currentEvent = '';
            for (const line of lines) {
              if (line.startsWith('event:')) {
                currentEvent = line.substring(6).trim();
              } else if (line.startsWith('data:')) {
                const raw = line.substring(5).trim();
                try {
                  const data = JSON.parse(raw);
                  const mapped = mapEvent(currentEvent, data);
                  if (mapped !== null) {
                    this.zone.run(() => observer.next(mapped));
                  }
                } catch {
                  // skip malformed
                }
                currentEvent = '';
              }
            }

            read();
          }).catch(err => {
            if (err.name !== 'AbortError') {
              this.zone.run(() => observer.error(err));
            }
          });
        };

        read();
      }).catch(err => {
        if (err.name !== 'AbortError') {
          this.zone.run(() => observer.error(err));
        }
      });

      return () => controller.abort();
    });
  }
}
