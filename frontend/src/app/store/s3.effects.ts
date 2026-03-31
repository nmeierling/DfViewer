import { inject, Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { of, tap, switchMap, map, catchError, delay } from 'rxjs';
import { S3Actions } from './s3.actions';
import { S3Service } from '../services/s3.service';
import { S3StateService } from '../services/s3-state.service';

const LS_CREDENTIALS = 'dfviewer_s3_credentials';

@Injectable()
export class S3Effects {
  private actions$ = inject(Actions);
  private s3Service = inject(S3Service);
  private store = inject(Store);

  checkStatus$ = createEffect(() => this.actions$.pipe(
    ofType(S3Actions.checkStatus),
    switchMap(() => this.s3Service.getStatus().pipe(
      switchMap(status => {
        if (status.configured) {
          return of(S3Actions.statusLoaded({ configured: true, region: status.region }));
        }
        const saved = S3StateService.getSavedCredentials();
        if (saved?.accessKeyId && saved?.secretAccessKey) {
          return of(S3Actions.configure({ credentials: saved }));
        }
        return of(S3Actions.statusLoaded({ configured: false, region: '' }));
      })
    ))
  ));

  configure$ = createEffect(() => this.actions$.pipe(
    ofType(S3Actions.configure),
    switchMap(({ credentials }) => this.s3Service.configure(credentials).pipe(
      tap(() => localStorage.setItem(LS_CREDENTIALS, JSON.stringify(credentials))),
      map(() => S3Actions.configureSuccess({ region: credentials.region })),
      catchError(err => of(S3Actions.configureFailure({ error: err.message || 'Failed to configure' })))
    ))
  ));

  /** Start scan: WS progress events, then "complete" signal (no large payload) */
  startScan$ = createEffect(() => this.actions$.pipe(
    ofType(S3Actions.startScan),
    tap(({ uri }) => localStorage.setItem('dfviewer_s3_scan_uri', uri)),
    switchMap(({ uri, maxObjects }) => this.s3Service.scan(uri, maxObjects).pipe(
      map(event => {
        if (event.type === 'progress') return S3Actions.scanProgress({ progress: event.data });
        if (event.type === 'complete') return S3Actions.scanWSComplete({ summary: event.data, taskId: event.taskId });
        if (event.type === 'error') {
          const error = event.data?.error || 'Unknown error';
          if (error.toLowerCase().includes('expired') || error.toLowerCase().includes('token')) {
            this.store.dispatch(S3Actions.tokenExpired());
          }
          return S3Actions.scanError({ error });
        }
        return S3Actions.scanError({ error: 'Unknown event' });
      }),
      catchError(err => of(S3Actions.scanError({ error: err.message || 'Scan failed' })))
    ))
  ));

  /** After WS signals scan is done, fetch full result via REST */
  fetchScanResult$ = createEffect(() => this.actions$.pipe(
    ofType(S3Actions.scanWSComplete),
    switchMap(({ taskId }) => this.s3Service.getScanResult(taskId).pipe(
      map(result => S3Actions.scanResultLoaded({ result })),
      catchError(err => of(S3Actions.scanError({ error: err.message || 'Failed to load scan results' })))
    ))
  ));

  startImport$ = createEffect(() => this.actions$.pipe(
    ofType(S3Actions.startImport),
    switchMap(({ files, name, entityPath, runTimestamp }) =>
      this.s3Service.importFiles({ files, name, entityPath, runTimestamp }).pipe(
        map(progress => {
          if (progress.done && progress.phase === 'done') {
            return S3Actions.importComplete({ datasetId: progress.datasetId! });
          }
          if (progress.phase === 'error') {
            return S3Actions.importError({ error: progress.error || 'Import failed' });
          }
          return S3Actions.importProgress({ progress });
        }),
        catchError(err => of(S3Actions.importError({ error: err.message || 'Import failed' })))
      )
    )
  ));

  autoDismissNotification$ = createEffect(() => this.actions$.pipe(
    ofType(S3Actions.scanResultLoaded, S3Actions.importComplete, S3Actions.scanError, S3Actions.importError),
    delay(5000),
    map(() => S3Actions.setNotification({ notification: null }))
  ));
}
