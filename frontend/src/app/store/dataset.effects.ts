import { inject, Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { of, switchMap, map, catchError, withLatestFrom, timer } from 'rxjs';
import { retry } from 'rxjs/operators';
import { DatasetActions } from './dataset.actions';
import { ApiService } from '../services/api.service';
import { selectCurrentDatasetId } from './dataset.selectors';

@Injectable()
export class DatasetEffects {
  private actions$ = inject(Actions);
  private api = inject(ApiService);
  private store = inject(Store);

  loadHealth$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.loadHealth),
    switchMap(() => this.api.getHealth().pipe(
      map((res: any) => DatasetActions.healthLoaded({ duckdbSizeBytes: res.duckdbSizeBytes ?? 0 })),
      catchError(() => of(DatasetActions.healthLoaded({ duckdbSizeBytes: 0 })))
    ))
  ));

  uploadFile$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.uploadFile),
    switchMap(({ file, name }) => this.api.uploadFile(file, name).pipe(
      switchMap(res => [
        DatasetActions.uploadComplete({ datasetId: res.datasetId }),
        DatasetActions.loadDatasets()
      ]),
      catchError(err => of(DatasetActions.uploadError({ error: err.error?.error || err.message || 'Upload failed' })))
    ))
  ));

  loadDatasets$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.loadDatasets),
    switchMap(() => this.api.listDatasets().pipe(
      map(datasets => DatasetActions.datasetsLoaded({ datasets })),
      catchError(err => of(DatasetActions.datasetsLoadError({ error: err.message })))
    ))
  ));

  deleteDataset$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.deleteDataset),
    switchMap(({ id }) => this.api.deleteDataset(id).pipe(
      map(() => DatasetActions.datasetDeleted({ id })),
      catchError(() => of(DatasetActions.datasetsLoadError({ error: 'Delete failed' })))
    ))
  ));

  openDataset$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.openDataset),
    switchMap(({ id }) => this.api.getDataset(id).pipe(
      retry({ count: 3, delay: () => timer(1000) }),
      map(dataset => DatasetActions.datasetLoaded({ dataset })),
      catchError(err => of(DatasetActions.datasetLoadError({ error: 'Dataset not found' })))
    ))
  ));

  loadSchema$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.openDataset),
    switchMap(({ id }) => this.api.getSchema(id).pipe(
      retry({ count: 3, delay: () => timer(1000) }),
      map(columns => DatasetActions.schemaLoaded({ columns })),
      catchError(err => of(DatasetActions.datasetLoadError({ error: 'Failed to load schema' })))
    ))
  ));

  loadNullColumns$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.openDataset),
    switchMap(({ id }) => this.api.getNullColumns(id).pipe(
      retry({ count: 3, delay: () => timer(1000) }),
      map(nullColumns => DatasetActions.nullColumnsLoaded({ nullColumns })),
      catchError(() => of(DatasetActions.nullColumnsLoaded({ nullColumns: [] })))
    ))
  ));

  loadHiddenColumns$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.openDataset),
    switchMap(({ id }) => this.api.getHiddenColumns(id).pipe(
      map(hiddenColumns => DatasetActions.hiddenColumnsLoaded({ hiddenColumns })),
      catchError(() => of(DatasetActions.hiddenColumnsLoaded({ hiddenColumns: [] })))
    ))
  ));

  saveHiddenColumns$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.setHiddenColumns),
    withLatestFrom(this.store.select(selectCurrentDatasetId)),
    switchMap(([{ hiddenColumns }, datasetId]) => {
      if (!datasetId) return of();
      return this.api.setHiddenColumns(datasetId, hiddenColumns).pipe(
        catchError(() => of())
      );
    })
  ), { dispatch: false });

  loadColumnOrder$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.openDataset),
    switchMap(({ id }) => this.api.getColumnOrder(id).pipe(
      map(columnOrder => DatasetActions.columnOrderLoaded({ columnOrder })),
      catchError(() => of(DatasetActions.columnOrderLoaded({ columnOrder: [] })))
    ))
  ));

  saveColumnOrder$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.setColumnOrder),
    withLatestFrom(this.store.select(selectCurrentDatasetId)),
    switchMap(([{ columnOrder }, datasetId]) => {
      if (!datasetId) return of();
      return this.api.setColumnOrder(datasetId, columnOrder).pipe(
        catchError(() => of())
      );
    })
  ), { dispatch: false });

  loadColumnWidths$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.openDataset),
    switchMap(({ id }) => this.api.getColumnWidths(id).pipe(
      map(columnWidths => DatasetActions.columnWidthsLoaded({ columnWidths })),
      catchError(() => of(DatasetActions.columnWidthsLoaded({ columnWidths: {} })))
    ))
  ));

  saveColumnWidths$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.setColumnWidths),
    withLatestFrom(this.store.select(selectCurrentDatasetId)),
    switchMap(([{ columnWidths }, datasetId]) => {
      if (!datasetId) return of();
      return this.api.setColumnWidths(datasetId, columnWidths).pipe(
        catchError(() => of())
      );
    })
  ), { dispatch: false });

  /** After schema loads, auto-fetch first page */
  autoLoadFirstPage$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.schemaLoaded),
    map(() => DatasetActions.loadData({ page: 0, size: 100 }))
  ));

  loadData$ = createEffect(() => this.actions$.pipe(
    ofType(DatasetActions.loadData),
    withLatestFrom(this.store.select(selectCurrentDatasetId)),
    switchMap(([{ page, size, sortField, sortOrder, filters }, datasetId]) => {
      if (!datasetId) return of(DatasetActions.dataLoadError({ error: 'No dataset selected' }));
      return this.api.getData(datasetId, page, size, sortField, sortOrder, filters).pipe(
        map(data => DatasetActions.dataLoaded({ data })),
        catchError(err => of(DatasetActions.dataLoadError({ error: err.message })))
      );
    })
  ));
}
