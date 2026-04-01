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
