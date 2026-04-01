import { inject, Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { of, switchMap, map, catchError, withLatestFrom } from 'rxjs';
import { CompareActions } from './comparison.actions';
import { selectLeftDatasetId, selectRightDatasetId, selectKeyColumns, selectIgnoreColumns } from './comparison.selectors';
import { ApiService } from '../services/api.service';

@Injectable()
export class ComparisonEffects {
  private actions$ = inject(Actions);
  private api = inject(ApiService);
  private store = inject(Store);

  loadLeftSchema$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.selectLeft),
    switchMap(({ datasetId }) => this.api.getSchema(datasetId).pipe(
      map(columns => CompareActions.leftSchemaLoaded({ columns })),
      catchError(() => of(CompareActions.comparisonError({ error: 'Failed to load schema' })))
    ))
  ));

  runComparison$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.runComparison),
    withLatestFrom(
      this.store.select(selectLeftDatasetId),
      this.store.select(selectRightDatasetId),
      this.store.select(selectKeyColumns),
      this.store.select(selectIgnoreColumns)
    ),
    switchMap(([_, leftId, rightId, keyColumns, ignoreColumns]) => {
      if (!leftId || !rightId || keyColumns.length === 0) {
        return of(CompareActions.comparisonError({ error: 'Select datasets and key columns' }));
      }
      return this.api.compare(leftId, rightId, keyColumns, ignoreColumns).pipe(
        map(summary => CompareActions.comparisonComplete({ summary })),
        catchError(err => of(CompareActions.comparisonError({ error: err.error?.message || err.message || 'Comparison failed' })))
      );
    })
  ));

  /** Auto-load first page + column changes after comparison completes */
  autoLoadTabs$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.comparisonComplete),
    switchMap(() => [
      CompareActions.loadAdded({ page: 0, size: 100 }),
      CompareActions.loadColumnChanges(),
    ])
  ));

  loadAdded$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadAdded),
    withLatestFrom(this.store.select(selectLeftDatasetId), this.store.select(selectRightDatasetId)),
    switchMap(([{ page, size }, leftId, rightId]) => {
      if (!leftId || !rightId) return of(CompareActions.comparisonError({ error: 'No datasets' }));
      return this.api.getCompareAdded(leftId, rightId, page, size).pipe(
        map(data => CompareActions.addedLoaded({ data })),
        catchError(err => of(CompareActions.comparisonError({ error: 'Failed to load added rows' })))
      );
    })
  ));

  loadRemoved$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadRemoved),
    withLatestFrom(this.store.select(selectLeftDatasetId), this.store.select(selectRightDatasetId)),
    switchMap(([{ page, size }, leftId, rightId]) => {
      if (!leftId || !rightId) return of(CompareActions.comparisonError({ error: 'No datasets' }));
      return this.api.getCompareRemoved(leftId, rightId, page, size).pipe(
        map(data => CompareActions.removedLoaded({ data })),
        catchError(err => of(CompareActions.comparisonError({ error: 'Failed to load removed rows' })))
      );
    })
  ));

  loadChanged$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadChanged),
    withLatestFrom(this.store.select(selectLeftDatasetId), this.store.select(selectRightDatasetId)),
    switchMap(([{ page, size }, leftId, rightId]) => {
      if (!leftId || !rightId) return of(CompareActions.comparisonError({ error: 'No datasets' }));
      return this.api.getCompareChanged(leftId, rightId, page, size).pipe(
        map(data => CompareActions.changedLoaded({ data })),
        catchError(err => of(CompareActions.comparisonError({ error: 'Failed to load changed rows' })))
      );
    })
  ));

  loadColumnChanges$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadColumnChanges),
    withLatestFrom(this.store.select(selectLeftDatasetId), this.store.select(selectRightDatasetId)),
    switchMap(([_, leftId, rightId]) => {
      if (!leftId || !rightId) return of(CompareActions.comparisonError({ error: 'No datasets' }));
      return this.api.getColumnChanges(leftId, rightId).pipe(
        map(columnChanges => CompareActions.columnChangesLoaded({ columnChanges })),
        catchError(err => of(CompareActions.comparisonError({ error: 'Failed to load column changes' })))
      );
    })
  ));

  selectColumn$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.selectColumn),
    withLatestFrom(this.store.select(selectLeftDatasetId), this.store.select(selectRightDatasetId)),
    switchMap(([{ column }, leftId, rightId]) => {
      if (!leftId || !rightId) return of(CompareActions.comparisonError({ error: 'No datasets' }));
      return this.api.getColumnChangeData(leftId, rightId, column, 0, 100).pipe(
        map(data => CompareActions.columnDataLoaded({ column, data })),
        catchError(err => of(CompareActions.comparisonError({ error: `Failed to load changes for ${column}` })))
      );
    })
  ));

  loadColumnPage$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadColumnPage),
    withLatestFrom(this.store.select(selectLeftDatasetId), this.store.select(selectRightDatasetId)),
    switchMap(([{ column, page, size }, leftId, rightId]) => {
      if (!leftId || !rightId) return of(CompareActions.comparisonError({ error: 'No datasets' }));
      return this.api.getColumnChangeData(leftId, rightId, column, page, size).pipe(
        map(data => CompareActions.columnDataLoaded({ column, data })),
        catchError(err => of(CompareActions.comparisonError({ error: `Failed to load changes for ${column}` })))
      );
    })
  ));
}
