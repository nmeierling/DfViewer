import { inject, Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';
import { Store } from '@ngrx/store';
import { of, switchMap, map, catchError, withLatestFrom, mergeMap } from 'rxjs';
import { CompareActions } from './comparison.actions';
import { selectLeftDatasetId, selectRightDatasetIds, selectKeyColumns, selectIgnoreColumns, selectCompareColumns, selectMultiSelectedColumn } from './comparison.selectors';
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
      catchError(() => of(CompareActions.comparisonError({ rightDatasetId: 0, error: 'Failed to load schema' })))
    ))
  ));

  /** Run comparison for EACH right dataset against the baseline */
  runComparison$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.runComparison),
    withLatestFrom(
      this.store.select(selectLeftDatasetId),
      this.store.select(selectRightDatasetIds),
      this.store.select(selectKeyColumns),
      this.store.select(selectIgnoreColumns)
    ),
    mergeMap(([_, leftId, rightIds, keyColumns, ignoreColumns]) => {
      if (!leftId || rightIds.length === 0 || keyColumns.length === 0) {
        return of(CompareActions.comparisonError({ rightDatasetId: 0, error: 'Select datasets and key columns' }));
      }
      // Dispatch a comparison for each right dataset
      return rightIds.map(rightId =>
        this.api.compare(leftId, rightId, keyColumns, ignoreColumns).pipe(
          switchMap(summary => [
            CompareActions.comparisonComplete({ rightDatasetId: rightId, summary }),
            CompareActions.loadAdded({ rightDatasetId: rightId, page: 0, size: 100 }),
            CompareActions.loadColumnChanges({ rightDatasetId: rightId }),
          ]),
          catchError(err => of(CompareActions.comparisonError({
            rightDatasetId: rightId,
            error: err.error?.message || err.message || 'Comparison failed'
          })))
        )
      ).reduce((acc, obs) => acc.pipe(mergeMap(() => obs)), of(null as any));
    })
  ));

  loadAdded$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadAdded),
    withLatestFrom(this.store.select(selectLeftDatasetId)),
    mergeMap(([{ rightDatasetId, page, size }, leftId]) => {
      if (!leftId) return of(CompareActions.comparisonError({ rightDatasetId, error: 'No left dataset' }));
      return this.api.getCompareAdded(leftId, rightDatasetId, page, size).pipe(
        map(data => CompareActions.addedLoaded({ rightDatasetId, data })),
        catchError(() => of(CompareActions.comparisonError({ rightDatasetId, error: 'Failed to load added rows' })))
      );
    })
  ));

  loadRemoved$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadRemoved),
    withLatestFrom(this.store.select(selectLeftDatasetId)),
    mergeMap(([{ rightDatasetId, page, size }, leftId]) => {
      if (!leftId) return of(CompareActions.comparisonError({ rightDatasetId, error: 'No left dataset' }));
      return this.api.getCompareRemoved(leftId, rightDatasetId, page, size).pipe(
        map(data => CompareActions.removedLoaded({ rightDatasetId, data })),
        catchError(() => of(CompareActions.comparisonError({ rightDatasetId, error: 'Failed to load removed rows' })))
      );
    })
  ));

  loadChanged$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadChanged),
    withLatestFrom(this.store.select(selectLeftDatasetId)),
    mergeMap(([{ rightDatasetId, page, size }, leftId]) => {
      if (!leftId) return of(CompareActions.comparisonError({ rightDatasetId, error: 'No left dataset' }));
      return this.api.getCompareChanged(leftId, rightDatasetId, page, size).pipe(
        map(data => CompareActions.changedLoaded({ rightDatasetId, data })),
        catchError(() => of(CompareActions.comparisonError({ rightDatasetId, error: 'Failed to load changed rows' })))
      );
    })
  ));

  loadColumnChanges$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadColumnChanges),
    withLatestFrom(this.store.select(selectLeftDatasetId)),
    mergeMap(([{ rightDatasetId }, leftId]) => {
      if (!leftId) return of(CompareActions.comparisonError({ rightDatasetId, error: 'No left dataset' }));
      return this.api.getColumnChanges(leftId, rightDatasetId).pipe(
        map(columnChanges => CompareActions.columnChangesLoaded({ rightDatasetId, columnChanges })),
        catchError(() => of(CompareActions.comparisonError({ rightDatasetId, error: 'Failed to load column changes' })))
      );
    })
  ));

  selectColumn$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.selectColumn),
    withLatestFrom(this.store.select(selectLeftDatasetId)),
    mergeMap(([{ rightDatasetId, column }, leftId]) => {
      if (!leftId) return of(CompareActions.comparisonError({ rightDatasetId, error: 'No left dataset' }));
      return this.api.getColumnChangeData(leftId, rightDatasetId, column, 0, 100).pipe(
        map(data => CompareActions.columnDataLoaded({ rightDatasetId, column, data })),
        catchError(() => of(CompareActions.comparisonError({ rightDatasetId, error: `Failed to load changes for ${column}` })))
      );
    })
  ));

  loadColumnPage$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadColumnPage),
    withLatestFrom(this.store.select(selectLeftDatasetId)),
    mergeMap(([{ rightDatasetId, column, page, size }, leftId]) => {
      if (!leftId) return of(CompareActions.comparisonError({ rightDatasetId, error: 'No left dataset' }));
      return this.api.getColumnChangeData(leftId, rightDatasetId, column, page, size).pipe(
        map(data => CompareActions.columnDataLoaded({ rightDatasetId, column, data })),
        catchError(() => of(CompareActions.comparisonError({ rightDatasetId, error: `Failed to load changes for ${column}` })))
      );
    })
  ));

  // Multi-compare effects
  runMultiComparison$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.runMultiComparison),
    withLatestFrom(
      this.store.select(selectLeftDatasetId),
      this.store.select(selectRightDatasetIds),
      this.store.select(selectKeyColumns),
      this.store.select(selectCompareColumns)
    ),
    switchMap(([_, leftId, rightIds, keyColumns, columns]) => {
      if (!leftId || rightIds.length === 0 || keyColumns.length === 0 || columns.length === 0) {
        return of(CompareActions.multiColumnSummaryLoaded({ columnChanges: [] }));
      }
      return this.api.multiCompareColumnSummary(leftId, rightIds, keyColumns, columns).pipe(
        map(columnChanges => CompareActions.multiColumnSummaryLoaded({ columnChanges })),
        catchError(() => of(CompareActions.multiColumnSummaryLoaded({ columnChanges: [] })))
      );
    })
  ));

  selectMultiColumn$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.selectMultiColumn),
    withLatestFrom(
      this.store.select(selectLeftDatasetId),
      this.store.select(selectRightDatasetIds),
      this.store.select(selectKeyColumns)
    ),
    switchMap(([{ column }, leftId, rightIds, keyColumns]) => {
      if (!leftId) return of(CompareActions.multiColumnDataLoaded({ data: { data: [], totalRows: 0, page: 0, pageSize: 100, totalPages: 0 } }));
      return this.api.multiCompareColumn(leftId, rightIds, keyColumns, column, 0, 100).pipe(
        map(data => CompareActions.multiColumnDataLoaded({ data })),
        catchError(() => of(CompareActions.multiColumnDataLoaded({ data: { data: [], totalRows: 0, page: 0, pageSize: 100, totalPages: 0 } })))
      );
    })
  ));

  loadMultiColumn$ = createEffect(() => this.actions$.pipe(
    ofType(CompareActions.loadMultiColumn),
    withLatestFrom(
      this.store.select(selectLeftDatasetId),
      this.store.select(selectRightDatasetIds),
      this.store.select(selectKeyColumns)
    ),
    switchMap(([{ column, page, size }, leftId, rightIds, keyColumns]) => {
      if (!leftId) return of(CompareActions.multiColumnDataLoaded({ data: { data: [], totalRows: 0, page: 0, pageSize: 100, totalPages: 0 } }));
      return this.api.multiCompareColumn(leftId, rightIds, keyColumns, column, page, size).pipe(
        map(data => CompareActions.multiColumnDataLoaded({ data })),
        catchError(() => of(CompareActions.multiColumnDataLoaded({ data: { data: [], totalRows: 0, page: 0, pageSize: 100, totalPages: 0 } })))
      );
    })
  ));
}
