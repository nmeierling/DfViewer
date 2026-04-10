import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { Store } from '@ngrx/store';
import { map } from 'rxjs';
import { SelectModule } from 'primeng/select';
import { MultiSelectModule } from 'primeng/multiselect';
import { ButtonModule } from 'primeng/button';
import { TableModule, TableLazyLoadEvent } from 'primeng/table';
import { TabsModule } from 'primeng/tabs';
import { TagModule } from 'primeng/tag';
import { MessageModule } from 'primeng/message';
import { CardModule } from 'primeng/card';
import { CompareActions } from '../../store/comparison.actions';
import * as Sel from '../../store/comparison.selectors';
import { selectDatasets } from '../../store/dataset.selectors';
import { DatasetActions } from '../../store/dataset.actions';
import { Dataset } from '../../models/dataset.model';

@Component({
  selector: 'app-comparison',
  standalone: true,
  imports: [
    CommonModule, FormsModule, SelectModule, MultiSelectModule, ButtonModule,
    TableModule, TabsModule, TagModule, MessageModule, CardModule
  ],
  template: `
    <div class="comparison-view">
      <div class="header">
        <p-button icon="pi pi-arrow-left" label="Back" [text]="true" (onClick)="goBack()" />
        <h2>Compare Datasets</h2>
      </div>

      <div class="config-section">
        <div class="config-row">
          <div class="config-field" style="flex:2">
            <label>Baseline</label>
            <p-select
              [options]="(availableForLeft$ | async) ?? []"
              optionLabel="name"
              optionValue="id"
              [ngModel]="leftId$ | async"
              (ngModelChange)="selectLeft($event)"
              placeholder="Select baseline dataset"
              [filter]="true" filterBy="name"
              [style]="{ width: '100%' }"
            />
          </div>
          <div class="config-field">
            <label>Key columns</label>
            <p-multiselect
              [options]="(leftColumns$ | async) ?? []"
              optionLabel="name" optionValue="name"
              [ngModel]="keyColumns$ | async"
              (ngModelChange)="setKeyColumns($event)"
              placeholder="Key columns"
              [style]="{ width: '100%' }"
            />
          </div>
          <div class="config-field">
            <label>Ignore columns</label>
            <p-multiselect
              [options]="(leftColumns$ | async) ?? []"
              optionLabel="name" optionValue="name"
              [ngModel]="ignoreColumns$ | async"
              (ngModelChange)="setIgnoreColumns($event)"
              placeholder="Columns to ignore"
              [style]="{ width: '100%' }"
            />
          </div>
        </div>

        <div class="config-row">
          <div class="config-field" style="flex:2">
            <label>Compare against</label>
            <p-multiselect
              [options]="(availableForRight$ | async) ?? []"
              optionLabel="name" optionValue="id"
              [ngModel]="rightDatasetIds$ | async"
              (ngModelChange)="setRightDatasets($event)"
              placeholder="Select datasets to compare"
              [filter]="true" filterBy="name"
              [style]="{ width: '100%' }"
              display="chip"
            />
          </div>
          @if (isMultiMode$ | async) {
            <div class="config-field" style="flex:2">
              <label>Columns to compare</label>
              <p-multiselect
                [options]="(leftColumns$ | async) ?? []"
                optionLabel="name" optionValue="name"
                [ngModel]="compareColumns$ | async"
                (ngModelChange)="setCompareColumns($event)"
                placeholder="Select columns to compare across datasets"
                [style]="{ width: '100%' }"
                display="chip"
              />
            </div>
          }
        </div>

        <div class="config-actions">
          @if (isMultiMode$ | async) {
            <p-button label="Compare All" icon="pi pi-bolt" (onClick)="compareMulti()" [loading]="(multiLoading$ | async) ?? false"
              [disabled]="!(leftId$ | async) || ((rightDatasetIds$ | async) ?? []).length === 0 || ((keyColumns$ | async) ?? []).length === 0 || ((compareColumns$ | async) ?? []).length === 0" />
          } @else {
            <p-button label="Compare" icon="pi pi-bolt" (onClick)="compareSingle()" [loading]="(anyComparing$ | async) ?? false"
              [disabled]="!(leftId$ | async) || ((rightDatasetIds$ | async) ?? []).length === 0 || ((keyColumns$ | async) ?? []).length === 0" />
          }
        </div>
      </div>

      <!-- SINGLE MODE: full comparison view -->
      @if (!(isMultiMode$ | async) && (activeRightId$ | async)) {
        @if (activeError$ | async; as error) {
          <p-message severity="error" [text]="error" />
        }
        @if (activeSummary$ | async; as s) {
          <div class="summary-cards">
            <p-card><div class="stat"><span class="stat-label">Left</span><span class="stat-value">{{ s.totalLeft | number }}</span></div></p-card>
            <p-card><div class="stat"><span class="stat-label">Right</span><span class="stat-value">{{ s.totalRight | number }}</span></div></p-card>
            <p-card><div class="stat added"><span class="stat-label">Added</span><span class="stat-value">{{ s.addedCount | number }}</span></div></p-card>
            <p-card><div class="stat removed"><span class="stat-label">Removed</span><span class="stat-value">{{ s.removedCount | number }}</span></div></p-card>
            <p-card><div class="stat changed"><span class="stat-label">Changed</span><span class="stat-value">{{ s.changedCount | number }}</span></div></p-card>
            <p-card><div class="stat"><span class="stat-label">Unchanged</span><span class="stat-value">{{ s.unchangedCount | number }}</span></div></p-card>
          </div>

          <p-tabs [value]="activeDiffTab" (valueChange)="onDiffTabChange($event)">
            <p-tablist>
              <p-tab [value]="0">Added ({{ s.addedCount | number }})</p-tab>
              <p-tab [value]="1">Removed ({{ s.removedCount | number }})</p-tab>
              <p-tab [value]="2">Changed ({{ s.changedCount | number }})</p-tab>
              <p-tab [value]="3">By Column</p-tab>
            </p-tablist>
            <p-tabpanels>
              <p-tabpanel [value]="0">
                <p-table [value]="(addedRows$ | async) ?? []" [lazy]="true" [paginator]="true" [rows]="100"
                  [totalRecords]="(addedTotal$ | async) ?? 0" [loading]="(tabLoading$ | async) ?? false"
                  [scrollable]="true" scrollHeight="calc(100vh - 420px)" (onLazyLoad)="onAddedPage($event)"
                  styleClass="p-datatable-sm p-datatable-gridlines added-table">
                  <ng-template #header><tr>@for (col of (addedColumns$ | async) ?? []; track col) { <th>{{ col }}</th> }</tr></ng-template>
                  <ng-template #body let-row><tr>@for (col of (addedColumns$ | async) ?? []; track col) { <td>{{ row[col] }}</td> }</tr></ng-template>
                </p-table>
              </p-tabpanel>
              <p-tabpanel [value]="1">
                <p-table [value]="(removedRows$ | async) ?? []" [lazy]="true" [paginator]="true" [rows]="100"
                  [totalRecords]="(removedTotal$ | async) ?? 0" [loading]="(tabLoading$ | async) ?? false"
                  [scrollable]="true" scrollHeight="calc(100vh - 420px)" (onLazyLoad)="onRemovedPage($event)"
                  styleClass="p-datatable-sm p-datatable-gridlines removed-table">
                  <ng-template #header><tr>@for (col of (removedColumns$ | async) ?? []; track col) { <th>{{ col }}</th> }</tr></ng-template>
                  <ng-template #body let-row><tr>@for (col of (removedColumns$ | async) ?? []; track col) { <td>{{ row[col] }}</td> }</tr></ng-template>
                </p-table>
              </p-tabpanel>
              <p-tabpanel [value]="2">
                <p-table [value]="(changedRows$ | async) ?? []" [lazy]="true" [paginator]="true" [rows]="100"
                  [totalRecords]="(changedTotal$ | async) ?? 0" [loading]="(tabLoading$ | async) ?? false"
                  [scrollable]="true" scrollHeight="calc(100vh - 420px)" (onLazyLoad)="onChangedPage($event)"
                  styleClass="p-datatable-sm p-datatable-gridlines changed-table">
                  <ng-template #header><tr>@for (col of (changedColumns$ | async) ?? []; track col) {
                    <th [class.key-col]="(keyColumnSet$ | async)?.has(col)" [class.left-col]="col.startsWith('left_')" [class.right-col]="col.startsWith('right_')">{{ formatChangedHeader(col) }}</th>
                  }</tr></ng-template>
                  <ng-template #body let-row><tr>@for (col of (changedColumns$ | async) ?? []; track col) {
                    <td [class.changed-cell]="isChangedCell(col, row)">{{ row[col] }}</td>
                  }</tr></ng-template>
                </p-table>
              </p-tabpanel>
              <p-tabpanel [value]="3">
                <ng-container *ngTemplateOutlet="byColumnView" />
              </p-tabpanel>
            </p-tabpanels>
          </p-tabs>
        } @else if (activeComparing$ | async) {
          <div class="loading-indicator"><i class="pi pi-spin pi-spinner" style="font-size:1.5rem"></i> Comparing...</div>
        }
      }

      <!-- MULTI MODE: by-column view only -->
      @if ((isMultiMode$ | async) && ((multiColumnChanges$ | async) ?? []).length > 0) {
        <div class="by-column-view">
          <div class="column-list">
            <h4>Changed Columns</h4>
            @if (multiColumnChangesLoading$ | async) {
              <div class="col-loading"><i class="pi pi-spin pi-spinner"></i> Loading...</div>
            } @else {
              @for (cc of (multiColumnChanges$ | async) ?? []; track cc.column) {
                <div class="column-item" [class.selected]="(multiSelectedColumn$ | async) === cc.column" (click)="selectMultiColumn(cc.column)">
                  <span class="col-name">{{ cc.column }}</span>
                  <p-tag [value]="'' + cc.changedCount" severity="warn" />
                </div>
              }
            }
          </div>
          <div class="column-detail">
            @if (multiSelectedColumn$ | async; as col) {
              <h4>{{ col }} across all datasets</h4>
              <p-table [value]="(multiColumnData$ | async) ?? []" [lazy]="true" [paginator]="true" [rows]="100"
                [totalRecords]="(multiColumnDataTotal$ | async) ?? 0" [loading]="(multiLoading$ | async) ?? false"
                [scrollable]="true" scrollHeight="calc(100vh - 380px)" (onLazyLoad)="onMultiColumnPage($event)"
                styleClass="p-datatable-sm p-datatable-gridlines">
                <ng-template #header><tr>@for (h of (multiColumnHeaders$ | async) ?? []; track h) {
                  <th [class.key-col]="(keyColumnSet$ | async)?.has(h)">{{ h }}</th>
                }</tr></ng-template>
                <ng-template #body let-row><tr>@for (h of (multiColumnHeaders$ | async) ?? []; track h) {
                  <td>{{ row[h] }}</td>
                }</tr></ng-template>
              </p-table>
            } @else {
              <p class="select-hint">Select a column from the list</p>
            }
          </div>
        </div>
      } @else if ((isMultiMode$ | async) && (multiLoading$ | async)) {
        <div class="loading-indicator"><i class="pi pi-spin pi-spinner" style="font-size:1.5rem"></i> Comparing...</div>
      }

      <!-- Single mode by-column template (reused) -->
      <ng-template #byColumnView>
        <div class="by-column-view">
          <div class="column-list">
            <h4>Changed Columns</h4>
            @if (columnChangesLoading$ | async) {
              <div class="col-loading"><i class="pi pi-spin pi-spinner"></i> Loading...</div>
            } @else {
              @for (cc of (columnChanges$ | async) ?? []; track cc.column) {
                <div class="column-item" [class.selected]="(selectedColumn$ | async) === cc.column" (click)="selectColumn(cc.column)">
                  <span class="col-name">{{ cc.column }}</span>
                  <p-tag [value]="'' + cc.changedCount" severity="warn" />
                </div>
              }
              @if (((columnChanges$ | async) ?? []).length === 0) {
                <p class="no-changes">No per-column changes</p>
              }
            }
          </div>
          <div class="column-detail">
            @if (selectedColumn$ | async; as col) {
              <h4>Changes in "{{ col }}"</h4>
              <p-table [value]="(columnData$ | async) ?? []" [lazy]="true" [paginator]="true" [rows]="100"
                [totalRecords]="(columnDataTotal$ | async) ?? 0" [loading]="(tabLoading$ | async) ?? false"
                [scrollable]="true" scrollHeight="calc(100vh - 380px)" (onLazyLoad)="onColumnPage($event)"
                styleClass="p-datatable-sm p-datatable-gridlines">
                <ng-template #header><tr>@for (h of (columnDetailHeaders$ | async) ?? []; track h) {
                  <th [class.left-col]="h.startsWith('left_')" [class.right-col]="h.startsWith('right_')">{{ formatChangedHeader(h) }}</th>
                }</tr></ng-template>
                <ng-template #body let-row><tr>@for (h of (columnDetailHeaders$ | async) ?? []; track h) {
                  <td [class.changed-cell]="h.startsWith('left_') || h.startsWith('right_')">{{ row[h] }}</td>
                }</tr></ng-template>
              </p-table>
            } @else {
              <p class="select-hint">Select a column from the list</p>
            }
          </div>
        </div>
      </ng-template>
    </div>
  `,
  styles: [`
    .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
    .config-section { display: flex; flex-direction: column; gap: 1rem; margin-bottom: 1.5rem; padding: 1rem; border: 1px solid var(--p-surface-border); border-radius: 8px; background: var(--p-surface-card); }
    .config-row { display: flex; gap: 1rem; flex-wrap: wrap; }
    .config-field { flex: 1; min-width: 180px; }
    .config-field label { display: block; font-weight: 500; margin-bottom: 0.4rem; font-size: 0.85rem; }
    .config-actions { display: flex; justify-content: flex-end; }
    .summary-cards { display: flex; gap: 0.75rem; margin: 1rem 0; flex-wrap: wrap; }
    .summary-cards p-card { flex: 1; min-width: 100px; }
    .stat { text-align: center; }
    .stat-label { display: block; font-size: 0.8rem; color: var(--p-text-muted-color); }
    .stat-value { display: block; font-size: 1.5rem; font-weight: 700; }
    .added .stat-value { color: var(--p-green-500); }
    .removed .stat-value { color: var(--p-red-500); }
    .changed .stat-value { color: var(--p-orange-500); }
    .key-col { background: var(--p-surface-100); font-weight: 600; }
    .left-col { color: var(--p-red-500); }
    .right-col { color: var(--p-green-500); }
    .changed-cell { background: var(--p-orange-50); }
    :host ::ng-deep .added-table .p-datatable-tbody > tr > td { background: color-mix(in srgb, var(--p-green-50) 30%, transparent); }
    :host ::ng-deep .removed-table .p-datatable-tbody > tr > td { background: color-mix(in srgb, var(--p-red-50) 30%, transparent); }
    .by-column-view { display: flex; gap: 1rem; height: calc(100vh - 380px); }
    .column-list { width: 280px; flex-shrink: 0; border-right: 1px solid var(--p-surface-border); padding-right: 1rem; overflow-y: auto; }
    .column-detail { flex: 1; min-width: 0; }
    .column-item { display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0.75rem; border-radius: 6px; cursor: pointer; margin-bottom: 0.25rem; }
    .column-item:hover { background: var(--p-surface-hover); }
    .column-item.selected { background: var(--p-primary-50); border: 1px solid var(--p-primary-200); }
    .col-name { font-weight: 500; }
    .col-loading { display: flex; align-items: center; gap: 0.5rem; padding: 1rem; color: var(--p-text-muted-color); font-size: 0.9rem; }
    .loading-indicator { display: flex; align-items: center; gap: 0.75rem; padding: 2rem; color: var(--p-text-muted-color); }
    .no-changes, .select-hint { color: var(--p-text-muted-color); padding: 2rem; text-align: center; }
  `]
})
export class ComparisonComponent implements OnInit {
  private store = inject(Store);
  private router = inject(Router);
  private route = inject(ActivatedRoute);

  allDatasets$ = this.store.select(selectDatasets);
  leftId$ = this.store.select(Sel.selectLeftDatasetId);
  rightDatasetIds$ = this.store.select(Sel.selectRightDatasetIds);
  activeRightId$ = this.store.select(Sel.selectActiveRightId);
  leftColumns$ = this.store.select(Sel.selectLeftColumns);
  keyColumns$ = this.store.select(Sel.selectKeyColumns);
  ignoreColumns$ = this.store.select(Sel.selectIgnoreColumns);
  compareColumns$ = this.store.select(Sel.selectCompareColumns);
  isMultiMode$ = this.store.select(Sel.selectIsMultiMode);
  anyComparing$ = this.store.select(Sel.selectAnyComparing);
  chosenIds$ = this.store.select(Sel.selectChosenDatasetIds);

  // Single mode
  activeSummary$ = this.store.select(Sel.selectActiveSummary);
  activeComparing$ = this.store.select(Sel.selectActiveComparing);
  activeError$ = this.store.select(Sel.selectActiveError);
  addedRows$ = this.store.select(Sel.selectActiveAddedRows);
  addedTotal$ = this.store.select(Sel.selectActiveAddedTotal);
  removedRows$ = this.store.select(Sel.selectActiveRemovedRows);
  removedTotal$ = this.store.select(Sel.selectActiveRemovedTotal);
  changedRows$ = this.store.select(Sel.selectActiveChangedRows);
  changedTotal$ = this.store.select(Sel.selectActiveChangedTotal);
  tabLoading$ = this.store.select(Sel.selectActiveTabLoading);
  columnChangesLoading$ = this.store.select(Sel.selectActiveColumnChangesLoading);
  columnChanges$ = this.store.select(Sel.selectActiveColumnChanges);
  selectedColumn$ = this.store.select(Sel.selectActiveSelectedColumn);
  columnData$ = this.store.select(Sel.selectActiveColumnData);
  columnDataTotal$ = this.store.select(Sel.selectActiveColumnDataTotal);
  addedColumns$ = this.store.select(Sel.selectAddedColumns);
  removedColumns$ = this.store.select(Sel.selectRemovedColumns);
  changedColumns$ = this.store.select(Sel.selectChangedColumns);
  columnDetailHeaders$ = this.store.select(Sel.selectColumnDetailHeaders);
  keyColumnSet$ = this.store.select(Sel.selectKeyColumnSet);

  // Multi mode
  multiColumnChanges$ = this.store.select(Sel.selectMultiColumnChanges);
  multiColumnChangesLoading$ = this.store.select(Sel.selectMultiColumnChangesLoading);
  multiSelectedColumn$ = this.store.select(Sel.selectMultiSelectedColumn);
  multiColumnData$ = this.store.select(Sel.selectMultiColumnData);
  multiColumnDataTotal$ = this.store.select(Sel.selectMultiColumnDataTotal);
  multiColumnHeaders$ = this.store.select(Sel.selectMultiColumnHeaders);
  multiLoading$ = this.store.select(Sel.selectMultiLoading);

  // Filtered
  availableForLeft$ = this.allDatasets$.pipe(map(datasets => {
    let chosen: Set<number> = new Set();
    this.chosenIds$.subscribe(c => chosen = c).unsubscribe();
    let leftId: number | null = null;
    this.leftId$.subscribe(id => leftId = id).unsubscribe();
    return datasets.filter(d => d.id === leftId || !chosen.has(d.id));
  }));
  availableForRight$ = this.allDatasets$.pipe(map(datasets => {
    let chosen: Set<number> = new Set();
    this.chosenIds$.subscribe(c => chosen = c).unsubscribe();
    let leftId: number | null = null;
    this.leftId$.subscribe(id => leftId = id).unsubscribe();
    return datasets.filter(d => d.id !== leftId);
  }));

  activeDiffTab = 0;
  private datasetsCache: Dataset[] = [];

  ngOnInit() {
    this.store.dispatch(DatasetActions.loadDatasets());
    this.allDatasets$.subscribe(d => this.datasetsCache = d);

    const params = this.route.snapshot.queryParams;
    if (params['left'] && params['right'] && params['keys']) {
      const leftId = +params['left'];
      const rightIds = (params['right'] as string).split(',').map(Number);
      const keyColumns = (params['keys'] as string).split(',');
      const ignoreColumns = params['ignore'] ? (params['ignore'] as string).split(',') : [];
      const compareColumns = params['cols'] ? (params['cols'] as string).split(',') : [];

      this.store.dispatch(CompareActions.selectLeft({ datasetId: leftId }));
      rightIds.forEach(id => this.store.dispatch(CompareActions.addRight({ datasetId: id })));
      this.store.dispatch(CompareActions.setKeyColumns({ keyColumns }));
      if (ignoreColumns.length > 0) this.store.dispatch(CompareActions.setIgnoreColumns({ ignoreColumns }));
      if (compareColumns.length > 0) this.store.dispatch(CompareActions.setCompareColumns({ columns: compareColumns }));

      setTimeout(() => {
        if (rightIds.length > 1 && compareColumns.length > 0) {
          this.store.dispatch(CompareActions.runMultiComparison());
        } else {
          this.store.dispatch(CompareActions.runComparison());
        }
      }, 500);
    }
  }

  goBack() { this.router.navigate(['/']); }
  selectLeft(id: number) { this.store.dispatch(CompareActions.selectLeft({ datasetId: id })); }
  setKeyColumns(cols: string[]) { this.store.dispatch(CompareActions.setKeyColumns({ keyColumns: cols })); }
  setIgnoreColumns(cols: string[]) { this.store.dispatch(CompareActions.setIgnoreColumns({ ignoreColumns: cols })); }
  setCompareColumns(cols: string[]) { this.store.dispatch(CompareActions.setCompareColumns({ columns: cols })); }

  setRightDatasets(ids: number[]) {
    // Sync: find added/removed
    let current: number[] = [];
    this.rightDatasetIds$.subscribe(c => current = c).unsubscribe();
    const added = ids.filter(id => !current.includes(id));
    const removed = current.filter(id => !ids.includes(id));
    removed.forEach(id => this.store.dispatch(CompareActions.removeRight({ datasetId: id })));
    added.forEach(id => this.store.dispatch(CompareActions.addRight({ datasetId: id })));
  }

  compareSingle() {
    this.updateUrl();
    this.store.dispatch(CompareActions.runComparison());
  }

  compareMulti() {
    this.updateUrl();
    this.store.dispatch(CompareActions.runMultiComparison());
  }

  private updateUrl() {
    let s: any = {};
    this.store.select(Sel.selectComparison).subscribe(st => s = st).unsubscribe();
    const queryParams: Record<string, string> = {
      left: '' + (s.leftDatasetId || 0),
      right: s.rightDatasetIds.join(','),
      keys: s.keyColumns.join(',')
    };
    if (s.ignoreColumns.length > 0) queryParams['ignore'] = s.ignoreColumns.join(',');
    if (s.compareColumns.length > 0) queryParams['cols'] = s.compareColumns.join(',');
    this.router.navigate([], { queryParams, queryParamsHandling: 'replace' });
  }

  // Single mode handlers
  onDiffTabChange(index: any) {
    this.activeDiffTab = index;
    let rid: number | null = null;
    this.activeRightId$.subscribe(id => rid = id).unsubscribe();
    if (!rid) return;
    if (index === 1) this.store.dispatch(CompareActions.loadRemoved({ rightDatasetId: rid, page: 0, size: 100 }));
    if (index === 2) this.store.dispatch(CompareActions.loadChanged({ rightDatasetId: rid, page: 0, size: 100 }));
  }

  onAddedPage(e: TableLazyLoadEvent) { this.dispatchForActive(rid => CompareActions.loadAdded({ rightDatasetId: rid, page: this.p(e), size: 100 })); }
  onRemovedPage(e: TableLazyLoadEvent) { this.dispatchForActive(rid => CompareActions.loadRemoved({ rightDatasetId: rid, page: this.p(e), size: 100 })); }
  onChangedPage(e: TableLazyLoadEvent) { this.dispatchForActive(rid => CompareActions.loadChanged({ rightDatasetId: rid, page: this.p(e), size: 100 })); }

  selectColumn(column: string) { this.dispatchForActive(rid => CompareActions.selectColumn({ rightDatasetId: rid, column })); }

  onColumnPage(e: TableLazyLoadEvent) {
    let col: string | null = null;
    this.selectedColumn$.subscribe(c => col = c).unsubscribe();
    if (col) this.dispatchForActive(rid => CompareActions.loadColumnPage({ rightDatasetId: rid, column: col!, page: this.p(e), size: 100 }));
  }

  // Multi mode handlers
  selectMultiColumn(column: string) { this.store.dispatch(CompareActions.selectMultiColumn({ column })); }

  onMultiColumnPage(e: TableLazyLoadEvent) {
    let col: string | null = null;
    this.multiSelectedColumn$.subscribe(c => col = c).unsubscribe();
    if (col) this.store.dispatch(CompareActions.loadMultiColumn({ column: col, page: this.p(e), size: 100 }));
  }

  // Helpers
  private dispatchForActive(actionFn: (rid: number) => any) {
    let rid: number | null = null;
    this.activeRightId$.subscribe(id => rid = id).unsubscribe();
    if (rid) this.store.dispatch(actionFn(rid));
  }

  private p(e: TableLazyLoadEvent): number {
    return e.first !== undefined ? Math.floor(e.first / 100) : 0;
  }

  isChangedCell(col: string, row: Record<string, unknown>): boolean {
    if (!col.startsWith('left_') && !col.startsWith('right_')) return false;
    const baseName = col.replace(/^(left_|right_)/, '');
    return row['left_' + baseName] !== row['right_' + baseName];
  }

  formatChangedHeader(col: string): string {
    if (col.startsWith('left_')) return col.substring(5) + ' (left)';
    if (col.startsWith('right_')) return col.substring(6) + ' (right)';
    return col;
  }
}
