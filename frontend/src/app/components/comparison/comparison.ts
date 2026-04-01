import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { Store } from '@ngrx/store';
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

      <!-- Dataset selection -->
      <div class="selector-bar">
        <div class="selector">
          <label>Left (baseline)</label>
          <p-select
            [options]="(datasets$ | async) ?? []"
            optionLabel="name"
            optionValue="id"
            [ngModel]="leftId$ | async"
            (ngModelChange)="selectLeft($event)"
            placeholder="Select dataset"
            [filter]="true"
            filterBy="name"
            [style]="{ width: '100%' }"
          />
        </div>
        <div class="selector">
          <label>Right (new)</label>
          <p-select
            [options]="(datasets$ | async) ?? []"
            optionLabel="name"
            optionValue="id"
            [ngModel]="rightId$ | async"
            (ngModelChange)="selectRight($event)"
            placeholder="Select dataset"
            [filter]="true"
            filterBy="name"
            [style]="{ width: '100%' }"
          />
        </div>
        <div class="selector">
          <label>Key columns</label>
          <p-multiselect
            [options]="(leftColumns$ | async) ?? []"
            optionLabel="name"
            optionValue="name"
            [ngModel]="keyColumns$ | async"
            (ngModelChange)="setKeyColumns($event)"
            placeholder="Select key columns"
            [style]="{ width: '100%' }"
          />
        </div>
        <div class="selector">
          <label>Ignore columns</label>
          <p-multiselect
            [options]="(leftColumns$ | async) ?? []"
            optionLabel="name"
            optionValue="name"
            [ngModel]="ignoreColumns$ | async"
            (ngModelChange)="setIgnoreColumns($event)"
            placeholder="Columns to ignore"
            [style]="{ width: '100%' }"
          />
        </div>
        <div class="selector action">
          <p-button
            label="Compare"
            icon="pi pi-bolt"
            (onClick)="compare()"
            [loading]="(comparing$ | async) ?? false"
            [disabled]="!(leftId$ | async) || !(rightId$ | async) || ((keyColumns$ | async)?.length ?? 0) === 0"
          />
        </div>
      </div>

      @if (error$ | async; as error) {
        <p-message severity="error" [text]="error" />
      }

      <!-- Summary cards -->
      @if (summary$ | async; as s) {
        <div class="summary-cards">
          <p-card>
            <div class="stat">
              <span class="stat-label">Left ({{ s.leftName }})</span>
              <span class="stat-value">{{ s.totalLeft | number }}</span>
            </div>
          </p-card>
          <p-card>
            <div class="stat">
              <span class="stat-label">Right ({{ s.rightName }})</span>
              <span class="stat-value">{{ s.totalRight | number }}</span>
            </div>
          </p-card>
          <p-card>
            <div class="stat added">
              <span class="stat-label">Added</span>
              <span class="stat-value">{{ s.addedCount | number }}</span>
            </div>
          </p-card>
          <p-card>
            <div class="stat removed">
              <span class="stat-label">Removed</span>
              <span class="stat-value">{{ s.removedCount | number }}</span>
            </div>
          </p-card>
          <p-card>
            <div class="stat changed">
              <span class="stat-label">Changed</span>
              <span class="stat-value">{{ s.changedCount | number }}</span>
            </div>
          </p-card>
          <p-card>
            <div class="stat">
              <span class="stat-label">Unchanged</span>
              <span class="stat-value">{{ s.unchangedCount | number }}</span>
            </div>
          </p-card>
        </div>

        <!-- Diff tabs -->
        <p-tabs [value]="activeTab" (valueChange)="onTabChange($event)">
          <p-tablist>
            <p-tab [value]="0">Added ({{ s.addedCount | number }})</p-tab>
            <p-tab [value]="1">Removed ({{ s.removedCount | number }})</p-tab>
            <p-tab [value]="2">Changed ({{ s.changedCount | number }})</p-tab>
            <p-tab [value]="3">By Column</p-tab>
          </p-tablist>

          <p-tabpanels>
            <p-tabpanel [value]="0">
              <p-table
                [value]="(addedRows$ | async) ?? []"
                [lazy]="true"
                [paginator]="true"
                [rows]="100"
                [totalRecords]="(addedTotal$ | async) ?? 0"
                [loading]="(tabLoading$ | async) ?? false"
                [scrollable]="true"
                scrollHeight="calc(100vh - 380px)"
                (onLazyLoad)="onAddedPage($event)"
                styleClass="p-datatable-sm p-datatable-gridlines added-table"
              >
                <ng-template #header>
                  <tr>
                    @for (col of addedColumns; track col) {
                      <th>{{ col }}</th>
                    }
                  </tr>
                </ng-template>
                <ng-template #body let-row>
                  <tr>
                    @for (col of addedColumns; track col) {
                      <td>{{ row[col] }}</td>
                    }
                  </tr>
                </ng-template>
              </p-table>
            </p-tabpanel>

            <p-tabpanel [value]="1">
              <p-table
                [value]="(removedRows$ | async) ?? []"
                [lazy]="true"
                [paginator]="true"
                [rows]="100"
                [totalRecords]="(removedTotal$ | async) ?? 0"
                [loading]="(tabLoading$ | async) ?? false"
                [scrollable]="true"
                scrollHeight="calc(100vh - 380px)"
                (onLazyLoad)="onRemovedPage($event)"
                styleClass="p-datatable-sm p-datatable-gridlines removed-table"
              >
                <ng-template #header>
                  <tr>
                    @for (col of removedColumns; track col) {
                      <th>{{ col }}</th>
                    }
                  </tr>
                </ng-template>
                <ng-template #body let-row>
                  <tr>
                    @for (col of removedColumns; track col) {
                      <td>{{ row[col] }}</td>
                    }
                  </tr>
                </ng-template>
              </p-table>
            </p-tabpanel>

            <p-tabpanel [value]="2">
              <p-table
                [value]="(changedRows$ | async) ?? []"
                [lazy]="true"
                [paginator]="true"
                [rows]="100"
                [totalRecords]="(changedTotal$ | async) ?? 0"
                [loading]="(tabLoading$ | async) ?? false"
                [scrollable]="true"
                scrollHeight="calc(100vh - 380px)"
                (onLazyLoad)="onChangedPage($event)"
                styleClass="p-datatable-sm p-datatable-gridlines changed-table"
              >
                <ng-template #header>
                  <tr>
                    @for (col of changedColumns; track col) {
                      <th [class.key-col]="isKeyColumn(col)" [class.left-col]="col.startsWith('left_')" [class.right-col]="col.startsWith('right_')">
                        {{ formatChangedHeader(col) }}
                      </th>
                    }
                  </tr>
                </ng-template>
                <ng-template #body let-row>
                  <tr>
                    @for (col of changedColumns; track col) {
                      <td [class.changed-cell]="isChangedCell(col, row)">
                        {{ row[col] }}
                      </td>
                    }
                  </tr>
                </ng-template>
              </p-table>
            </p-tabpanel>

            <p-tabpanel [value]="3">
              <div class="by-column-view">
                <!-- Column list with change counts -->
                <div class="column-list">
                  <h4>Changed Columns</h4>
                  @for (cc of (columnChanges$ | async) ?? []; track cc.column) {
                    <div
                      class="column-item"
                      [class.selected]="(selectedColumn$ | async) === cc.column"
                      (click)="selectColumn(cc.column)"
                    >
                      <span class="col-name">{{ cc.column }}</span>
                      <p-tag [value]="'' + cc.changedCount" severity="warn" />
                    </div>
                  }
                  @if (((columnChanges$ | async) ?? []).length === 0) {
                    <p class="no-changes">No per-column changes</p>
                  }
                </div>

                <!-- Column detail table -->
                <div class="column-detail">
                  @if (selectedColumn$ | async; as col) {
                    <h4>Changes in "{{ col }}"</h4>
                    <p-table
                      [value]="(columnData$ | async) ?? []"
                      [lazy]="true"
                      [paginator]="true"
                      [rows]="100"
                      [totalRecords]="(columnDataTotal$ | async) ?? 0"
                      [loading]="(tabLoading$ | async) ?? false"
                      [scrollable]="true"
                      scrollHeight="calc(100vh - 380px)"
                      (onLazyLoad)="onColumnPage($event)"
                      styleClass="p-datatable-sm p-datatable-gridlines"
                    >
                      <ng-template #header>
                        <tr>
                          @for (h of columnDetailHeaders; track h) {
                            <th [class.left-col]="h.startsWith('left_')" [class.right-col]="h.startsWith('right_')">
                              {{ formatChangedHeader(h) }}
                            </th>
                          }
                        </tr>
                      </ng-template>
                      <ng-template #body let-row>
                        <tr>
                          @for (h of columnDetailHeaders; track h) {
                            <td [class.changed-cell]="h.startsWith('left_') || h.startsWith('right_')">
                              {{ row[h] }}
                            </td>
                          }
                        </tr>
                      </ng-template>
                    </p-table>
                  } @else {
                    <p class="select-hint">Select a column from the list to see its changes</p>
                  }
                </div>
              </div>
            </p-tabpanel>
          </p-tabpanels>
        </p-tabs>
      }
    </div>
  `,
  styles: [`
    .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
    .selector-bar { display: flex; gap: 1rem; margin-bottom: 1.5rem; flex-wrap: wrap; align-items: flex-end; }
    .selector { flex: 1; min-width: 200px; }
    .selector label { display: block; font-weight: 500; margin-bottom: 0.5rem; font-size: 0.9rem; }
    .selector.action { flex: 0; align-self: flex-end; }
    .summary-cards { display: flex; gap: 0.75rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
    .summary-cards p-card { flex: 1; min-width: 120px; }
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
    .column-item {
      display: flex; justify-content: space-between; align-items: center;
      padding: 0.5rem 0.75rem; border-radius: 6px; cursor: pointer; margin-bottom: 0.25rem;
    }
    .column-item:hover { background: var(--p-surface-hover); }
    .column-item.selected { background: var(--p-primary-50); border: 1px solid var(--p-primary-200); }
    .col-name { font-weight: 500; }
    .no-changes, .select-hint { color: var(--p-text-muted-color); padding: 2rem; text-align: center; }
  `]
})
export class ComparisonComponent implements OnInit {
  private store = inject(Store);
  private router = inject(Router);
  private route = inject(ActivatedRoute);

  datasets$ = this.store.select(selectDatasets);
  leftId$ = this.store.select(Sel.selectLeftDatasetId);
  rightId$ = this.store.select(Sel.selectRightDatasetId);
  leftColumns$ = this.store.select(Sel.selectLeftColumns);
  keyColumns$ = this.store.select(Sel.selectKeyColumns);
  ignoreColumns$ = this.store.select(Sel.selectIgnoreColumns);
  comparing$ = this.store.select(Sel.selectComparing);
  summary$ = this.store.select(Sel.selectSummary);
  error$ = this.store.select(Sel.selectCompareError);
  addedRows$ = this.store.select(Sel.selectAddedRows);
  addedTotal$ = this.store.select(Sel.selectAddedTotal);
  removedRows$ = this.store.select(Sel.selectRemovedRows);
  removedTotal$ = this.store.select(Sel.selectRemovedTotal);
  changedRows$ = this.store.select(Sel.selectChangedRows);
  changedTotal$ = this.store.select(Sel.selectChangedTotal);
  tabLoading$ = this.store.select(Sel.selectTabLoading);
  columnChanges$ = this.store.select(Sel.selectColumnChanges);
  selectedColumn$ = this.store.select(Sel.selectSelectedColumn);
  columnData$ = this.store.select(Sel.selectColumnData);
  columnDataTotal$ = this.store.select(Sel.selectColumnDataTotal);

  activeTab = 0;
  addedColumns: string[] = [];
  removedColumns: string[] = [];
  changedColumns: string[] = [];
  columnDetailHeaders: string[] = [];
  private keyColumnsLocal: string[] = [];

  ngOnInit() {
    this.store.dispatch(DatasetActions.loadDatasets());

    // Restore from URL query params if present
    const params = this.route.snapshot.queryParams;
    if (params['left'] && params['right'] && params['keys']) {
      const leftId = +params['left'];
      const rightId = +params['right'];
      const keyColumns = (params['keys'] as string).split(',');
      const ignoreColumns = params['ignore'] ? (params['ignore'] as string).split(',') : [];

      this.store.dispatch(CompareActions.selectLeft({ datasetId: leftId }));
      this.store.dispatch(CompareActions.selectRight({ datasetId: rightId }));
      this.store.dispatch(CompareActions.setKeyColumns({ keyColumns }));
      if (ignoreColumns.length > 0) {
        this.store.dispatch(CompareActions.setIgnoreColumns({ ignoreColumns }));
      }
      // Auto-compare after schema loads
      setTimeout(() => this.store.dispatch(CompareActions.runComparison()), 500);
    }

    // Derive column lists from first data load
    this.addedRows$.subscribe(rows => {
      if (rows.length > 0 && this.addedColumns.length === 0) {
        this.addedColumns = Object.keys(rows[0]);
      }
    });
    this.removedRows$.subscribe(rows => {
      if (rows.length > 0 && this.removedColumns.length === 0) {
        this.removedColumns = Object.keys(rows[0]);
      }
    });
    this.changedRows$.subscribe(rows => {
      if (rows.length > 0 && this.changedColumns.length === 0) {
        this.changedColumns = Object.keys(rows[0]);
      }
    });
    this.keyColumns$.subscribe(kc => this.keyColumnsLocal = kc);
    this.columnData$.subscribe(rows => {
      if (rows.length > 0) {
        this.columnDetailHeaders = Object.keys(rows[0]);
      }
    });
  }

  goBack() { this.router.navigate(['/']); }

  selectLeft(id: number) { this.store.dispatch(CompareActions.selectLeft({ datasetId: id })); }
  selectRight(id: number) { this.store.dispatch(CompareActions.selectRight({ datasetId: id })); }
  setKeyColumns(cols: string[]) { this.store.dispatch(CompareActions.setKeyColumns({ keyColumns: cols })); }
  setIgnoreColumns(cols: string[]) { this.store.dispatch(CompareActions.setIgnoreColumns({ ignoreColumns: cols })); }

  compare() {
    this.addedColumns = [];
    this.removedColumns = [];
    this.changedColumns = [];

    // Encode selection into URL
    let leftId = 0, rightId = 0, keys: string[] = [], ignore: string[] = [];
    this.leftId$.subscribe(v => leftId = v || 0).unsubscribe();
    this.rightId$.subscribe(v => rightId = v || 0).unsubscribe();
    this.keyColumns$.subscribe(v => keys = v).unsubscribe();
    this.ignoreColumns$.subscribe(v => ignore = v).unsubscribe();

    const queryParams: Record<string, string> = {
      left: '' + leftId,
      right: '' + rightId,
      keys: keys.join(',')
    };
    if (ignore.length > 0) queryParams['ignore'] = ignore.join(',');
    this.router.navigate([], { queryParams, queryParamsHandling: 'replace' });

    this.store.dispatch(CompareActions.runComparison());
  }

  onTabChange(index: any) {
    this.activeTab = index;
    if (index === 1) this.store.dispatch(CompareActions.loadRemoved({ page: 0, size: 100 }));
    if (index === 2) this.store.dispatch(CompareActions.loadChanged({ page: 0, size: 100 }));
  }

  onAddedPage(event: TableLazyLoadEvent) {
    const page = event.first !== undefined ? Math.floor(event.first / 100) : 0;
    this.store.dispatch(CompareActions.loadAdded({ page, size: 100 }));
  }

  onRemovedPage(event: TableLazyLoadEvent) {
    const page = event.first !== undefined ? Math.floor(event.first / 100) : 0;
    this.store.dispatch(CompareActions.loadRemoved({ page, size: 100 }));
  }

  onChangedPage(event: TableLazyLoadEvent) {
    const page = event.first !== undefined ? Math.floor(event.first / 100) : 0;
    this.store.dispatch(CompareActions.loadChanged({ page, size: 100 }));
  }

  selectColumn(column: string) {
    this.columnDetailHeaders = [];
    this.store.dispatch(CompareActions.selectColumn({ column }));
  }

  onColumnPage(event: TableLazyLoadEvent) {
    const page = event.first !== undefined ? Math.floor(event.first / 100) : 0;
    let col = '';
    this.selectedColumn$.subscribe(c => col = c || '').unsubscribe();
    if (col) {
      this.store.dispatch(CompareActions.loadColumnPage({ column: col, page, size: 100 }));
    }
  }

  isKeyColumn(col: string): boolean {
    return this.keyColumnsLocal.includes(col);
  }

  isChangedCell(col: string, row: Record<string, unknown>): boolean {
    if (!col.startsWith('left_') && !col.startsWith('right_')) return false;
    const baseName = col.replace(/^(left_|right_)/, '');
    const leftVal = row['left_' + baseName];
    const rightVal = row['right_' + baseName];
    return leftVal !== rightVal;
  }

  formatChangedHeader(col: string): string {
    if (col.startsWith('left_')) return col.substring(5) + ' (left)';
    if (col.startsWith('right_')) return col.substring(6) + ' (right)';
    return col;
  }
}
