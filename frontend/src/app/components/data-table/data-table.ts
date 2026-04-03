import { Component, OnInit, OnDestroy, AfterViewChecked, ViewChild, ElementRef, NgZone, ChangeDetectorRef, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute, Router } from '@angular/router';
import { Title } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { Store } from '@ngrx/store';
import { TableModule, TableLazyLoadEvent } from 'primeng/table';
import { ButtonModule } from 'primeng/button';
import { InputTextModule } from 'primeng/inputtext';
import { MessageModule } from 'primeng/message';
import { TagModule } from 'primeng/tag';
import { MultiSelectModule } from 'primeng/multiselect';
import { PopoverModule } from 'primeng/popover';
import { TooltipModule } from 'primeng/tooltip';
import { DialogModule } from 'primeng/dialog';
import { DatasetActions } from '../../store/dataset.actions';
import { ApiService } from '../../services/api.service';
import * as Sel from '../../store/dataset.selectors';

@Component({
  selector: 'app-data-table',
  standalone: true,
  imports: [CommonModule, FormsModule, TableModule, ButtonModule, InputTextModule, MessageModule, TagModule, MultiSelectModule, PopoverModule, TooltipModule, DialogModule],
  template: `
    <div class="data-table-view">
      <div class="header">
        <p-button icon="pi pi-arrow-left" label="Back" [text]="true" (onClick)="goBack()" />
        <h2>{{ (currentDataset$ | async)?.name }}</h2>
        @if (isFiltered$ | async) {
          <span class="row-count">{{ (totalRows$ | async) | number }} / {{ (currentDataset$ | async)?.rowCount | number }} rows</span>
        } @else {
          <span class="row-count">{{ (totalRows$ | async) | number }} rows</span>
        }
        <div class="header-spacer"></div>
        <p-button
          icon="pi pi-tags"
          [label]="showNameBubbles ? 'Hide Labels' : 'Show Labels'"
          [outlined]="true"
          [severity]="showNameBubbles ? 'primary' : 'secondary'"
          size="small"
          (onClick)="toggleNameBubbles()"
        />
        <p-button
          icon="pi pi-eye"
          label="Columns"
          [outlined]="true"
          size="small"
          [badge]="hiddenCount > 0 ? '' + hiddenCount : ''"
          badgeSeverity="warn"
          (onClick)="colPanel.toggle($event)"
        />
      </div>

      <p-popover #colPanel [style]="{ width: '500px', maxHeight: 'calc(100vh - 100px)' }">
        <div class="col-panel">
          <div class="col-panel-header">
            <span>Columns</span>
            <div>
              <p-button label="Reset Order" [text]="true" size="small" (onClick)="resetColumnOrder()" />
              <p-button label="Show All" [text]="true" size="small" (onClick)="showAllColumns()" />
            </div>
          </div>
          <div class="col-panel-list">
            @for (col of orderedAllColumns; track col.name; let i = $index) {
              <div class="col-panel-item"
                [class.null-col]="isNullColumn(col.name)"
                [class.drag-over]="dragOverIndex === i"
                draggable="true"
                (dragstart)="onDragStart(i)"
                (dragover)="onDragOverCol($event, i)"
                (dragleave)="dragOverIndex = -1"
                (drop)="onDropCol($event, i)"
                (dragend)="dragOverIndex = -1"
              >
                <i class="pi pi-bars drag-handle"></i>
                <i [class]="isColumnVisible(col.name) ? 'pi pi-eye' : 'pi pi-eye-slash'" [class.muted]="!isColumnVisible(col.name)" (click)="toggleColumn(col.name)"></i>
                <span [class.muted]="!isColumnVisible(col.name)" (click)="toggleColumn(col.name)">{{ col.name }}</span>
                @if (isNullColumn(col.name)) {
                  <span class="null-badge">null</span>
                }
              </div>
            }
          </div>
        </div>
      </p-popover>

      @if (hiddenManualColumns.length > 0) {
        <div class="hidden-columns-bar">
          <span class="hidden-label">Hidden:</span>
          @for (col of hiddenManualColumns; track col) {
            <p-tag [value]="col" severity="warn" (click)="toggleColumn(col)" styleClass="clickable-tag" />
          }
          @if ((nullColumns$ | async)?.length) {
            <span class="hidden-label" style="margin-left: 0.5rem">All null:</span>
            @for (col of (nullColumns$ | async) ?? []; track col) {
              <p-tag [value]="col" severity="secondary" />
            }
          }
        </div>
      } @else if ((nullColumns$ | async)?.length) {
        <div class="hidden-columns-bar">
          <span class="hidden-label">Hidden (all null):</span>
          @for (col of (nullColumns$ | async) ?? []; track col) {
            <p-tag [value]="col" severity="secondary" />
          }
        </div>
      }

      @if (showNameBubbles) {
        <div class="bubble-container" #bubbleContainer>
          @for (b of bubbles; track b.colName) {
            <div class="name-bubble" [style.left.px]="b.x" [style.top.px]="b.row * 28">
              {{ b.colName }}
              <svg class="bubble-arrow" [style.left.px]="b.arrowX" [attr.height]="b.arrowHeight">
                <line x1="0" [attr.y1]="0" x2="0" [attr.y2]="b.arrowHeight" stroke="var(--p-primary-color)" stroke-width="1" stroke-dasharray="3,2" />
              </svg>
            </div>
          }
        </div>
      }

      @if (error$ | async; as error) {
        <p-message severity="error" [text]="error" />
      } @else {
        <p-table #dataTable
          [value]="(rows$ | async) ?? []"
          [lazy]="true"
          [paginator]="true"
          [rows]="pageSize"
          [rowsPerPageOptions]="[25, 50, 100, 250, 500]"
          [totalRecords]="(totalRows$ | async) ?? 0"
          [loading]="(loading$ | async) ?? false"
          [scrollable]="true"
          scrollHeight="calc(100vh - 200px)"
          [resizableColumns]="true"
          columnResizeMode="expand"
          (onColResize)="onColResize($event)"
          (onLazyLoad)="onLazyLoad($event)"
          (onPage)="onPageChange($event)"
          styleClass="p-datatable-sm p-datatable-gridlines p-datatable-striped"
        >
          <ng-template #header>
            <tr>
              @for (col of (visibleColumns$ | async) ?? []; track col.name) {
                <th pResizableColumn [style.min-width.px]="10">
                  <div class="col-header" [class.sorted]="currentSort?.field === col.name">
                    <span class="col-header-name">
                      {{ col.name }}
                      @if (currentSort?.field === col.name) {
                        <i [class]="currentSort!.order === 'ASC' ? 'pi pi-arrow-up sort-icon' : 'pi pi-arrow-down sort-icon'"></i>
                      }
                    </span>
                    <i class="pi pi-ellipsis-v col-settings-btn" (click)="openColSettings($event, col.name)"></i>
                  </div>
                </th>
              }
            </tr>
            <tr>
              @for (col of (visibleColumns$ | async) ?? []; track col.name) {
                <th>
                  <input
                    pInputText
                    type="text"
                    [placeholder]="'Filter (' + col.name + ')'"
                    pTooltip="Special: $null, $notnull, $empty, $notempty"
                    tooltipPosition="top"
                    (input)="onFilter(col.name, $event)"
                    [class.filter-active]="!!filters[col.name]"
                    style="width: 100%"
                  />
                </th>
              }
            </tr>
          </ng-template>
          <ng-template #body let-row>
            <tr>
              @for (col of (visibleColumns$ | async) ?? []; track col.name) {
                <td>{{ row[col.name] }}</td>
              }
            </tr>
          </ng-template>
          <ng-template #emptymessage>
            <tr><td [attr.colspan]="(visibleColumns$ | async)?.length ?? 1" style="text-align: center">No data</td></tr>
          </ng-template>
        </p-table>
      }

      <!-- Column settings popover -->
      <p-popover #colSettingsPanel [style]="{ width: '200px' }">
        <div class="col-settings">
          <div class="col-settings-title">{{ activeColName }}</div>
          <div class="col-settings-actions">
            <div class="sort-btn" [class.active]="currentSort?.field === activeColName && currentSort?.order === 'ASC'" (click)="sortColumn('ASC')">
              <i class="pi pi-sort-amount-up"></i> Sort A-Z
            </div>
            <div class="sort-btn" [class.active]="currentSort?.field === activeColName && currentSort?.order === 'DESC'" (click)="sortColumn('DESC')">
              <i class="pi pi-sort-amount-down"></i> Sort Z-A
            </div>
            <div class="sort-btn" (click)="sortColumn(null)">
              <i class="pi pi-times"></i> Clear Sort
            </div>
            <hr style="margin: 0.25rem 0; border-color: var(--p-surface-border)">
            <div class="sort-btn" (click)="showDistinct()">
              <i class="pi pi-list"></i> Distinct Values
            </div>
            <div class="sort-btn" (click)="toggleColumn(activeColName)">
              <i class="pi pi-eye-slash"></i> Hide Column
            </div>
            @if (columnWidthMap[activeColName]) {
              <div class="sort-btn" (click)="clearColumnWidth()">
                <i class="pi pi-arrows-h"></i> Reset Width
              </div>
            }
          </div>
        </div>
      </p-popover>

      <!-- Distinct values modal -->
      <p-dialog [header]="'Distinct: ' + distinctColName" [(visible)]="showDistinctDialog" [modal]="true" [style]="{ width: '500px', maxHeight: '80vh' }">
        <div class="distinct-dialog">
          @if (distinctLoading) {
            <p>Loading...</p>
          } @else {
            <div class="distinct-summary">
              <span>{{ distinctValues.length }} unique values @if (distinctValues.length >= distinctLimit) { <span class="distinct-capped" (click)="limitPanel.toggle($event)">(showing top {{ distinctLimit }})</span> }</span>
              <p-button icon="pi pi-copy" [label]="copiedDistinct ? 'Copied!' : 'Copy'" [text]="true" size="small" (onClick)="copyDistinct()" />
            </div>
            <div class="distinct-list">
              @for (item of distinctValues; track item.value) {
                <div class="distinct-row" (click)="filterByDistinct(item.value)">
                  <span class="distinct-val">{{ item.value === null ? '(null)' : item.value === '' ? '(empty)' : item.value }}</span>
                  <span class="distinct-count">{{ item.count | number }}</span>
                </div>
              }
            </div>
          }
        </div>
      </p-dialog>

      <p-popover #limitPanel [style]="{ width: '200px' }">
        <div class="limit-panel">
          <label>Max results</label>
          <input pInputText type="number" [(ngModel)]="distinctLimitInput" (keyup.enter)="applyDistinctLimit(limitPanel)" style="width: 100%" />
          <p-button label="Apply" size="small" (onClick)="applyDistinctLimit(limitPanel)" [style]="{ width: '100%' }" />
        </div>
      </p-popover>
    </div>
  `,
  styles: [`
    .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 0.5rem; }
    .header-spacer { flex: 1; }
    .row-count { color: var(--p-text-muted-color); font-size: 0.9rem; }
    .hidden-columns-bar {
      display: flex; align-items: center; gap: 0.4rem; flex-wrap: wrap;
      margin-bottom: 0.75rem; font-size: 0.85rem;
    }
    .hidden-label { color: var(--p-text-muted-color); font-weight: 500; margin-right: 0.25rem; }
    :host ::ng-deep .clickable-tag { cursor: pointer; }
    :host ::ng-deep .clickable-tag:hover { opacity: 0.7; }
    .col-panel { display: flex; flex-direction: column; gap: 0.5rem; }
    .col-panel-header { display: flex; justify-content: space-between; align-items: center; }
    .col-panel-header span { font-weight: 600; font-size: 0.9rem; }
    .col-panel-list { max-height: calc(100vh - 180px); overflow-y: auto; display: flex; flex-direction: column; }
    .col-panel-item {
      display: flex; align-items: center; gap: 0.5rem; padding: 0.35rem 0.5rem;
      cursor: pointer; border-radius: 4px; font-size: 0.85rem;
    }
    .col-panel-item:hover { background: var(--p-surface-hover); }
    .col-panel-item.drag-over { border-top: 2px solid var(--p-primary-color); }
    .col-panel-item .muted { opacity: 0.4; }
    .drag-handle { cursor: grab; opacity: 0.3; font-size: 0.75rem; }
    .null-col { font-style: italic; }
    .null-badge { font-size: 0.7rem; color: var(--p-text-muted-color); margin-left: auto; background: var(--p-surface-100); padding: 0.1rem 0.4rem; border-radius: 4px; }
    .col-header { display: flex; align-items: center; gap: 0.25rem; }
    .col-header.sorted { background: color-mix(in srgb, var(--p-primary-color) 8%, transparent); border-radius: 4px; padding: 0 0.25rem; }
    .col-header-name { flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .sort-icon { font-size: 0.7rem; margin-left: 0.25rem; color: var(--p-primary-color); }
    :host ::ng-deep .filter-active { background: var(--p-primary-50) !important; border-color: var(--p-primary-color) !important; }
    .col-settings-btn { cursor: pointer; opacity: 0.4; font-size: 0.75rem; padding: 0.25rem; flex-shrink: 0; }
    .col-settings-btn:hover { opacity: 1; }
    .col-settings-title { font-weight: 600; font-size: 0.9rem; margin-bottom: 0.5rem; }
    .col-settings-actions { display: flex; flex-direction: column; }
    .sort-btn {
      display: flex; align-items: center; gap: 0.5rem; padding: 0.4rem 0.5rem;
      cursor: pointer; border-radius: 4px; font-size: 0.85rem;
    }
    .sort-btn:hover { background: var(--p-surface-hover); }
    .sort-btn.active { background: var(--p-primary-50); color: var(--p-primary-color); font-weight: 600; }
    .distinct-dialog { display: flex; flex-direction: column; gap: 0.5rem; }
    .distinct-summary { font-size: 0.85rem; color: var(--p-text-muted-color); display: flex; justify-content: space-between; align-items: center; }
    .distinct-list { max-height: 50vh; overflow-y: auto; }
    .distinct-row {
      display: flex; justify-content: space-between; align-items: center;
      padding: 0.3rem 0.5rem; border-radius: 4px; cursor: pointer; font-size: 0.85rem;
    }
    .distinct-row:hover { background: var(--p-surface-hover); }
    .distinct-val { flex: 1; word-break: break-all; }
    .distinct-count { color: var(--p-text-muted-color); margin-left: 1rem; white-space: nowrap; }
    .distinct-capped { color: var(--p-orange-500); font-weight: 500; cursor: pointer; text-decoration: underline; }
    .distinct-capped:hover { color: var(--p-orange-700); }
    .limit-panel { display: flex; flex-direction: column; gap: 0.5rem; }
    .limit-panel label { font-size: 0.85rem; font-weight: 500; }
    .bubble-container { position: relative; min-height: 30px; margin-bottom: 0.25rem; }
    .name-bubble {
      position: absolute;
      background: var(--p-primary-50);
      border: 1px solid var(--p-primary-200);
      color: var(--p-primary-700);
      padding: 0.15rem 0.5rem;
      border-radius: 4px;
      font-size: 0.75rem;
      font-weight: 600;
      white-space: nowrap;
      z-index: 1;
    }
    .bubble-arrow {
      position: absolute;
      bottom: -1px;
      transform: translateY(100%);
      width: 1px;
      overflow: visible;
    }
  `]
})
export class DataTableComponent implements OnInit, OnDestroy, AfterViewChecked {
  private store = inject(Store);
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private titleService = inject(Title);
  private api = inject(ApiService);
  private el = inject(ElementRef);
  private ngZone = inject(NgZone);
  private cdr = inject(ChangeDetectorRef);

  @ViewChild('colSettingsPanel') colSettingsPanel: any;
  @ViewChild('dataTable') dataTable: any;

  currentDataset$ = this.store.select(Sel.selectCurrentDataset);
  columns$ = this.store.select(Sel.selectColumns);
  visibleColumns$ = this.store.select(Sel.selectVisibleColumns);
  nullColumns$ = this.store.select(Sel.selectNullColumns);
  hiddenColumns$ = this.store.select(Sel.selectHiddenColumns);
  rows$ = this.store.select(Sel.selectRows);
  totalRows$ = this.store.select(Sel.selectTotalRows);
  loading$ = this.store.select(Sel.selectDataLoading);
  isFiltered$ = this.store.select(Sel.selectFiltered);
  error$ = this.store.select(Sel.selectDatasetError);

  pageSize = 100;
  private datasetId!: number;
  filters: Record<string, string> = {};
  private filterTimeout: ReturnType<typeof setTimeout> | null = null;

  // Local copies for the column panel
  allColumns: { name: string; type: string }[] = [];
  orderedAllColumns: { name: string; type: string }[] = [];
  hiddenManualColumns: string[] = [];
  hiddenCount = 0;
  private nullColumnsLocal: string[] = [];
  private columnOrderLocal: string[] = [];

  // Drag and drop
  dragOverIndex = -1;
  private dragStartIndex = -1;

  // Column settings popover
  activeColName = '';
  currentSort: { field: string; order: string } | null = null;

  // Column widths
  columnWidths$ = this.store.select(Sel.selectColumnWidths);
  columnWidthMap: Record<string, number> = {};
  private resizeTimeout: ReturnType<typeof setTimeout> | null = null;

  // Name bubbles
  showNameBubbles = false;
  bubbles: { colName: string; x: number; row: number; arrowX: number; arrowHeight: number }[] = [];
  private bubblesDirty = false;

  // Distinct values
  showDistinctDialog = false;
  distinctColName = '';
  distinctValues: { value: unknown; count: number }[] = [];
  distinctLoading = false;
  copiedDistinct = false;
  distinctLimit = 500;
  distinctLimitInput = 500;

  constructor() {
    this.currentDataset$.subscribe(ds => {
      if (ds) this.titleService.setTitle(`${ds.name} - DfViewer`);
    });
    this.columns$.subscribe(cols => {
      this.allColumns = [...cols];
      this.recomputeOrderedColumns();
    });
    this.store.select(Sel.selectColumnOrder).subscribe(order => {
      this.columnOrderLocal = [...order];
      this.recomputeOrderedColumns();
    });
    this.hiddenColumns$.subscribe(h => {
      this.hiddenManualColumns = [...h];
      this.hiddenCount = h.length;
    });
    this.nullColumns$.subscribe(n => this.nullColumnsLocal = [...n]);
    this.columnWidths$.subscribe(w => {
      this.columnWidthMap = { ...w };
    });
    // Apply widths after rows load (table is fully rendered at that point)
    this.rows$.subscribe(rows => {
      if (rows.length > 0 && Object.keys(this.columnWidthMap).length > 0) {
        setTimeout(() => this.applyColumnWidths(), 200);
      }
    });
  }

  ngOnInit() {
    this.datasetId = +this.route.snapshot.paramMap.get('id')!;
    const savedPageSize = localStorage.getItem(`dfviewer_pagesize_${this.datasetId}`);
    if (savedPageSize) this.pageSize = +savedPageSize;
    this.store.dispatch(DatasetActions.openDataset({ id: this.datasetId }));
  }

  ngAfterViewChecked() {
    if (this.showNameBubbles && this.bubblesDirty) {
      this.bubblesDirty = false;
      this.ngZone.runOutsideAngular(() => {
        requestAnimationFrame(() => {
          this.computeBubbles();
          this.cdr.detectChanges();
        });
      });
    }
  }

  ngOnDestroy() {
    this.titleService.setTitle('DfViewer');
    if (this.widthStyleEl) {
      this.widthStyleEl.remove();
      this.widthStyleEl = null;
    }
  }

  toggleNameBubbles() {
    this.showNameBubbles = !this.showNameBubbles;
    if (this.showNameBubbles) {
      this.bubblesDirty = true;
    } else {
      this.bubbles = [];
    }
  }

  private computeBubbles() {
    const tableEl = this.el.nativeElement.querySelector('.p-datatable-thead');
    if (!tableEl) return;

    const headerRow = tableEl.querySelector('tr');
    if (!headerRow) return;

    const ths = Array.from(headerRow.querySelectorAll('th')) as HTMLElement[];
    const containerEl = this.el.nativeElement.querySelector('.bubble-container') as HTMLElement;
    if (!containerEl) return;

    const containerRect = containerEl.getBoundingClientRect();

    // Measure which columns have truncated names
    const candidates: { colName: string; colCenterX: number; colWidth: number }[] = [];

    ths.forEach(th => {
      const nameSpan = th.querySelector('.col-header-name') as HTMLElement;
      if (!nameSpan) return;
      const colName = nameSpan.textContent?.trim() || '';
      const thRect = th.getBoundingClientRect();
      const colCenterX = thRect.left - containerRect.left + thRect.width / 2;

      // Check if text is truncated: scrollWidth > clientWidth
      if (nameSpan.scrollWidth > nameSpan.clientWidth + 2) {
        candidates.push({ colName, colCenterX, colWidth: thRect.width });
      }
    });

    if (candidates.length === 0) {
      this.bubbles = [];
      containerEl.style.minHeight = '0';
      return;
    }

    // Measure bubble widths using a temp element
    const measureEl = document.createElement('span');
    measureEl.style.cssText = 'position:absolute;visibility:hidden;font-size:0.75rem;font-weight:600;padding:0 0.5rem;white-space:nowrap';
    document.body.appendChild(measureEl);

    const items = candidates.map(c => {
      measureEl.textContent = c.colName;
      const bubbleWidth = measureEl.offsetWidth + 12; // padding
      return { ...c, bubbleWidth };
    });
    document.body.removeChild(measureEl);

    // Layout: place bubbles, detect overlaps, push to next row
    const placed: { colName: string; x: number; width: number; row: number; colCenterX: number }[] = [];
    const ROW_HEIGHT = 28;

    for (const item of items) {
      let x = item.colCenterX - item.bubbleWidth / 2;
      x = Math.max(0, x); // don't go off left edge

      let row = 0;
      let fits = false;
      while (!fits && row < 5) {
        fits = true;
        for (const p of placed) {
          if (p.row !== row) continue;
          const overlap = !(x + item.bubbleWidth + 4 < p.x || x > p.x + p.width + 4);
          if (overlap) { fits = false; break; }
        }
        if (!fits) row++;
      }

      placed.push({ colName: item.colName, x, width: item.bubbleWidth, row, colCenterX: item.colCenterX });
    }

    const maxRow = placed.reduce((m, p) => Math.max(m, p.row), 0);
    const containerHeight = (maxRow + 1) * ROW_HEIGHT + 4;
    containerEl.style.minHeight = containerHeight + 'px';

    this.bubbles = placed.map(p => ({
      colName: p.colName,
      x: p.x,
      row: p.row,
      arrowX: p.colCenterX - p.x,
      arrowHeight: containerHeight - p.row * ROW_HEIGHT - ROW_HEIGHT + 4
    }));
  }

  onPageChange(event: any) {
    if (event.rows && event.rows !== this.pageSize) {
      this.pageSize = event.rows;
      localStorage.setItem(`dfviewer_pagesize_${this.datasetId}`, '' + this.pageSize);
    }
  }

  isNullColumn(name: string): boolean {
    return this.nullColumnsLocal.includes(name);
  }

  isColumnVisible(name: string): boolean {
    return !this.hiddenManualColumns.includes(name) && !this.nullColumnsLocal.includes(name);
  }

  toggleColumn(name: string) {
    const current = [...this.hiddenManualColumns];
    const idx = current.indexOf(name);
    if (idx >= 0) {
      current.splice(idx, 1);
    } else {
      current.push(name);
    }
    this.store.dispatch(DatasetActions.setHiddenColumns({ hiddenColumns: current }));
  }

  showAllColumns() {
    this.store.dispatch(DatasetActions.setHiddenColumns({ hiddenColumns: [] }));
  }

  private recomputeOrderedColumns() {
    if (this.allColumns.length === 0) return;
    if (this.columnOrderLocal.length === 0) {
      this.orderedAllColumns = [...this.allColumns];
    } else {
      const orderIndex = new Map(this.columnOrderLocal.map((name, i) => [name, i]));
      this.orderedAllColumns = [...this.allColumns].sort((a, b) => {
        const ia = orderIndex.get(a.name) ?? 999999;
        const ib = orderIndex.get(b.name) ?? 999999;
        return ia - ib;
      });
    }
  }

  onDragStart(index: number) {
    this.dragStartIndex = index;
  }

  onDragOverCol(event: DragEvent, index: number) {
    event.preventDefault();
    this.dragOverIndex = index;
  }

  onDropCol(event: DragEvent, dropIndex: number) {
    event.preventDefault();
    this.dragOverIndex = -1;
    if (this.dragStartIndex === dropIndex) return;

    const cols = [...this.orderedAllColumns];
    const [moved] = cols.splice(this.dragStartIndex, 1);
    cols.splice(dropIndex, 0, moved);
    this.orderedAllColumns = cols;

    const newOrder = cols.map(c => c.name);
    this.columnOrderLocal = newOrder;
    this.store.dispatch(DatasetActions.setColumnOrder({ columnOrder: newOrder }));
  }

  resetColumnOrder() {
    this.store.dispatch(DatasetActions.setColumnOrder({ columnOrder: [] }));
    this.orderedAllColumns = [...this.allColumns];
    this.columnOrderLocal = [];
  }

  onLazyLoad(event: TableLazyLoadEvent) {
    const page = event.first !== undefined ? Math.floor(event.first / this.pageSize) : 0;
    const sortField = event.sortField as string | undefined;
    const sortOrder = event.sortOrder === -1 ? 'DESC' : 'ASC';
    this.store.dispatch(DatasetActions.loadData({
      page, size: this.pageSize, sortField, sortOrder, filters: Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined
    }));
  }

  onFilter(columnName: string, event: Event) {
    const value = (event.target as HTMLInputElement).value;
    if (value) {
      this.filters = { ...this.filters, [columnName]: value };
    } else {
      const { [columnName]: _, ...rest } = this.filters;
      this.filters = rest;
    }
    if (this.filterTimeout) clearTimeout(this.filterTimeout);
    this.filterTimeout = setTimeout(() => {
      this.store.dispatch(DatasetActions.loadData({
        page: 0, size: this.pageSize,
        sortField: this.currentSort?.field,
        sortOrder: this.currentSort?.order,
        filters: Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined
      }));
    }, 400);
  }

  private widthStyleEl: HTMLStyleElement | null = null;

  private applyColumnWidths() {
    const nativeEl = this.el.nativeElement;

    // Find PrimeNG's table id
    const tableWrapper = nativeEl.querySelector('[id$="-table"]') as HTMLElement;
    if (!tableWrapper) return;
    const tableId = tableWrapper.id; // e.g. "pr_id_5-table"

    // Get column names from header to map index -> name
    const headerRow = nativeEl.querySelector('.p-datatable-thead tr');
    if (!headerRow) return;
    const ths = Array.from(headerRow.querySelectorAll('th')) as HTMLElement[];
    const colNames = ths.map(th => {
      const nameSpan = th.querySelector('.col-header-name') as HTMLElement;
      return nameSpan?.textContent?.trim() || '';
    });

    // Build CSS rules matching PrimeNG's own approach
    let css = '';
    colNames.forEach((colName, index) => {
      const width = this.columnWidthMap[colName];
      if (width) {
        const style = `width: ${width}px !important; max-width: ${width}px !important`;
        css += `
          #${tableId} > .p-datatable-thead > tr > th:nth-child(${index + 1}),
          #${tableId} > .p-datatable-tbody > tr > td:nth-child(${index + 1}),
          #${tableId} > .p-datatable-tfoot > tr > td:nth-child(${index + 1}) { ${style} }
        `;
      }
    });

    if (!css) return;

    // Remove old style element
    if (this.widthStyleEl) {
      this.widthStyleEl.remove();
    }

    // Inject style element
    this.widthStyleEl = document.createElement('style');
    this.widthStyleEl.innerHTML = css;
    document.head.appendChild(this.widthStyleEl);
  }

  onColResize(event: any) {
    const th = event.element as HTMLElement;
    const colName = th.querySelector('.col-header-name')?.textContent?.trim();
    if (colName) {
      const width = th.offsetWidth;
      this.columnWidthMap = { ...this.columnWidthMap, [colName]: width };
      if (this.resizeTimeout) clearTimeout(this.resizeTimeout);
      this.resizeTimeout = setTimeout(() => {
        this.store.dispatch(DatasetActions.setColumnWidths({ columnWidths: { ...this.columnWidthMap } }));
      }, 500);
    }
    if (this.showNameBubbles) this.bubblesDirty = true;
  }

  // Column settings popover
  openColSettings(event: Event, colName: string) {
    event.stopPropagation();
    this.activeColName = colName;
    this.colSettingsPanel.toggle(event);
  }

  sortColumn(order: string | null) {
    this.colSettingsPanel.hide();
    if (order) {
      this.currentSort = { field: this.activeColName, order };
      this.store.dispatch(DatasetActions.loadData({
        page: 0, size: this.pageSize, sortField: this.activeColName, sortOrder: order,
        filters: Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined
      }));
    } else {
      this.currentSort = null;
      this.store.dispatch(DatasetActions.loadData({
        page: 0, size: this.pageSize,
        filters: Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined
      }));
    }
  }

  clearColumnWidth() {
    this.colSettingsPanel.hide();
    const { [this.activeColName]: _, ...rest } = this.columnWidthMap;
    this.columnWidthMap = rest;
    this.store.dispatch(DatasetActions.setColumnWidths({ columnWidths: { ...rest } }));
    // Remove and re-apply style element
    if (this.widthStyleEl) {
      this.widthStyleEl.remove();
      this.widthStyleEl = null;
    }
    if (Object.keys(rest).length > 0) {
      setTimeout(() => this.applyColumnWidths(), 50);
    }
  }

  showDistinct() {
    this.colSettingsPanel.hide();
    this.distinctColName = this.activeColName;
    this.loadDistinctValues();
  }

  private loadDistinctValues() {
    this.distinctValues = [];
    this.distinctLoading = true;
    this.showDistinctDialog = true;
    const currentFilters = Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined;
    this.api.getDistinctValues(this.datasetId, this.distinctColName, currentFilters, this.distinctLimit).subscribe({
      next: (values) => {
        this.distinctValues = values;
        this.distinctLoading = false;
      },
      error: () => { this.distinctLoading = false; }
    });
  }

  applyDistinctLimit(panel: any) {
    panel.hide();
    this.distinctLimit = this.distinctLimitInput || 500;
    this.loadDistinctValues();
  }

  copyDistinct() {
    const text = this.distinctValues
      .map(item => `${item.value === null ? '(null)' : item.value === '' ? '(empty)' : item.value},${item.count}`)
      .join('\n');
    navigator.clipboard.writeText(text);
    this.copiedDistinct = true;
    setTimeout(() => this.copiedDistinct = false, 2000);
  }

  filterByDistinct(value: unknown) {
    this.showDistinctDialog = false;
    if (value === null) {
      this.filters = { ...this.filters, [this.distinctColName]: '$null' };
    } else if (value === '') {
      this.filters = { ...this.filters, [this.distinctColName]: '$empty' };
    } else {
      this.filters = { ...this.filters, [this.distinctColName]: '' + value };
    }
    this.store.dispatch(DatasetActions.loadData({
      page: 0, size: this.pageSize,
      sortField: this.currentSort?.field,
      sortOrder: this.currentSort?.order,
      filters: { ...this.filters }
    }));
  }

  goBack() {
    this.router.navigate(['/']);
  }
}
