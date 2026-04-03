import { Component, OnInit, OnDestroy, ViewChild, ElementRef, ChangeDetectionStrategy, ChangeDetectorRef, inject } from '@angular/core';
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
import { SelectModule } from 'primeng/select';
import { DatasetActions } from '../../store/dataset.actions';
import { ApiService } from '../../services/api.service';
import * as Sel from '../../store/dataset.selectors';
import { map } from 'rxjs';

@Component({
  selector: 'app-data-table',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [CommonModule, FormsModule, TableModule, ButtonModule, InputTextModule, MessageModule, TagModule, MultiSelectModule, PopoverModule, TooltipModule, DialogModule, SelectModule],
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
          [badge]="((hiddenCount$ | async) ?? 0) > 0 ? '' + (hiddenCount$ | async) : ''"
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
            @for (col of (orderedAllColumns$ | async) ?? []; track col.name; let i = $index) {
              <div class="col-panel-item"
                [class.null-col]="(nullColumnSet$ | async)?.has(col.name)"
                [class.drag-over]="dragOverIndex === i"
                draggable="true"
                (dragstart)="onDragStart(i)"
                (dragover)="onDragOverCol($event, i)"
                (dragleave)="dragOverIndex = -1"
                (drop)="onDropCol($event, i)"
                (dragend)="dragOverIndex = -1"
              >
                <i class="pi pi-bars drag-handle"></i>
                <i [class]="!((nullColumnSet$ | async)?.has(col.name)) && !((hiddenColumnSet$ | async)?.has(col.name)) ? 'pi pi-eye' : 'pi pi-eye-slash'"
                   [class.muted]="(nullColumnSet$ | async)?.has(col.name) || (hiddenColumnSet$ | async)?.has(col.name)"
                   (click)="toggleColumn(col.name)"></i>
                <span [class.muted]="(nullColumnSet$ | async)?.has(col.name) || (hiddenColumnSet$ | async)?.has(col.name)" (click)="toggleColumn(col.name)">{{ col.name }}</span>
                @if ((nullColumnSet$ | async)?.has(col.name)) {
                  <span class="null-badge">null</span>
                }
              </div>
            }
          </div>
        </div>
      </p-popover>

      @if (((hiddenColumns$ | async) ?? []).length > 0) {
        <div class="hidden-columns-bar">
          <span class="hidden-label">Hidden:</span>
          @for (col of (hiddenColumns$ | async) ?? []; track col) {
            <p-tag [value]="col" severity="warn" (click)="toggleColumn(col)" styleClass="clickable-tag" />
          }
          @if (((nullColumns$ | async) ?? []).length > 0) {
            <span class="hidden-label" style="margin-left: 0.5rem">All null:</span>
            @for (col of (nullColumns$ | async) ?? []; track col) {
              <p-tag [value]="col" severity="secondary" />
            }
          }
        </div>
      } @else if (((nullColumns$ | async) ?? []).length > 0) {
        <div class="hidden-columns-bar">
          <span class="hidden-label">Hidden (all null):</span>
          @for (col of (nullColumns$ | async) ?? []; track col) {
            <p-tag [value]="col" severity="secondary" />
          }
        </div>
      }

      @if (showNameBubbles) {
        <div class="bubble-container" #bubbleContainer [style.height.px]="bubbleContainerHeight">
          @for (b of bubbles; track b.colName) {
            <div class="name-bubble" [style.left.px]="b.x" [style.bottom.px]="b.row * 28">
              {{ b.colName }}
              <svg class="bubble-arrow" [style.left.px]="b.arrowX" [attr.height]="b.arrowHeight">
                <line x1="0" [attr.y1]="b.arrowHeight" x2="0" y2="0" stroke="var(--p-primary-color)" stroke-width="1" stroke-dasharray="3,2" />
              </svg>
            </div>
          }
        </div>
      }

      @if (error$ | async; as error) {
        <p-message severity="error" [text]="error" />
      } @else {
        <p-table #dataTable
          [value]="(displayRows$ | async) ?? []"
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
                  <div class="col-header" [class.sorted]="currentSort?.field === col.name" [class.joined]="(joinedColumnSet$ | async)?.has(col.name)">
                    <span class="col-header-name">
                      @if ((joinedColumnSet$ | async)?.has(col.name)) {
                        <i class="pi pi-link join-icon" pTooltip="Joined/substituted" tooltipPosition="top"></i>
                      }
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
                    pTooltip="Special: $null, $notnull, $empty, $notempty"
                    tooltipPosition="top"
                    (input)="onFilter(col.name, $event)"
                    [class.filter-active]="!!filters[col.name]"
                    [disabled]="col.name.endsWith('_display') || (joinedColumnSet$ | async)?.has(col.name)"
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
      <p-popover #colSettingsPanel [style]="{ minWidth: '200px', maxWidth: '400px' }">
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
            <div class="sort-btn" (click)="openJoinDialog()">
              <i class="pi pi-link"></i> Substitute Values
            </div>
            @if ((joinedColumnSet$ | async)?.has(activeColName)) {
              <div class="sort-btn" (click)="removeJoin()">
                <i class="pi pi-times"></i> Remove Substitution
              </div>
            }
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

      <!-- Join/Substitute dialog -->
      <p-dialog header="Substitute Values" [(visible)]="showJoinDialog" [modal]="true" [style]="{ width: '700px', minHeight: '50vh' }">
        <div class="join-dialog">
          <div class="join-field">
            <label>Source column</label>
            <input pInputText [value]="joinConfig.sourceColumn" disabled style="width: 100%" />
          </div>
          <div class="join-field">
            <label>Join with dataset</label>
            <p-select
              [options]="(joinDatasets$ | async) ?? []"
              optionLabel="name"
              optionValue="id"
              [(ngModel)]="joinConfig.joinDatasetId"
              (ngModelChange)="onJoinDatasetChange($event)"
              placeholder="Select dataset"
              [filter]="true"
              [style]="{ width: '100%' }"
            />
          </div>
          @if (joinTargetColumns.length > 0) {
            <div class="join-field">
              <label>Join on column (in target dataset)</label>
              <p-select
                [options]="joinTargetColumns"
                optionLabel="name"
                optionValue="name"
                [(ngModel)]="joinConfig.joinColumn"
                placeholder="Select join column"
                [filter]="true"
                [style]="{ width: '100%' }"
              />
            </div>
            <div class="join-field">
              <label>Display template</label>
              <input pInputText [(ngModel)]="joinConfig.displayTemplate" placeholder="{column1} ({column2})" style="width: 100%" />
              <small class="join-hint">Use &#123;column_name&#125; to reference fields. E.g. &#123;name&#125; (&#123;code&#125;)</small>
            </div>
            <div class="join-field">
              <label>Display mode</label>
              <div class="join-mode-options">
                <label class="join-mode-option" [class.active]="joinConfig.mode === 'replace'">
                  <input type="radio" name="joinMode" value="replace" [(ngModel)]="joinConfig.mode" /> Replace original column
                </label>
                <label class="join-mode-option" [class.active]="joinConfig.mode === 'add'">
                  <input type="radio" name="joinMode" value="add" [(ngModel)]="joinConfig.mode" /> Add as additional column
                </label>
              </div>
            </div>
            <div class="join-field">
              <label>Available columns</label>
              <div class="join-chips">
                @for (col of joinTargetColumns; track col.name) {
                  <span class="join-chip" (click)="insertJoinField(col.name)">{{ col.name }}</span>
                }
              </div>
            </div>
          }
          <div class="join-footer">
            <p-button label="Apply" icon="pi pi-check" (onClick)="saveJoin()" [disabled]="!joinConfig.joinDatasetId || !joinConfig.joinColumn || !joinConfig.displayTemplate" />
          </div>
        </div>
      </p-dialog>
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
    .col-settings-title { font-weight: 600; font-size: 0.9rem; margin-bottom: 0.5rem; word-break: break-all; }
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
    .col-header.joined { background: color-mix(in srgb, var(--p-cyan-500) 10%, transparent); border-radius: 4px; padding: 0 0.25rem; }
    .join-icon { font-size: 0.7rem; color: var(--p-cyan-500); margin-right: 0.25rem; }
    .join-dialog { display: flex; flex-direction: column; gap: 1rem; }
    .join-field { display: flex; flex-direction: column; gap: 0.3rem; }
    .join-field label { font-weight: 500; font-size: 0.85rem; }
    .join-hint { color: var(--p-text-muted-color); font-size: 0.8rem; }
    .join-chips { display: flex; flex-wrap: wrap; gap: 0.3rem; }
    .join-chip {
      padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.8rem; cursor: pointer;
      background: var(--p-surface-100); border: 1px solid var(--p-surface-border);
    }
    .join-chip:hover { background: var(--p-primary-50); border-color: var(--p-primary-color); }
    .join-mode-options { display: flex; gap: 0.75rem; }
    .join-mode-option {
      display: flex; align-items: center; gap: 0.4rem; padding: 0.4rem 0.75rem;
      border: 1px solid var(--p-surface-border); border-radius: 6px; cursor: pointer; font-size: 0.85rem;
    }
    .join-mode-option:hover { border-color: var(--p-primary-color); }
    .join-mode-option.active { border-color: var(--p-primary-color); background: var(--p-primary-50); }
    .join-mode-option input[type=radio] { margin: 0; }
    .join-footer { display: flex; justify-content: flex-end; }
    .bubble-container { position: relative; margin-bottom: 0.25rem; }
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
export class DataTableComponent implements OnInit, OnDestroy {
  private store = inject(Store);
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private titleService = inject(Title);
  private api = inject(ApiService);
  private el = inject(ElementRef);
  private cdr = inject(ChangeDetectorRef);

  @ViewChild('colSettingsPanel') colSettingsPanel: any;

  // All store selectors — no manual subscriptions
  currentDataset$ = this.store.select(Sel.selectCurrentDataset);
  visibleColumns$ = this.store.select(Sel.selectVisibleColumns);
  displayRows$ = this.store.select(Sel.selectDisplayRows);
  nullColumns$ = this.store.select(Sel.selectNullColumns);
  hiddenColumns$ = this.store.select(Sel.selectHiddenColumns);
  totalRows$ = this.store.select(Sel.selectTotalRows);
  loading$ = this.store.select(Sel.selectDataLoading);
  isFiltered$ = this.store.select(Sel.selectFiltered);
  error$ = this.store.select(Sel.selectDatasetError);
  orderedAllColumns$ = this.store.select(Sel.selectOrderedAllColumns);
  joinedColumnSet$ = this.store.select(Sel.selectJoinedColumnSet);
  joinDatasets$ = this.store.select(Sel.selectDatasets);
  columnJoins$ = this.store.select(Sel.selectColumnJoins);
  hiddenCount$ = this.store.select(Sel.selectHiddenColumns).pipe(map(h => h.length));
  nullColumnSet$ = this.store.select(Sel.selectNullColumns).pipe(map(n => new Set(n)));
  hiddenColumnSet$ = this.store.select(Sel.selectHiddenColumns).pipe(map(h => new Set(h)));

  // Truly local UI state (not in store)
  pageSize = 100;
  private datasetId!: number;
  filters: Record<string, string> = {};
  private filterTimeout: ReturnType<typeof setTimeout> | null = null;
  dragOverIndex = -1;
  private dragStartIndex = -1;
  activeColName = '';
  currentSort: { field: string; order: string } | null = null;
  columnWidthMap: Record<string, number> = {};
  private resizeTimeout: ReturnType<typeof setTimeout> | null = null;
  showNameBubbles = false;
  bubbles: { colName: string; x: number; row: number; arrowX: number; arrowHeight: number }[] = [];
  bubbleContainerHeight = 0;
  private bubbleTimer: ReturnType<typeof setTimeout> | null = null;
  showDistinctDialog = false;
  distinctColName = '';
  distinctValues: { value: unknown; count: number }[] = [];
  distinctLoading = false;
  copiedDistinct = false;
  distinctLimit = 500;
  distinctLimitInput = 500;
  showJoinDialog = false;
  joinConfig = { sourceColumn: '', joinDatasetId: 0, joinColumn: '', displayTemplate: '', mode: 'replace' as 'replace' | 'add' };
  joinTargetColumns: { name: string; type: string }[] = [];
  private widthStyleEl: HTMLStyleElement | null = null;

  // Single subscription for side effects that can't be async pipe (title, column widths DOM)
  private subs: { unsubscribe: () => void }[] = [];

  ngOnInit() {
    this.datasetId = +this.route.snapshot.paramMap.get('id')!;
    const savedPageSize = localStorage.getItem(`dfviewer_pagesize_${this.datasetId}`);
    if (savedPageSize) this.pageSize = +savedPageSize;
    this.store.dispatch(DatasetActions.openDataset({ id: this.datasetId }));
    this.store.dispatch(DatasetActions.loadDatasets()); // for join dataset picker

    // Title update — can't use async pipe for document.title
    this.subs.push(this.currentDataset$.subscribe(ds => {
      if (ds) this.titleService.setTitle(`${ds.name} - DfViewer`);
    }));

    // Column widths DOM application — needs imperative DOM access
    this.subs.push(this.store.select(Sel.selectColumnWidths).subscribe(w => {
      this.columnWidthMap = { ...w };
    }));
    this.subs.push(this.displayRows$.subscribe(rows => {
      if (rows.length > 0 && Object.keys(this.columnWidthMap).length > 0) {
        setTimeout(() => this.applyColumnWidths(), 200);
      }
    }));
  }

  ngOnDestroy() {
    this.titleService.setTitle('DfViewer');
    this.subs.forEach(s => s.unsubscribe());
    if (this.widthStyleEl) {
      this.widthStyleEl.remove();
      this.widthStyleEl = null;
    }
  }

  // Page size
  onPageChange(event: any) {
    if (event.rows && event.rows !== this.pageSize) {
      this.pageSize = event.rows;
      localStorage.setItem(`dfviewer_pagesize_${this.datasetId}`, '' + this.pageSize);
    }
  }

  // Column visibility
  toggleColumn(name: string) {
    let current: string[] = [];
    this.hiddenColumns$.subscribe(h => current = [...h]).unsubscribe();
    const idx = current.indexOf(name);
    if (idx >= 0) current.splice(idx, 1); else current.push(name);
    this.store.dispatch(DatasetActions.setHiddenColumns({ hiddenColumns: current }));
  }

  showAllColumns() {
    this.store.dispatch(DatasetActions.setHiddenColumns({ hiddenColumns: [] }));
  }

  // Column order drag/drop
  onDragStart(index: number) { this.dragStartIndex = index; }

  onDragOverCol(event: DragEvent, index: number) {
    event.preventDefault();
    this.dragOverIndex = index;
  }

  onDropCol(event: DragEvent, dropIndex: number) {
    event.preventDefault();
    this.dragOverIndex = -1;
    if (this.dragStartIndex === dropIndex) return;

    let cols: { name: string; type: string }[] = [];
    this.orderedAllColumns$.subscribe(c => cols = [...c]).unsubscribe();
    const [moved] = cols.splice(this.dragStartIndex, 1);
    cols.splice(dropIndex, 0, moved);
    this.store.dispatch(DatasetActions.setColumnOrder({ columnOrder: cols.map(c => c.name) }));
  }

  resetColumnOrder() {
    this.store.dispatch(DatasetActions.setColumnOrder({ columnOrder: [] }));
  }

  // Bubbles
  toggleNameBubbles() {
    this.showNameBubbles = !this.showNameBubbles;
    this.bubbles = [];
    this.bubbleContainerHeight = 0;
    this.cdr.markForCheck();
    if (this.showNameBubbles) this.scheduleBubbleUpdate();
  }

  private scheduleBubbleUpdate() {
    if (this.bubbleTimer) clearTimeout(this.bubbleTimer);
    this.bubbleTimer = setTimeout(() => this.computeBubbles(), 300);
  }

  private computeBubbles() {
    if (!this.showNameBubbles) return;
    const tableEl = this.el.nativeElement.querySelector('.p-datatable-thead');
    if (!tableEl) return;
    const headerRow = tableEl.querySelector('tr');
    if (!headerRow) return;
    const containerEl = this.el.nativeElement.querySelector('.bubble-container') as HTMLElement;
    if (!containerEl) return;

    const ths = Array.from(headerRow.querySelectorAll('th')) as HTMLElement[];
    const containerRect = containerEl.getBoundingClientRect();
    const candidates: { colName: string; colCenterX: number }[] = [];
    ths.forEach(th => {
      const nameSpan = th.querySelector('.col-header-name') as HTMLElement;
      if (!nameSpan) return;
      const colName = nameSpan.textContent?.trim() || '';
      const thRect = th.getBoundingClientRect();
      const colCenterX = thRect.left - containerRect.left + thRect.width / 2;
      if (nameSpan.scrollWidth > nameSpan.clientWidth + 2) candidates.push({ colName, colCenterX });
    });

    if (candidates.length === 0) { this.bubbles = []; this.bubbleContainerHeight = 0; this.cdr.markForCheck(); return; }

    const measureEl = document.createElement('span');
    measureEl.style.cssText = 'position:absolute;visibility:hidden;font-size:0.75rem;font-weight:600;padding:0 0.5rem;white-space:nowrap';
    document.body.appendChild(measureEl);
    const items = candidates.map(c => { measureEl.textContent = c.colName; return { ...c, bubbleWidth: measureEl.offsetWidth + 12 }; });
    document.body.removeChild(measureEl);

    const placed: { colName: string; x: number; width: number; row: number; colCenterX: number }[] = [];
    const ROW_HEIGHT = 28;
    for (const item of items) {
      let x = Math.max(0, item.colCenterX - item.bubbleWidth / 2);
      let row = 0;
      while (row < 5) {
        if (!placed.some(p => p.row === row && !(x + item.bubbleWidth + 4 < p.x || x > p.x + p.width + 4))) break;
        row++;
      }
      placed.push({ colName: item.colName, x, width: item.bubbleWidth, row, colCenterX: item.colCenterX });
    }

    const maxRow = placed.reduce((m, p) => Math.max(m, p.row), 0);
    this.bubbleContainerHeight = (maxRow + 1) * ROW_HEIGHT + 4;
    this.bubbles = placed.map(p => ({
      colName: p.colName, x: p.x, row: p.row, arrowX: p.colCenterX - p.x,
      arrowHeight: p.row * ROW_HEIGHT + 4
    }));
    this.cdr.markForCheck();
  }

  // Column widths
  private applyColumnWidths() {
    const nativeEl = this.el.nativeElement;
    const tableWrapper = nativeEl.querySelector('[id$="-table"]') as HTMLElement;
    if (!tableWrapper) return;
    const tableId = tableWrapper.id;
    const headerRow = nativeEl.querySelector('.p-datatable-thead tr');
    if (!headerRow) return;
    const ths = Array.from(headerRow.querySelectorAll('th')) as HTMLElement[];
    const colNames = ths.map(th => th.querySelector('.col-header-name')?.textContent?.trim() || '');

    let css = '';
    colNames.forEach((colName, index) => {
      const width = this.columnWidthMap[colName];
      if (width) {
        css += `#${tableId} > .p-datatable-thead > tr > th:nth-child(${index + 1}),
          #${tableId} > .p-datatable-tbody > tr > td:nth-child(${index + 1}),
          #${tableId} > .p-datatable-tfoot > tr > td:nth-child(${index + 1}) { width: ${width}px !important; max-width: ${width}px !important; }\n`;
      }
    });
    if (!css) return;
    if (this.widthStyleEl) this.widthStyleEl.remove();
    this.widthStyleEl = document.createElement('style');
    this.widthStyleEl.innerHTML = css;
    document.head.appendChild(this.widthStyleEl);
  }

  onColResize(event: any) {
    const th = event.element as HTMLElement;
    const colName = th.querySelector('.col-header-name')?.textContent?.trim();
    if (colName) {
      this.columnWidthMap = { ...this.columnWidthMap, [colName]: th.offsetWidth };
      if (this.resizeTimeout) clearTimeout(this.resizeTimeout);
      this.resizeTimeout = setTimeout(() => {
        this.store.dispatch(DatasetActions.setColumnWidths({ columnWidths: { ...this.columnWidthMap } }));
      }, 500);
    }
    if (this.showNameBubbles) this.scheduleBubbleUpdate();
  }

  clearColumnWidth() {
    this.colSettingsPanel.hide();
    const { [this.activeColName]: _, ...rest } = this.columnWidthMap;
    this.columnWidthMap = rest;
    this.store.dispatch(DatasetActions.setColumnWidths({ columnWidths: { ...rest } }));
    if (this.widthStyleEl) { this.widthStyleEl.remove(); this.widthStyleEl = null; }
    if (Object.keys(rest).length > 0) setTimeout(() => this.applyColumnWidths(), 50);
  }

  // Sort + Filter + Lazy load
  openColSettings(event: Event, colName: string) {
    event.stopPropagation();
    this.activeColName = colName;
    this.colSettingsPanel.toggle(event);
  }

  sortColumn(order: string | null) {
    this.colSettingsPanel.hide();
    this.currentSort = order ? { field: this.activeColName, order } : null;
    this.store.dispatch(DatasetActions.loadData({
      page: 0, size: this.pageSize,
      sortField: this.currentSort?.field, sortOrder: this.currentSort?.order,
      filters: Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined
    }));
  }

  onLazyLoad(event: TableLazyLoadEvent) {
    const page = event.first !== undefined ? Math.floor(event.first / this.pageSize) : 0;
    const sortField = event.sortField as string | undefined;
    const sortOrder = event.sortOrder === -1 ? 'DESC' : 'ASC';
    this.store.dispatch(DatasetActions.loadData({
      page, size: this.pageSize, sortField, sortOrder,
      filters: Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined
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
        sortField: this.currentSort?.field, sortOrder: this.currentSort?.order,
        filters: Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined
      }));
    }, 400);
  }

  // Distinct values
  showDistinct() {
    this.colSettingsPanel.hide();
    this.distinctColName = this.activeColName;
    this.loadDistinctValues();
  }

  private loadDistinctValues() {
    this.distinctValues = [];
    this.distinctLoading = true;
    this.showDistinctDialog = true;
    this.cdr.markForCheck();
    const currentFilters = Object.keys(this.filters).length > 0 ? { ...this.filters } : undefined;
    this.api.getDistinctValues(this.datasetId, this.distinctColName, currentFilters, this.distinctLimit).subscribe({
      next: (values) => { this.distinctValues = values; this.distinctLoading = false; this.cdr.markForCheck(); },
      error: () => { this.distinctLoading = false; this.cdr.markForCheck(); }
    });
  }

  applyDistinctLimit(panel: any) {
    panel.hide();
    this.distinctLimit = this.distinctLimitInput || 500;
    this.loadDistinctValues();
  }

  copyDistinct() {
    const text = this.distinctValues.map(item => `${item.value === null ? '(null)' : item.value === '' ? '(empty)' : item.value},${item.count}`).join('\n');
    navigator.clipboard.writeText(text);
    this.copiedDistinct = true;
    setTimeout(() => { this.copiedDistinct = false; this.cdr.markForCheck(); }, 2000);
  }

  filterByDistinct(value: unknown) {
    this.showDistinctDialog = false;
    if (value === null) this.filters = { ...this.filters, [this.distinctColName]: '$null' };
    else if (value === '') this.filters = { ...this.filters, [this.distinctColName]: '$empty' };
    else this.filters = { ...this.filters, [this.distinctColName]: '' + value };
    this.store.dispatch(DatasetActions.loadData({
      page: 0, size: this.pageSize,
      sortField: this.currentSort?.field, sortOrder: this.currentSort?.order,
      filters: { ...this.filters }
    }));
  }

  // Column joins
  openJoinDialog() {
    this.colSettingsPanel.hide();
    let joins: any[] = [];
    this.columnJoins$.subscribe(j => joins = j).unsubscribe();
    const existing = joins.find((j: any) => j.sourceColumn === this.activeColName);
    this.joinConfig = existing
      ? { ...existing, mode: existing.mode || 'replace' }
      : { sourceColumn: this.activeColName, joinDatasetId: 0, joinColumn: '', displayTemplate: '', mode: 'replace' as 'replace' | 'add' };
    this.joinTargetColumns = [];
    if (existing?.joinDatasetId) this.onJoinDatasetChange(existing.joinDatasetId);
    this.showJoinDialog = true;
    this.cdr.markForCheck();
  }

  onJoinDatasetChange(datasetId: number) {
    if (!datasetId) { this.joinTargetColumns = []; return; }
    this.api.getSchema(datasetId).subscribe(cols => { this.joinTargetColumns = cols; this.cdr.markForCheck(); });
  }

  insertJoinField(fieldName: string) {
    this.joinConfig.displayTemplate += `{${fieldName}}`;
  }

  saveJoin() {
    let joins: any[] = [];
    this.columnJoins$.subscribe(j => joins = [...j]).unsubscribe();
    joins = joins.filter((j: any) => j.sourceColumn !== this.joinConfig.sourceColumn);
    joins.push({ ...this.joinConfig });
    this.store.dispatch(DatasetActions.setColumnJoins({ columnJoins: joins }));
    this.showJoinDialog = false;
  }

  removeJoin() {
    this.colSettingsPanel.hide();
    let joins: any[] = [];
    this.columnJoins$.subscribe(j => joins = [...j]).unsubscribe();
    joins = joins.filter((j: any) => j.sourceColumn !== this.activeColName);
    this.store.dispatch(DatasetActions.setColumnJoins({ columnJoins: joins }));
  }

  goBack() {
    this.router.navigate(['/']);
  }
}
