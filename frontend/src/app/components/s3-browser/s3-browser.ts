import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { Store } from '@ngrx/store';
import { InputTextModule } from 'primeng/inputtext';
import { AutoCompleteModule } from 'primeng/autocomplete';
import { TooltipModule } from 'primeng/tooltip';
import { ButtonModule } from 'primeng/button';
import { TableModule } from 'primeng/table';
import { TagModule } from 'primeng/tag';
import { AccordionModule } from 'primeng/accordion';
import { MultiSelectModule } from 'primeng/multiselect';
import { ProgressBarModule } from 'primeng/progressbar';
import { MessageModule } from 'primeng/message';
import { DialogModule } from 'primeng/dialog';
import { CheckboxModule } from 'primeng/checkbox';
import { S3ConfigComponent } from '../s3-config/s3-config';
import { S3Actions } from '../../store/s3.actions';
import * as Sel from '../../store/s3.selectors';
import { EtlRun, EtlRunGroup, ScanCacheEntry } from '../../models/s3.model';

@Component({
  selector: 'app-s3-browser',
  standalone: true,
  imports: [
    CommonModule, FormsModule, InputTextModule, AutoCompleteModule, TooltipModule, ButtonModule, TableModule,
    TagModule, AccordionModule, MultiSelectModule, CheckboxModule, ProgressBarModule, MessageModule, DialogModule,
    S3ConfigComponent
  ],
  template: `
    <div class="s3-browser">
      <div class="header">
        <p-button icon="pi pi-arrow-left" label="Back" [text]="true" (onClick)="goBack()" />
        <h2>S3 Browser</h2>
        <div class="header-spacer"></div>
        <app-s3-config />
      </div>

      @if (configured$ | async) {
        <div class="scan-bar">
          <p-autoComplete
            [(ngModel)]="scanUri"
            [suggestions]="filteredCacheEntries"
            (completeMethod)="filterCache($event)"
            [dropdown]="true"
            field="uri"
            placeholder="s3://bucket/prefix/"
            [style]="{ flex: 1 }"
            [inputStyle]="{ width: '100%' }"
            (onSelect)="onCacheSelect($event)"
            (onKeyUp)="onInputKeyUp($event)"
          >
            <ng-template let-entry #item>
              <div class="cache-suggestion">
                <div class="cache-top">
                  @if (isStale(entry)) {
                    <i class="pi pi-exclamation-circle stale-icon" title="Older than 24h"></i>
                  }
                  <span class="cache-uri">{{ entry.uri }}</span>
                </div>
                <span class="cache-meta">{{ entry.fileCount }} files &middot; {{ entry.scannedAt | date:'medium' }}</span>
              </div>
            </ng-template>
          </p-autoComplete>
          <input pInputText [(ngModel)]="maxObjects" type="number" placeholder="Max objects" style="width: 140px" />
          <p-button
            [label]="selectedCacheEntry ? 'Rescan' : 'Scan'"
            [icon]="selectedCacheEntry ? 'pi pi-refresh' : 'pi pi-search'"
            (onClick)="scan(!!selectedCacheEntry)"
            [loading]="(scanning$ | async) ?? false"
          />
        </div>

        <!-- Scan progress -->
        @if (scanning$ | async) {
          @if (scanProgress$ | async; as sp) {
            <div class="scan-progress">
              <p>{{ sp.message }}</p>
              <p-progressBar mode="indeterminate" />
              <div class="scan-stats">
                <span>Objects scanned: <strong>{{ sp.objectsScanned | number }}</strong></span>
                <span>Data files found: <strong>{{ sp.dataFilesFound | number }}</strong></span>
              </div>
            </div>
          }
        }

        @if (scanResult$ | async; as result) {
          @if (result.etlRuns.length > 0) {
            <!-- Filter bar -->
            <div class="filter-bar">
              <h3>ETL Runs (auto-discovered)</h3>
              <div class="filter-controls">
                <p-multiselect
                  [options]="allTimestamps"
                  [(ngModel)]="selectedTimestamps"
                  (ngModelChange)="applyTimestampFilter()"
                  placeholder="Filter by timestamp"
                  [style]="{ minWidth: '250px' }"
                  [showToggleAll]="true"
                />
                <p-button
                  label="Import Multiple ({{ filteredRunCount }})"
                  icon="pi pi-download"
                  [outlined]="true"
                  (onClick)="showBatchImport()"
                  [disabled]="filteredRunCount === 0"
                />
                <p-button
                  [icon]="allExpanded ? 'pi pi-minus' : 'pi pi-plus'"
                  [label]="allExpanded ? 'Collapse All' : 'Expand All'"
                  [text]="true"
                  size="small"
                  (onClick)="toggleAll()"
                />
              </div>
            </div>

            <p-accordion [multiple]="true" [(value)]="expandedPanels">
              @for (group of filteredEtlRuns; track group.entityPath; let i = $index) {
                <p-accordionpanel [value]="''+i" class="run-group">
                  <p-accordionheader>
                    <i class="pi pi-folder" style="margin-right: 0.5rem"></i>
                    {{ group.entityPath }} ({{ group.runs.length }} runs)
                  </p-accordionheader>
                  <p-accordioncontent>
                    <p-table [value]="group.runs" styleClass="p-datatable-sm">
                      <ng-template #header>
                        <tr>
                          <th>Timestamp</th>
                          <th>Files</th>
                          <th>Total Size</th>
                          <th>Actions</th>
                        </tr>
                      </ng-template>
                      <ng-template #body let-run>
                        <tr>
                          <td>{{ run.timestamp }}</td>
                          <td>{{ run.files.length }}</td>
                          <td>{{ formatSize(run.totalSize) }}</td>
                          <td>
                            <p-button
                              label="Import"
                              icon="pi pi-download"
                              size="small"
                              (onClick)="importRun(group.entityPath, run)"
                              [loading]="(importing$ | async) ?? false"
                            />
                          </td>
                        </tr>
                      </ng-template>
                    </p-table>
                  </p-accordioncontent>
                </p-accordionpanel>
              }
            </p-accordion>
          }

          @if (flatFiles$ | async; as ff) {
            @if (ff.length > 0) {
              <h3>Files ({{ ff.length }})</h3>
              <p-table
                [value]="ff"
                [paginator]="true"
                [rows]="20"
                [(selection)]="selectedFiles"
                styleClass="p-datatable-sm"
              >
                <ng-template #header>
                  <tr>
                    <th style="width: 3rem"><p-tableHeaderCheckbox /></th>
                    <th>File</th>
                    <th>Type</th>
                    <th>Size</th>
                    <th>Modified</th>
                  </tr>
                </ng-template>
                <ng-template #body let-file>
                  <tr>
                    <td><p-tableCheckbox [value]="file" /></td>
                    <td class="truncate" [title]="file.uri">{{ file.key }}</td>
                    <td><p-tag [value]="file.type" /></td>
                    <td>{{ formatSize(file.size) }}</td>
                    <td>{{ file.lastModified | date:'short' }}</td>
                  </tr>
                </ng-template>
              </p-table>
              <div class="import-bar">
                <input pInputText [(ngModel)]="importName" placeholder="Dataset name" />
                <p-button
                  label="Import Selected ({{ selectedFiles.length }})"
                  icon="pi pi-download"
                  (onClick)="importSelected()"
                  [disabled]="selectedFiles.length === 0 || !importName"
                  [loading]="(importing$ | async) ?? false"
                />
              </div>
            }
          }
        }
      } @else {
        <p-message severity="info" text="Configure S3 credentials to get started." />
      }

      <!-- Import progress dialog -->
      <p-dialog header="Importing..." [visible]="(showImportDialog$ | async) ?? false" (visibleChange)="onDialogClose()" [modal]="true" [closable]="!(importing$ | async)" [style]="{ width: '500px' }">
        @if (importProgress$ | async; as p) {
          <div class="progress-info">
            <p><strong>{{ p.phase | titlecase }}</strong></p>
            @if (p.phase === 'downloading') {
              <p>File {{ p.fileIndex }} / {{ p.totalFiles }}: {{ p.fileName }}</p>
              <p-progressBar
                [value]="p.bytesTotal > 0 ? (p.bytesDownloaded / p.bytesTotal * 100) : 0"
                [showValue]="true"
              />
              <p class="size-text">{{ formatSize(p.bytesDownloaded) }} / {{ formatSize(p.bytesTotal) }}</p>
            }
            @if (p.phase === 'ingesting') {
              <p>Loading data into database...</p>
              <p-progressBar mode="indeterminate" />
            }
            @if (p.phase === 'done') {
              <p-message severity="success" text="Import complete!" />
              <p-button label="View Dataset" icon="pi pi-eye" (onClick)="openDataset(p.datasetId!)" [style]="{ 'margin-top': '1rem' }" />
            }
            @if (p.phase === 'error') {
              <p-message severity="error" [text]="p.error || 'Unknown error'" />
            }
          </div>
        }
      </p-dialog>

      <!-- Batch import dialog -->
      <p-dialog header="Import Multiple Datasets" [(visible)]="showBatchDialog" [modal]="true" [style]="{ width: '600px', maxHeight: '80vh' }">
        <div class="batch-dialog">
          <div class="batch-actions">
            <span class="batch-count">{{ batchItems.length }} selected</span>
            <p-button label="Select All" [text]="true" size="small" (onClick)="batchSelectAll()" />
            <p-button label="Deselect All" [text]="true" size="small" (onClick)="batchDeselectAll()" />
          </div>
          <div class="batch-list">
            @for (item of batchItems; track item.key) {
              <div class="batch-row" [class.deselected]="!item.selected">
                <p-checkbox [(ngModel)]="item.selected" [binary]="true" />
                <span class="batch-entity">{{ item.entityShort }}</span>
                <span class="batch-ts">{{ item.timestamp }}</span>
                <span class="batch-size">{{ formatSize(item.totalSize) }}</span>
              </div>
            }
          </div>
          <div class="batch-footer">
            <p-button
              label="Import {{ batchSelectedCount }} datasets"
              icon="pi pi-download"
              (onClick)="executeBatchImport()"
              [disabled]="batchSelectedCount === 0"
              [loading]="(importing$ | async) ?? false"
            />
          </div>
        </div>
      </p-dialog>
    </div>
  `,
  styles: [`
    .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
    .header-spacer { flex: 1; }
    .scan-bar { display: flex; gap: 0.5rem; margin-bottom: 1.5rem; align-items: center; }
    .cache-suggestion { display: flex; flex-direction: column; gap: 0.1rem; }
    .cache-top { display: flex; align-items: center; gap: 0.4rem; }
    .cache-uri { font-weight: 500; font-size: 0.9rem; }
    .cache-meta { font-size: 0.75rem; color: var(--p-text-muted-color); }
    .stale-icon { color: var(--p-red-500); font-size: 0.85rem; }
    .scan-progress {
      padding: 1rem; margin-bottom: 1.5rem; border-radius: 8px;
      background: var(--p-surface-card); border: 1px solid var(--p-surface-border);
    }
    .scan-stats { display: flex; gap: 2rem; margin-top: 0.5rem; font-size: 0.9rem; color: var(--p-text-muted-color); }
    .filter-bar { display: flex; align-items: center; justify-content: space-between; margin-bottom: 1rem; flex-wrap: wrap; gap: 0.75rem; }
    .filter-bar h3 { margin: 0; }
    .filter-controls { display: flex; gap: 0.5rem; align-items: center; }
    .import-bar { display: flex; gap: 0.5rem; margin-top: 1rem; align-items: center; }
    .truncate { max-width: 500px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .run-group { margin-bottom: 0.5rem; }
    .progress-info { display: flex; flex-direction: column; gap: 0.5rem; }
    .size-text { font-size: 0.85rem; color: var(--p-text-muted-color); }
    .batch-dialog { display: flex; flex-direction: column; gap: 0.75rem; }
    .batch-actions { display: flex; align-items: center; gap: 0.5rem; }
    .batch-count { font-weight: 600; font-size: 0.9rem; margin-right: auto; }
    .batch-list { max-height: 50vh; overflow-y: auto; display: flex; flex-direction: column; gap: 2px; }
    .batch-row {
      display: flex; align-items: center; gap: 0.5rem; padding: 0.3rem 0.5rem;
      border-radius: 4px; font-size: 0.85rem;
    }
    .batch-row:hover { background: var(--p-surface-hover); }
    .batch-row.deselected { opacity: 0.4; }
    .batch-entity { flex: 1; font-weight: 500; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .batch-ts { color: var(--p-text-muted-color); white-space: nowrap; }
    .batch-size { color: var(--p-text-muted-color); white-space: nowrap; min-width: 60px; text-align: right; }
    .batch-footer { display: flex; justify-content: flex-end; padding-top: 0.5rem; border-top: 1px solid var(--p-surface-border); }
  `]
})
export class S3BrowserComponent {
  private store = inject(Store);
  private router = inject(Router);

  configured$ = this.store.select(Sel.selectConfigured);
  scanning$ = this.store.select(Sel.selectScanning);
  scanProgress$ = this.store.select(Sel.selectScanProgress);
  scanResult$ = this.store.select(Sel.selectScanResult);
  flatFiles$ = this.store.select(Sel.selectFlatFiles);
  importing$ = this.store.select(Sel.selectImporting);
  showImportDialog$ = this.store.select(Sel.selectShowImportDialog);
  importProgress$ = this.store.select(Sel.selectImportProgress);
  scanUri$ = this.store.select(Sel.selectScanUri);
  scanCacheEntries$ = this.store.select(Sel.selectScanCacheEntries);

  scanUri = '';
  filteredCacheEntries: ScanCacheEntry[] = [];
  private allCacheEntries: ScanCacheEntry[] = [];
  selectedCacheEntry: ScanCacheEntry | null = null;
  maxObjects: number | null = null;
  selectedFiles: any[] = [];
  importName = '';

  // Accordion
  expandedPanels: string[] = [];
  allExpanded = false;

  // Timestamp filter
  allTimestamps: string[] = [];
  selectedTimestamps: string[] = [];
  private allEtlRuns: EtlRunGroup[] = [];
  filteredEtlRuns: EtlRunGroup[] = [];
  filteredRunCount = 0;

  // Batch import
  showBatchDialog = false;
  batchItems: { key: string; entityPath: string; entityShort: string; timestamp: string; totalSize: number; files: string[]; selected: boolean }[] = [];
  get batchSelectedCount() { return this.batchItems.filter(i => i.selected).length; }

  constructor() {
    this.scanUri$.subscribe(uri => this.scanUri = uri);
    this.scanCacheEntries$.subscribe(entries => this.allCacheEntries = entries);
    this.store.dispatch(S3Actions.loadScanCache());
    this.scanResult$.subscribe(result => {
      if (result) {
        this.allEtlRuns = result.etlRuns;
        // Collect all unique timestamps across all groups
        const tsSet = new Set<string>();
        result.etlRuns.forEach(g => g.runs.forEach(r => tsSet.add(r.timestamp)));
        this.allTimestamps = [...tsSet].sort().reverse();
        this.selectedTimestamps = [];
        this.filteredEtlRuns = result.etlRuns;
        this.updateFilteredCount();
      }
    });
  }

  goBack() {
    this.router.navigate(['/']);
  }

  filterCache(event: { query: string }) {
    const q = event.query.toLowerCase();
    this.filteredCacheEntries = this.allCacheEntries.filter(e => e.uri.toLowerCase().includes(q));
  }

  onInputKeyUp(event: KeyboardEvent) {
    if (event.key !== 'Enter') return;
    const uri = typeof this.scanUri === 'string' ? this.scanUri : (this.scanUri as any)?.uri ?? '';
    if (!uri) return;
    // Check if there's a cached entry for this URI
    const cached = this.allCacheEntries.find(e => e.uri === uri);
    if (cached) {
      this.scanUri = uri;
      this.selectedCacheEntry = cached;
      this.selectedFiles = [];
      this.allTimestamps = [];
      this.selectedTimestamps = [];
      this.filteredEtlRuns = [];
      this.store.dispatch(S3Actions.loadCachedResult({ uri }));
    } else {
      this.scan();
    }
  }

  onCacheSelect(event: any) {
    const entry = event.value ?? event;
    this.scanUri = entry.uri ?? entry;
    this.selectedCacheEntry = entry;
    // Load cached result directly
    this.selectedFiles = [];
    this.allTimestamps = [];
    this.selectedTimestamps = [];
    this.filteredEtlRuns = [];
    this.store.dispatch(S3Actions.loadCachedResult({ uri: this.scanUri }));
  }

  isStale(entry: ScanCacheEntry): boolean {
    const scannedAt = new Date(entry.scannedAt).getTime();
    return Date.now() - scannedAt > 24 * 60 * 60 * 1000;
  }

  scan(forceRescan: boolean = false) {
    const uri = typeof this.scanUri === 'string' ? this.scanUri : (this.scanUri as any)?.uri ?? '';
    if (!uri) return;
    this.scanUri = uri;
    this.selectedCacheEntry = null;
    this.selectedFiles = [];
    this.allTimestamps = [];
    this.selectedTimestamps = [];
    this.filteredEtlRuns = [];
    this.store.dispatch(S3Actions.startScan({ uri, maxObjects: this.maxObjects ?? undefined, forceRescan }));
  }

  applyTimestampFilter() {
    if (this.selectedTimestamps.length === 0) {
      // No filter = show all
      this.filteredEtlRuns = this.allEtlRuns;
    } else {
      const tsSet = new Set(this.selectedTimestamps);
      this.filteredEtlRuns = this.allEtlRuns
        .map(group => ({
          ...group,
          runs: group.runs.filter(r => tsSet.has(r.timestamp))
        }))
        .filter(group => group.runs.length > 0);
    }
    this.updateFilteredCount();
  }

  private updateFilteredCount() {
    this.filteredRunCount = this.filteredEtlRuns.reduce((sum, g) => sum + g.runs.length, 0);
  }

  toggleAll() {
    if (this.allExpanded) {
      this.expandedPanels = [];
      this.allExpanded = false;
    } else {
      this.expandedPanels = this.filteredEtlRuns.map((_, i) => '' + i);
      this.allExpanded = true;
    }
  }

  showBatchImport() {
    this.batchItems = [];
    for (const group of this.filteredEtlRuns) {
      for (const run of group.runs) {
        this.batchItems.push({
          key: group.entityPath + '/' + run.timestamp,
          entityPath: group.entityPath,
          entityShort: group.entityPath.split('/').pop() || group.entityPath,
          timestamp: run.timestamp,
          totalSize: run.totalSize,
          files: run.files.map(f => f.uri),
          selected: true
        });
      }
    }
    this.showBatchDialog = true;
  }

  batchSelectAll() { this.batchItems.forEach(i => i.selected = true); }
  batchDeselectAll() { this.batchItems.forEach(i => i.selected = false); }

  executeBatchImport() {
    const selected = this.batchItems.filter(i => i.selected);
    for (const item of selected) {
      this.store.dispatch(S3Actions.startImport({
        files: item.files,
        name: item.entityShort + ' @ ' + item.timestamp,
        entityPath: item.entityPath,
        runTimestamp: item.timestamp
      }));
    }
    this.showBatchDialog = false;
  }

  importRun(entityPath: string, run: EtlRun) {
    const name = entityPath.split('/').pop() + ' @ ' + run.timestamp;
    this.store.dispatch(S3Actions.startImport({
      files: run.files.map(f => f.uri),
      name,
      entityPath,
      runTimestamp: run.timestamp
    }));
  }

  importSelected() {
    this.store.dispatch(S3Actions.startImport({
      files: this.selectedFiles.map((f: any) => f.uri),
      name: this.importName
    }));
  }

  onDialogClose() {
    this.store.dispatch(S3Actions.dismissImportDialog());
  }

  openDataset(id: number) {
    this.store.dispatch(S3Actions.dismissImportDialog());
    this.router.navigate(['/datasets', id]);
  }

  formatSize(bytes: number): string {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
  }
}
