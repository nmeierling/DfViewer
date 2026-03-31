import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { Store } from '@ngrx/store';
import { InputTextModule } from 'primeng/inputtext';
import { ButtonModule } from 'primeng/button';
import { TableModule } from 'primeng/table';
import { TagModule } from 'primeng/tag';
import { AccordionModule } from 'primeng/accordion';
import { ProgressBarModule } from 'primeng/progressbar';
import { MessageModule } from 'primeng/message';
import { DialogModule } from 'primeng/dialog';
import { S3ConfigComponent } from '../s3-config/s3-config';
import { S3Actions } from '../../store/s3.actions';
import * as Sel from '../../store/s3.selectors';
import { EtlRun } from '../../models/s3.model';

@Component({
  selector: 'app-s3-browser',
  standalone: true,
  imports: [
    CommonModule, FormsModule, InputTextModule, ButtonModule, TableModule,
    TagModule, AccordionModule, ProgressBarModule, MessageModule, DialogModule,
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
          <input pInputText [ngModel]="scanUri$ | async" (ngModelChange)="scanUri = $event" placeholder="s3://bucket/prefix/" style="flex: 1" />
          <input pInputText [(ngModel)]="maxObjects" type="number" placeholder="Max objects (optional)" style="width: 180px" />
          <p-button label="Scan" icon="pi pi-search" (onClick)="scan()" [loading]="(scanning$ | async) ?? false" />
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
            <h3>ETL Runs (auto-discovered)</h3>
            @for (group of result.etlRuns; track group.entityPath) {
              <p-accordion [multiple]="true" class="run-group">
                <p-accordionpanel>
                  <p-accordionheader>
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
              </p-accordion>
            }
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
    </div>
  `,
  styles: [`
    .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
    .header-spacer { flex: 1; }
    .scan-bar { display: flex; gap: 0.5rem; margin-bottom: 1.5rem; }
    .scan-progress {
      padding: 1rem; margin-bottom: 1.5rem; border-radius: 8px;
      background: var(--p-surface-card); border: 1px solid var(--p-surface-border);
    }
    .scan-stats { display: flex; gap: 2rem; margin-top: 0.5rem; font-size: 0.9rem; color: var(--p-text-muted-color); }
    .import-bar { display: flex; gap: 0.5rem; margin-top: 1rem; align-items: center; }
    .truncate { max-width: 500px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .run-group { margin-bottom: 0.5rem; }
    .progress-info { display: flex; flex-direction: column; gap: 0.5rem; }
    .size-text { font-size: 0.85rem; color: var(--p-text-muted-color); }
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

  scanUri = '';
  maxObjects: number | null = null;
  selectedFiles: any[] = [];
  importName = '';

  constructor() {
    this.scanUri$.subscribe(uri => this.scanUri = uri);
  }

  goBack() {
    this.router.navigate(['/']);
  }

  scan() {
    this.selectedFiles = [];
    this.store.dispatch(S3Actions.startScan({ uri: this.scanUri, maxObjects: this.maxObjects ?? undefined }));
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
