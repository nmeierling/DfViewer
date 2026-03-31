import { Component, OnInit, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { InputTextModule } from 'primeng/inputtext';
import { ButtonModule } from 'primeng/button';
import { TableModule } from 'primeng/table';
import { TagModule } from 'primeng/tag';
import { AccordionModule } from 'primeng/accordion';
import { ProgressBarModule } from 'primeng/progressbar';
import { MessageModule } from 'primeng/message';
import { DialogModule } from 'primeng/dialog';
import { S3ConfigComponent } from '../s3-config/s3-config';
import { S3Service } from '../../services/s3.service';
import { NotificationService } from '../../services/notification.service';
import { S3ScanResult, S3FileEntry, EtlRun, ImportProgress, ScanProgress } from '../../models/s3.model';

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
      </div>

      <app-s3-config (configuredChange)="s3Configured = $event" />

      @if (s3Configured) {
        <div class="scan-bar">
          <input pInputText [(ngModel)]="scanUri" placeholder="s3://bucket/prefix/" style="flex: 1" />
          <input pInputText [(ngModel)]="maxObjects" type="number" placeholder="Max objects (optional)" style="width: 180px" />
          <p-button label="Scan" icon="pi pi-search" (onClick)="scan()" [loading]="scanning" />
        </div>

        <!-- Scan progress -->
        @if (scanning && scanProgress) {
          <div class="scan-progress">
            <p>{{ scanProgress.message }}</p>
            <p-progressBar mode="indeterminate" />
            <div class="scan-stats">
              <span>Objects scanned: <strong>{{ scanProgress.objectsScanned | number }}</strong></span>
              <span>Data files found: <strong>{{ scanProgress.dataFilesFound | number }}</strong></span>
            </div>
          </div>
        }

        @if (scanResult) {
          <!-- ETL Runs -->
          @if (scanResult.etlRuns.length > 0) {
            <h3>ETL Runs (auto-discovered)</h3>
            @for (group of scanResult.etlRuns; track group.entityPath) {
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
                              [loading]="importing"
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

          <!-- Flat files -->
          @if (flatFiles.length > 0) {
            <h3>Files ({{ flatFiles.length }})</h3>
            <p-table
              [value]="flatFiles"
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
                [loading]="importing"
              />
            </div>
          }
        }
      }

      <!-- Import progress dialog -->
      <p-dialog header="Importing..." [(visible)]="showProgress" [modal]="true" [closable]="!importing" [style]="{ width: '500px' }">
        @if (progress) {
          <div class="progress-info">
            <p><strong>{{ progress.phase | titlecase }}</strong></p>
            @if (progress.phase === 'downloading') {
              <p>File {{ progress.fileIndex }} / {{ progress.totalFiles }}: {{ progress.fileName }}</p>
              <p-progressBar
                [value]="progress.bytesTotal > 0 ? (progress.bytesDownloaded / progress.bytesTotal * 100) : 0"
                [showValue]="true"
              />
              <p class="size-text">{{ formatSize(progress.bytesDownloaded) }} / {{ formatSize(progress.bytesTotal) }}</p>
            }
            @if (progress.phase === 'ingesting') {
              <p>Loading data into database...</p>
              <p-progressBar mode="indeterminate" />
            }
            @if (progress.phase === 'done') {
              <p-message severity="success" text="Import complete!" />
              <p-button label="View Dataset" icon="pi pi-eye" (onClick)="openDataset(progress.datasetId!)" [style]="{ 'margin-top': '1rem' }" />
            }
            @if (progress.phase === 'error') {
              <p-message severity="error" [text]="progress.error || 'Unknown error'" />
            }
          </div>
        }
      </p-dialog>
    </div>
  `,
  styles: [`
    .header { display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem; }
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
export class S3BrowserComponent implements OnInit {
  @ViewChild(S3ConfigComponent) s3Config!: S3ConfigComponent;

  s3Configured = false;
  scanUri = '';
  maxObjects: number | null = null;
  scanning = false;
  scanProgress: ScanProgress | null = null;
  scanResult: S3ScanResult | null = null;
  flatFiles: S3FileEntry[] = [];
  selectedFiles: S3FileEntry[] = [];
  importName = '';
  importing = false;
  showProgress = false;
  progress: ImportProgress | null = null;

  constructor(private s3Service: S3Service, private router: Router, private notifications: NotificationService) {}

  ngOnInit() {
    this.scanUri = S3ConfigComponent.getSavedScanUri();
  }

  goBack() {
    this.router.navigate(['/']);
  }

  scan() {
    this.scanning = true;
    this.scanResult = null;
    this.scanProgress = null;
    this.flatFiles = [];
    this.selectedFiles = [];
    S3ConfigComponent.saveScanUri(this.scanUri);

    this.notifications.show({
      id: 'scan',
      message: 'Scanning S3...',
      detail: 'Starting scan',
      type: 'info',
      progress: { mode: 'indeterminate' }
    });

    this.s3Service.scan(this.scanUri, this.maxObjects ?? undefined).subscribe({
      next: (event) => {
        if (event.type === 'progress') {
          this.scanProgress = event.data;
          if (event.data.phase === 'analyzing') {
            this.notifications.update('scan', {
              message: 'Analyzing...',
              detail: `Discovering ETL runs in ${event.data.dataFilesFound.toLocaleString()} data files`
            });
          } else {
            this.notifications.update('scan', {
              detail: `${event.data.objectsScanned.toLocaleString()} objects scanned, ${event.data.dataFilesFound.toLocaleString()} data files found`
            });
          }
        } else if (event.type === 'result') {
          this.scanResult = event.data;
          const etlFileUris = new Set(
            event.data.etlRuns.flatMap(g => g.runs.flatMap(r => r.files.map(f => f.uri)))
          );
          this.flatFiles = event.data.files.filter(f => !etlFileUris.has(f.uri));
          this.scanning = false;
          this.scanProgress = null;
          const runs = event.data.etlRuns.reduce((sum, g) => sum + g.runs.length, 0);
          this.notifications.update('scan', {
            message: 'Scan complete',
            detail: `${event.data.files.length.toLocaleString()} files, ${event.data.etlRuns.length} entities, ${runs} runs`,
            type: 'success',
            progress: undefined
          });
          this.notifications.dismissAfterDelay('scan', 5000);
        } else if (event.type === 'error') {
          this.scanning = false;
          this.scanProgress = null;
          const errorMsg = event.data?.error || 'Unknown error';
          const isExpired = errorMsg.toLowerCase().includes('expired') || errorMsg.toLowerCase().includes('token');
          if (isExpired) {
            window.dispatchEvent(new CustomEvent('s3-token-expired'));
          }
          this.notifications.update('scan', {
            message: isExpired ? 'AWS credentials expired' : 'Scan failed',
            detail: errorMsg,
            type: 'error',
            progress: undefined
          });
          this.notifications.dismissAfterDelay('scan', 8000);
        }
      },
      error: () => {
        this.scanning = false;
        this.scanProgress = null;
        this.notifications.update('scan', { message: 'Scan failed', detail: 'Connection lost', type: 'error', progress: undefined });
        this.notifications.dismissAfterDelay('scan', 5000);
      },
      complete: () => {
        this.scanning = false;
        // If no result or error was received, the stream ended unexpectedly
        if (!this.scanResult) {
          this.notifications.update('scan', { message: 'Scan ended', detail: 'Connection closed', type: 'error', progress: undefined });
          this.notifications.dismissAfterDelay('scan', 5000);
        }
      }
    });
  }

  importRun(entityPath: string, run: EtlRun) {
    const name = entityPath.split('/').pop() + ' @ ' + run.timestamp;
    this.doImport({
      files: run.files.map(f => f.uri),
      name,
      entityPath,
      runTimestamp: run.timestamp
    });
  }

  importSelected() {
    this.doImport({
      files: this.selectedFiles.map(f => f.uri),
      name: this.importName
    });
  }

  private doImport(request: { files: string[]; name: string; entityPath?: string; runTimestamp?: string }) {
    this.importing = true;
    this.showProgress = true;
    this.progress = null;

    this.notifications.show({
      id: 'import',
      message: `Importing "${request.name}"`,
      detail: 'Starting download...',
      type: 'info',
      progress: { mode: 'indeterminate' }
    });

    this.s3Service.importFiles(request).subscribe({
      next: (progress) => {
        this.progress = progress;
        if (progress.phase === 'downloading') {
          const pct = progress.bytesTotal > 0 ? Math.round(progress.bytesDownloaded / progress.bytesTotal * 100) : 0;
          this.notifications.update('import', {
            detail: `File ${progress.fileIndex}/${progress.totalFiles}: ${progress.fileName}`,
            progress: { mode: 'determinate', value: pct }
          });
        } else if (progress.phase === 'ingesting') {
          this.notifications.update('import', {
            detail: 'Loading into database...',
            progress: { mode: 'indeterminate' }
          });
        } else if (progress.phase === 'done') {
          this.importing = false;
          this.notifications.update('import', {
            message: `Imported "${request.name}"`,
            detail: 'Ready to view',
            type: 'success',
            progress: undefined
          });
          this.notifications.dismissAfterDelay('import', 5000);
        } else if (progress.phase === 'error') {
          this.importing = false;
          this.notifications.update('import', {
            message: 'Import failed',
            detail: progress.error,
            type: 'error',
            progress: undefined
          });
          this.notifications.dismissAfterDelay('import', 8000);
        }
      },
      error: () => {
        this.importing = false;
        this.progress = { phase: 'error', fileIndex: 0, totalFiles: 0, fileName: '', bytesDownloaded: 0, bytesTotal: 0, error: 'Connection lost', done: true };
        this.notifications.update('import', { message: 'Import failed', detail: 'Connection lost', type: 'error', progress: undefined });
        this.notifications.dismissAfterDelay('import', 8000);
      },
      complete: () => {
        this.importing = false;
      }
    });
  }

  openDataset(id: number) {
    this.showProgress = false;
    this.router.navigate(['/datasets', id]);
  }

  formatSize(bytes: number): string {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
  }
}
