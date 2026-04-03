import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { Store } from '@ngrx/store';
import { TableModule } from 'primeng/table';
import { ButtonModule } from 'primeng/button';
import { TagModule } from 'primeng/tag';
import { ConfirmDialogModule } from 'primeng/confirmdialog';
import { DialogModule } from 'primeng/dialog';
import { FileUploadModule } from 'primeng/fileupload';
import { InputTextModule } from 'primeng/inputtext';
import { FloatLabelModule } from 'primeng/floatlabel';
import { ConfirmationService } from 'primeng/api';
import { DatasetActions } from '../../store/dataset.actions';
import { selectDatasets, selectListLoading, selectDuckdbSizeBytes, selectUploading } from '../../store/dataset.selectors';
import { Dataset } from '../../models/dataset.model';

@Component({
  selector: 'app-dataset-list',
  standalone: true,
  imports: [
    CommonModule, FormsModule, TableModule, ButtonModule, TagModule,
    ConfirmDialogModule, DialogModule, FileUploadModule, InputTextModule, FloatLabelModule
  ],
  providers: [ConfirmationService],
  template: `
    <div class="dataset-list">
      <div class="title-bar">
        <h2>Datasets</h2>
        @if (duckdbSize$ | async; as size) {
          @if (size > 0) {
            <span class="db-size">DuckDB: {{ formatSize(size) }}</span>
          }
        }
        <div class="spacer"></div>
        <p-button label="Compare" icon="pi pi-arrows-h" [outlined]="true" (onClick)="goToCompare()" />
        <p-button label="Upload File" icon="pi pi-upload" [outlined]="true" (onClick)="showUploadDialog = true" />
        <p-button label="Import from S3" icon="pi pi-cloud-download" (onClick)="goToS3()" />
      </div>
      <p-table [value]="(datasets$ | async) ?? []" [loading]="(loading$ | async) ?? false" styleClass="p-datatable-sm p-datatable-striped">
        <ng-template #header>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Source</th>
            <th>Type</th>
            <th>Entity Path</th>
            <th>Run</th>
            <th>Rows</th>
            <th>Size</th>
            <th>Imported</th>
            <th>Actions</th>
          </tr>
        </ng-template>
        <ng-template #body let-ds>
          <tr>
            <td>{{ ds.id }}</td>
            <td>
              <a (click)="openDataset(ds)" class="dataset-link">{{ ds.name }}</a>
            </td>
            <td class="truncate" [title]="ds.sourceUri">{{ ds.sourceUri }}</td>
            <td><p-tag [value]="ds.sourceType" severity="info" /></td>
            <td>{{ ds.entityPath || '-' }}</td>
            <td>{{ ds.runTimestamp || '-' }}</td>
            <td>{{ ds.rowCount | number }}</td>
            <td>{{ formatSize(ds.sizeBytes) }}</td>
            <td>{{ ds.importedAt | date:'short' }}</td>
            <td>
              <p-button icon="pi pi-eye" [rounded]="true" [text]="true" (onClick)="openDataset(ds)" />
              <p-button icon="pi pi-trash" [rounded]="true" [text]="true" severity="danger" (onClick)="confirmDelete(ds)" />
            </td>
          </tr>
        </ng-template>
        <ng-template #emptymessage>
          <tr><td colspan="10" class="text-center">No datasets imported yet.</td></tr>
        </ng-template>
      </p-table>
    </div>
    <p-confirmDialog />

    <!-- Upload dialog -->
    <p-dialog header="Upload File" [(visible)]="showUploadDialog" [modal]="true" [style]="{ width: '500px' }">
      <div class="upload-dialog">
        <div
          class="drop-zone"
          [class.drag-over]="isDragOver"
          (dragover)="onDragOver($event)"
          (dragleave)="isDragOver = false"
          (drop)="onDrop($event)"
          (click)="fileInput.click()"
        >
          <input #fileInput type="file" accept=".parquet,.csv,.csv.gz" (change)="onFileSelected($event)" hidden />
          @if (uploadFile) {
            <div class="file-info">
              <i class="pi pi-file"></i>
              <span class="file-name">{{ uploadFile.name }}</span>
              <span class="file-size">{{ formatSize(uploadFile.size) }}</span>
            </div>
          } @else {
            <div class="drop-hint">
              <i class="pi pi-cloud-upload" style="font-size: 2rem"></i>
              <p>Drop a .parquet or .csv file here, or click to browse</p>
            </div>
          }
        </div>

        @if (uploadFile) {
          <div class="upload-name">
            <p-floatlabel>
              <input pInputText id="uploadName" [(ngModel)]="uploadName" style="width: 100%" />
              <label for="uploadName">Dataset name</label>
            </p-floatlabel>
          </div>
        }

        <div class="upload-footer">
          <p-button
            label="Upload"
            icon="pi pi-upload"
            (onClick)="doUpload()"
            [disabled]="!uploadFile || !uploadName"
            [loading]="(uploading$ | async) ?? false"
          />
        </div>
      </div>
    </p-dialog>
  `,
  styles: [`
    .dataset-link { cursor: pointer; color: var(--p-primary-color); text-decoration: underline; }
    .truncate { max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .text-center { text-align: center; }
    .title-bar { display: flex; align-items: center; gap: 1rem; }
    .spacer { flex: 1; }
    .db-size { font-size: 0.85rem; color: var(--p-text-muted-color); padding: 0.25rem 0.75rem; border-radius: 6px; background: var(--p-surface-card); border: 1px solid var(--p-surface-border); }
    .upload-dialog { display: flex; flex-direction: column; gap: 1rem; }
    .drop-zone {
      border: 2px dashed var(--p-surface-border); border-radius: 8px;
      padding: 2rem; text-align: center; cursor: pointer;
      transition: border-color 0.2s, background 0.2s;
    }
    .drop-zone:hover, .drop-zone.drag-over {
      border-color: var(--p-primary-color); background: var(--p-primary-50);
    }
    .drop-hint { color: var(--p-text-muted-color); }
    .drop-hint p { margin: 0.5rem 0 0; font-size: 0.9rem; }
    .file-info { display: flex; align-items: center; gap: 0.75rem; justify-content: center; }
    .file-name { font-weight: 600; }
    .file-size { color: var(--p-text-muted-color); font-size: 0.85rem; }
    .upload-name { margin-top: 0.5rem; }
    .upload-footer { display: flex; justify-content: flex-end; }
  `]
})
export class DatasetListComponent implements OnInit {
  private store = inject(Store);
  private router = inject(Router);
  private confirmationService = inject(ConfirmationService);

  datasets$ = this.store.select(selectDatasets);
  loading$ = this.store.select(selectListLoading);
  duckdbSize$ = this.store.select(selectDuckdbSizeBytes);
  uploading$ = this.store.select(selectUploading);

  showUploadDialog = false;
  uploadFile: File | null = null;
  uploadName = '';
  isDragOver = false;
  private droppedEntryPath: string | null = null;

  constructor() {
    // Close dialog on upload complete
    this.store.select(selectUploading).subscribe(uploading => {
      if (!uploading && this.uploadFile) {
        // Upload just finished
        this.showUploadDialog = false;
        this.uploadFile = null;
        this.uploadName = '';
      }
    });
  }

  ngOnInit() {
    this.store.dispatch(DatasetActions.loadDatasets());
    this.store.dispatch(DatasetActions.loadHealth());
  }

  openDataset(ds: Dataset) {
    this.router.navigate(['/datasets', ds.id]);
  }

  goToCompare() {
    this.router.navigate(['/compare']);
  }

  goToS3() {
    this.router.navigate(['/s3']);
  }

  formatSize(bytes: number): string {
    if (bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
  }

  confirmDelete(ds: Dataset) {
    this.confirmationService.confirm({
      message: `Delete dataset "${ds.name}"?`,
      accept: () => {
        this.store.dispatch(DatasetActions.deleteDataset({ id: ds.id }));
      }
    });
  }

  // Upload
  onFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    if (input.files?.length) {
      this.setFile(input.files[0]);
    }
  }

  onDragOver(event: DragEvent) {
    event.preventDefault();
    this.isDragOver = true;
  }

  onDrop(event: DragEvent) {
    event.preventDefault();
    this.isDragOver = false;
    if (event.dataTransfer?.files.length) {
      const file = event.dataTransfer.files[0];
      // Try to get full path from entry API for folder name extraction
      const item = event.dataTransfer.items?.[0];
      const entry = item?.webkitGetAsEntry?.();
      if (entry) {
        this.droppedEntryPath = entry.fullPath; // e.g. "/BasePriceList/part-00000-..."
      }
      this.setFile(file);
    }
  }

  private setFile(file: File) {
    this.uploadFile = file;
    const lastModified = new Date(file.lastModified);
    const ts = lastModified.toISOString().replace(/[:.]/g, '-').substring(0, 19);

    let displayName = file.name.replace(/\.(snappy\.)?parquet$|\.csv(\.gz)?$/i, '');

    // Detect part-NNNNN-UUID pattern (e.g. part-00000-abcd1234-...-c000)
    const partPattern = /^part-\d{5}-[0-9a-f]{8}/i;
    if (partPattern.test(file.name)) {
      let folderName: string | null = null;

      // Try entry API path from drop event (e.g. "/BasePriceList/part-00000-...")
      if (this.droppedEntryPath) {
        const segments = this.droppedEntryPath.split('/').filter(s => s);
        if (segments.length >= 2) {
          folderName = segments[segments.length - 2];
        }
      }

      // Try webkitRelativePath
      if (!folderName) {
        const relPath = (file as any).webkitRelativePath as string;
        if (relPath) {
          const parts = relPath.split('/');
          if (parts.length >= 2) {
            folderName = parts[parts.length - 2];
          }
        }
      }

      displayName = folderName || 'parquet-upload';
    }

    this.droppedEntryPath = null;

    this.uploadName = `${displayName} @ ${ts}`;
  }

  doUpload() {
    if (!this.uploadFile || !this.uploadName) return;
    this.store.dispatch(DatasetActions.uploadFile({ file: this.uploadFile, name: this.uploadName }));
  }
}
